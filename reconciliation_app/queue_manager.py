import csv
import io
import os
import queue
import threading
import time
import logging
from typing import Optional
from .models import ReconciliationJob, ReconciliationResult
from datetime import datetime
from .reconciliation_engine import ReconciliationEngine

logger = logging.getLogger('reconciliation_app.queue')



class JobQueue:

    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self._queue = queue.Queue()
            self._initialized = True
    
    def submit_job(self, job_id: int) -> bool:
        try:
            self._queue.put(job_id, timeout=5)
            logger.info(f"Job {job_id} submitted to queue")
            return True
        except queue.Full:
            logger.error(f"Queue is full, cannot submit job {job_id}")
            return False
    
    def get_next_job(self, timeout: Optional[float] = None) -> Optional[int]:
        try:
            return self._queue.get(timeout=timeout)
        except queue.Empty:
            return None
    
    def mark_job_done(self):
        self._queue.task_done()
    
    def get_queue_size(self) -> int:
        return self._queue.qsize()


class JobProcessor:

    def __init__(self, job_queue: JobQueue):
        self.job_queue = job_queue
        self.is_running = False
        self.worker_thread = None
        self.reconciliation_engine = ReconciliationEngine()
    
    def start(self):
        if not self.is_running:
            self.is_running = True
            self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
            self.worker_thread.start()
            logger.info("Job processor started")
    
    def stop(self):
        self.is_running = False
        if self.worker_thread:
            self.worker_thread.join(timeout=10)
        logger.info("Job processor stopped")
    
    def _worker_loop(self):
        logger.info("Reconciliation Worker started")
        
        while self.is_running:
            try:
                job_id = self.job_queue.get_next_job(timeout=1.0)
                if job_id is not None:
                    self._process_job(job_id)
                    self.job_queue.mark_job_done()
            except Exception as e:
                logger.error(f"Error in worker loop: {e}")
                time.sleep(1)
    
    def _process_job(self, job_id: int):
        start_time = datetime.now()
        try:
            job = ReconciliationJob.objects.get(id=job_id)
            logger.info(f"Starting job {job_id} - Ruleset: {job.ruleset.name if job.ruleset else 'None'}")
            
            job.status = 'processing'
            job.save()
            logger.debug(f"Job {job_id} status updated to 'processing'")
            
            logger.debug(f"Reading CSV files for job {job_id}")
            source_data = self._read_csv_file(job.source_file_path)
            target_data = self._read_csv_file(job.target_file_path)
            
            if not source_data or not target_data:
                raise ValueError("Failed to read CSV files")
            
            logger.info(f"Job {job_id} - Source: {len(source_data)} records, Target: {len(target_data)} records")
            
            logger.debug(f"Validating CSV data for job {job_id}")
            validation_errors = self.reconciliation_engine.validate_csv_data(job, source_data, target_data)
            if validation_errors:
                logger.error(f"Job {job_id} validation failed with {len(validation_errors)} errors")
                raise ValueError(f"Data validation failed: {'; '.join(validation_errors)}")
            
            logger.debug(f"Job {job_id} data validation passed")
            
            logger.debug(f"Starting reconciliation for job {job_id}")
            results = self.reconciliation_engine.reconcile_data(job, source_data, target_data)
            
            logger.debug(f"Saving results for job {job_id}")
            self._save_results(job, results)
            
            summary = self.reconciliation_engine.calculate_summary(job, results)
            job.result_summary = summary
            job.save()
            
            job.status = 'completed'
            job.save()
            job.cleanup_files(logger=logger)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"Job {job_id} completed successfully in {processing_time:.2f} seconds")
            
        except ReconciliationJob.DoesNotExist:
            logger.error(f"Job {job_id} not found in database")
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Job {job_id} failed after {processing_time:.2f} seconds: {e}")
            try:
                job = ReconciliationJob.objects.get(id=job_id)
                job.status = 'failed'
                job.error_message = str(e)
                job.save()
                logger.debug(f"Job {job_id} status updated to 'failed'")
            except Exception as save_error:
                logger.error(f"Failed to update job {job_id} status to failed: {save_error}")
    
    def _read_csv_file(self, file_path: str) -> list:
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                return list(reader)
        except Exception as e:
            logger.error(f"Error reading CSV file {file_path}: {e}")
            return []

    
    def _save_results(self, job: ReconciliationJob, results: dict):
        """Save reconciliation results to database"""
        # Save matched records
        for result in results['matched']:
            ReconciliationResult.objects.create(
                job=job,
                result_type='matched',
                source_row_data=result['source_row'],
                target_row_data=result['target_row'],
                match_key=result['match_key'],
                differences=result['differences']
            )
        
        # Save unmatched source records
        for result in results['unmatched_source']:
            ReconciliationResult.objects.create(
                job=job,
                result_type='unmatched_source',
                source_row_data=result['source_row'],
                match_key=result['match_key']
            )
        
        # Save unmatched target records
        for result in results['unmatched_target']:
            ReconciliationResult.objects.create(
                job=job,
                result_type='unmatched_target',
                target_row_data=result['target_row'],
                match_key=result['match_key']
            )
        


class JobManager:
    """Orchestrates job submission and processing"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.job_queue = JobQueue()
            self.job_processor = JobProcessor(self.job_queue)
            self._initialized = True
    
    def start_processing(self):
        """Start the background job processor"""
        self.job_processor.start()
    
    def stop_processing(self):
        """Stop the background job processor"""
        self.job_processor.stop()
    
    def submit_job(self, job_id: int) -> bool:
        """Submit a job for processing"""
        success = self.job_queue.submit_job(job_id)
        if success:
            # Update job status to queued
            try:
                job = ReconciliationJob.objects.get(id=job_id)
                job.status = 'queued'
                job.save()
            except ReconciliationJob.DoesNotExist:
                logger.error(f"Job {job_id} not found when updating status to queued")
                return False
        return success
    
    def get_queue_status(self) -> dict:
        return {
            'queue_size': self.job_queue.get_queue_size(),
            'processor_running': self.job_processor.is_running
        }


job_manager = JobManager()