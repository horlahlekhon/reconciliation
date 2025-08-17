import csv
import io
import os
import queue
import threading
import time
import logging
from typing import Optional
from django.conf import settings
from .models import ReconciliationJob, ReconciliationResult
from datetime import datetime
import re

logger = logging.getLogger('reconciliation_app.queue')

URL_PATTERN = r'^https?://[^\s/$.?#].[^\s]*$'
EMAIL_PATTERN = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

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
    
    def start(self):
        """Start the background worker thread"""
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
            
            # Read CSV files from stored paths
            logger.debug(f"Reading CSV files for job {job_id}")
            source_data = self._read_csv_file(job.source_file_path)
            target_data = self._read_csv_file(job.target_file_path)
            
            if not source_data or not target_data:
                raise ValueError("Failed to read CSV files")
            
            logger.info(f"Job {job_id} - Source: {len(source_data)} records, Target: {len(target_data)} records")
            
            # Validate CSV data against ruleset
            logger.debug(f"Validating CSV data for job {job_id}")
            validation_errors = self._validate_csv_data(job, source_data, target_data)
            if validation_errors:
                logger.error(f"Job {job_id} validation failed with {len(validation_errors)} errors")
                raise ValueError(f"Data validation failed: {'; '.join(validation_errors)}")
            
            logger.debug(f"Job {job_id} data validation passed")
            
            # Perform reconciliation
            logger.debug(f"Starting reconciliation for job {job_id}")
            results = self._reconcile_data(job, source_data, target_data)
            
            # Save results
            logger.debug(f"Saving results for job {job_id}")
            self._save_results(job, results)
            
            # Update job status
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

    def validate_field_value(self, value, data_type, file_name):
            if not value and data_type in ['integer', 'float']:
                return None  # Allow empty values for numeric fields
            if not value:
                return None  # Allow empty values for other fields too
            
            try:
                if data_type == 'integer':
                    int(value)
                elif data_type == 'float':
                    float(value)
                elif data_type == 'boolean':
                    if value.lower() not in ['true', 'false', '1', '0', 'yes', 'no']:
                        return f"Invalid boolean value '{value}'"
                elif data_type == 'date':
                    datetime.strptime(value, '%Y-%m-%d')
                elif data_type == 'datetime':
                    # TODO make dateformat be sent as part of ruleset
                    formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S.%f']
                    parsed = False
                    for fmt in formats:
                        try:
                            datetime.strptime(value, fmt)
                            parsed = True
                            break
                        except ValueError:
                            continue
                    if not parsed:
                        return f"Invalid datetime format '{value}'"
                elif data_type == 'email':
                    if not re.match(EMAIL_PATTERN, value):
                        return f"Invalid email format '{value}'"
                elif data_type == 'url':
                    if not re.match(URL_PATTERN, value):
                        return f"Invalid URL format '{value}'"
            except ValueError:
                return f"Invalid {data_type} value '{value}'"
            
            return None
        
    def _validate_csv_data(self, job: ReconciliationJob, source_data: list, target_data: list) -> list:
        """
            Validate CSV data against ruleset definitions
            PS: the errors are limited to 100 errors, if a file has 100 errors we just abort the process
        """
        
        validation_errors = []
        
        if not job.ruleset:
            logger.warning(f"Job {job.id} has no ruleset - skipping data validation")
            return validation_errors
        
        field_type_map = {field.field_name: field.data_type for field in job.ruleset.fields.all()}
        
        
        error_count = 0
        for i, row in enumerate(source_data, 1):
            if error_count >= 100:
                validation_errors.append("... and more errors (showing first 100)")
                break
            for field_name, value in row.items():
                if field_name in field_type_map:
                    error = self.validate_field_value(value, field_type_map[field_name], "Source")
                    if error:
                        validation_errors.append(f"Source row {i}, field '{field_name}': {error}")
                        error_count += 1
                        if error_count >= 100:
                            break
        
        for i, row in enumerate(target_data, 1):
            if error_count >= 100:
                break
            for field_name, value in row.items():
                if field_name in field_type_map:
                    error = self.validate_field_value(value, field_type_map[field_name], "Target")
                    if error:
                        validation_errors.append(f"Target row {i}, field '{field_name}': {error}")
                        error_count += 1
                        if error_count >= 100:
                            break
        
        return validation_errors
    
    def _reconcile_data(self, job: ReconciliationJob, source_data: list, target_data: list) -> dict:
        if not source_data or not target_data:
            raise ValueError("Empty data sets")

        if not job.ruleset:
            logger.warning(f"Job {job.id} has no ruleset - skipping data validation")
            raise ValueError("Job doesnt have a specified ruleset")
        source_keys = set(source_data[0].keys()) if source_data else set()
        target_keys = set(target_data[0].keys()) if target_data else set()

        match_key = job.ruleset.match_key
        if match_key not in source_keys:
            raise ValueError(f"Match key '{match_key}' not found in source data")
        if match_key not in target_keys:
            raise ValueError(f"Match key '{match_key}' not found in target data")
        
        logger.info(f"Using match key '{match_key}' for reconciliation")
        
        # Create dictionaries for fast lookup
        source_dict = {row[match_key]: row for row in source_data}
        target_dict = {row[match_key]: row for row in target_data}
        
        results = {
            'matched': [],
            'unmatched_source': [],
            'unmatched_target': []
        }
        
        # Find all unique keys
        all_keys = set(source_dict.keys()) | set(target_dict.keys())
        
        for key in all_keys:
            source_row = source_dict.get(key)
            target_row = target_dict.get(key)
            
            if source_row and target_row:
                # Found in both - check for differences
                differences = {}
                for field in source_keys.union(target_keys):
                    source_val = source_row.get(field, '')
                    target_val = target_row.get(field, '')
                    if source_val != target_val:
                        differences[field] = {'source': source_val, 'target': target_val}
                
                results['matched'].append({
                    'source_row': source_row,
                    'target_row': target_row,
                    'match_key': key,
                    'differences': differences if differences else None
                })
                
            elif source_row and not target_row:
                results['unmatched_source'].append({
                    'source_row': source_row,
                    'match_key': key
                })
                
            elif target_row and not source_row:
                results['unmatched_target'].append({
                    'target_row': target_row,
                    'match_key': key
                })
        
        return results
    
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
        
        # Update job summary
        summary = {
            'total_source_records': job.source_record_count,
            'total_target_records': job.target_record_count,
            'matched_records': len(results['matched']),
            'unmatched_source_records': len(results['unmatched_source']),
            'unmatched_target_records': len(results['unmatched_target']),
            'match_percentage': round(
                (len(results['matched']) / max(job.source_record_count, job.target_record_count)) * 100, 2
            ) if job.source_record_count and job.target_record_count else 0
        }
        
        job.result_summary = summary
        job.save()


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
        """Get current queue status"""
        return {
            'queue_size': self.job_queue.get_queue_size(),
            'processor_running': self.job_processor.is_running
        }


job_manager = JobManager()