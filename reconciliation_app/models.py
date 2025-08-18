import os
import shutil
import uuid
from logging import Logger

from django.db import models
from django.conf import settings


class Ruleset(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255, unique=True)
    description = models.TextField(blank=True)
    match_key = models.CharField(max_length=255, help_text="Field name to use for matching records between source and target")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        ordering = ['-created_at']
    
    def __str__(self):
        return f"Ruleset: {self.name}"


class RulesetField(models.Model):
    DATA_TYPE_CHOICES = [
        ('string', 'String'),
        ('integer', 'Integer'),
        ('float', 'Float'),
        ('boolean', 'Boolean'),
        ('date', 'Date'),
        ('datetime', 'DateTime'),
        ('email', 'Email'),
        ('phone', 'Phone'),
        ('url', 'URL'),
    ]
    
    ruleset = models.ForeignKey(Ruleset, on_delete=models.CASCADE, related_name='fields')
    field_name = models.CharField(max_length=255)
    data_type = models.CharField(max_length=20, choices=DATA_TYPE_CHOICES, default='string')
    is_required = models.BooleanField(default=True)
    description = models.TextField(blank=True)
    
    class Meta:
        unique_together = ['ruleset', 'field_name']
        ordering = ['field_name']
    
    def __str__(self):
        return f"{self.ruleset.name} - {self.field_name} ({self.data_type})"


class ReconciliationJob(models.Model):
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('queued', 'Queued'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    ruleset = models.ForeignKey(Ruleset, on_delete=models.CASCADE, related_name='jobs', null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    source_file = models.FileField(upload_to='reconciliation/source/', null=True, blank=True)
    target_file = models.FileField(upload_to='reconciliation/target/', null=True, blank=True)
    source_file_path = models.CharField(max_length=500, null=True, blank=True)
    target_file_path = models.CharField(max_length=500, null=True, blank=True)
    source_record_count = models.IntegerField(null=True, blank=True)
    target_record_count = models.IntegerField(null=True, blank=True)
    result_summary = models.JSONField(null=True, blank=True)
    error_message = models.JSONField(null=True, blank=True)
    
    def get_job_directory(self):
        return os.path.join(settings.MEDIA_ROOT, 'reconciliation', 'jobs', str(self.id))
    
    def get_source_file_path(self):
        return os.path.join(self.get_job_directory(), 'source.csv')
    
    def get_target_file_path(self):
        return os.path.join(self.get_job_directory(), 'target.csv')
    
    def cleanup_files(self, logger: Logger):
        try:
            logger.info(f"Cleaning Job {self.id} files After processing")
            job_dir = self.get_job_directory()
            if os.path.exists(job_dir):
                shutil.rmtree(job_dir)
                return True
        except Exception:
            return False
        return False
    
    class Meta:
        ordering = ['-created_at']
    
    def __str__(self):
        return f"Reconciliation Job #{self.id} - {self.status}"


class ReconciliationResult(models.Model):
    RESULT_TYPE_CHOICES = [
        ('matched', 'Matched'),
        ('unmatched_source', 'Unmatched in Source'),
        ('unmatched_target', 'Unmatched in Target'),
        ('duplicate', 'Duplicate'),
    ]
    
    job = models.ForeignKey(ReconciliationJob, on_delete=models.CASCADE, related_name='results')
    result_type = models.CharField(max_length=20, choices=RESULT_TYPE_CHOICES)
    source_row_data = models.JSONField(null=True, blank=True)
    target_row_data = models.JSONField(null=True, blank=True)
    match_key = models.CharField(max_length=255, null=True, blank=True)
    differences = models.JSONField(null=True, blank=True)
    
    def __str__(self):
        return f"Result for Job #{self.job.id} - {self.result_type}"
