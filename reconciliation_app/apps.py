from django.apps import AppConfig


class ReconciliationAppConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'reconciliation_app'
    
    def ready(self):
        from .queue_manager import job_manager
        job_manager.start_processing()
