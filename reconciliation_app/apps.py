import atexit
import signal
import sys

from django.apps import AppConfig


class ReconciliationAppConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "reconciliation_app"

    def ready(self):
        from .queue_manager import job_manager

        job_manager.start_processing()

        def shutdown_handler(signum=None, frame=None):
            print("\nReceived shutdown signal, stopping job processor...")
            job_manager.stop_processing()
            sys.exit(0)

        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)

        # Register atexit handler as fallback
        atexit.register(lambda: job_manager.stop_processing())
