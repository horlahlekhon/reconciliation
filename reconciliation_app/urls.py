from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

router = DefaultRouter()
router.register(r'rulesets', views.RulesetViewSet)

urlpatterns = [
    path('reconcile/', views.ReconcileCSVFilesView.as_view(), name='reconcile_csv_files'),
    path('jobs/', views.ReconciliationJobListView.as_view(), name='list_reconciliation_jobs'),
    path('jobs/<uuid:job_id>/', views.ReconciliationJobDetailView.as_view(), name='get_reconciliation_job'),
    path('jobs/<uuid:job_id>/results/', views.JobResultsView.as_view(), name='get_job_results'),
    path('', include(router.urls)),
]