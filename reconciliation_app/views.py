import logging
import os

from django.db.models import Count
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework import status, viewsets
from rest_framework.decorators import api_view
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger("reconciliation_app.api")
from .models import ReconciliationJob, ReconciliationResult, Ruleset
from .queue_manager import job_manager
from .serializers import (
    JobResultsSerializer,
    ReconciliationJobDetailSerializer,
    ReconciliationJobSerializer,
    RulesetListSerializer,
    RulesetSerializer,
)


def save_csv_files_to_job_directory(job, source_file, target_file):
    """Save CSV files to job-specific directory"""
    try:
        job_dir = job.get_job_directory()
        os.makedirs(job_dir, exist_ok=True)

        source_path = job.get_source_file_path()
        with open(source_path, "wb") as f:
            for chunk in source_file.chunks():
                f.write(chunk)

        target_path = job.get_target_file_path()
        with open(target_path, "wb") as f:
            for chunk in target_file.chunks():
                f.write(chunk)

        job.source_file_path = source_path
        job.target_file_path = target_path
        job.save()

        return True

    except Exception as e:
        raise ValueError(f"Failed to save files: {str(e)}")


class ReconcileCSVFilesView(APIView):
    @swagger_auto_schema(
        operation_description="Upload two CSV files (source and target) for reconciliation with ruleset validation",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                "source_file": openapi.Schema(
                    type=openapi.TYPE_FILE, description="Source CSV file"
                ),
                "target_file": openapi.Schema(
                    type=openapi.TYPE_FILE, description="Target CSV file"
                ),
                "ruleset_id": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    format=openapi.FORMAT_UUID,
                    description="Ruleset ID for validation",
                ),
            },
            required=["source_file", "target_file", "ruleset_id"],
        ),
        responses={
            201: openapi.Response("Reconciliation job created", ReconciliationJobSerializer),
            400: "Bad Request - Invalid files, format, or ruleset",
        },
    )
    def post(self, request):
        logger.info(
            f"Reconciliation request received from {request.META.get('REMOTE_ADDR', 'unknown')}"
        )

        source_file = request.FILES.get("source_file")
        target_file = request.FILES.get("target_file")
        ruleset_id = request.data.get("ruleset_id")

        logger.debug(
            f"Request parameters: source_file={source_file.name if source_file else None}, "
            f"target_file={target_file.name if target_file else None}, ruleset_id={ruleset_id}"
        )

        if not source_file or not target_file:
            logger.warning("Reconciliation request missing required files")
            return Response(
                {"error": "Both source_file and target_file are required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not ruleset_id:
            logger.warning("Reconciliation request missing ruleset_id")
            return Response({"error": "ruleset_id is required"}, status=status.HTTP_400_BAD_REQUEST)

        # Validate file types
        if not source_file.name.endswith(".csv") or not target_file.name.endswith(".csv"):
            logger.warning(f"Invalid file types: {source_file.name}, {target_file.name}")
            return Response(
                {"error": "Both files must be CSV files"}, status=status.HTTP_400_BAD_REQUEST
            )

        try:
            ruleset = Ruleset.objects.get(id=ruleset_id)
            logger.debug(f"Using ruleset: {ruleset.name}")
        except Ruleset.DoesNotExist:
            logger.error(f"Ruleset {ruleset_id} not found")
            return Response({"error": "Ruleset not found"}, status=status.HTTP_400_BAD_REQUEST)

        validation_result = ReconciliationJobSerializer.validate_and_count_csv_files(
            source_file, target_file, ruleset
        )

        if not validation_result["success"]:
            logger.error(f"CSV validation failed: {validation_result['error']}")
            return Response(
                {"error": validation_result["error"]}, status=status.HTTP_400_BAD_REQUEST
            )

        try:
            # Create reconciliation job with ruleset
            job = ReconciliationJob.objects.create(
                ruleset=ruleset,
                status="pending",
                source_record_count=validation_result["source_count"],
                target_record_count=validation_result["target_count"],
            )

            logger.info(
                f"Created job {job.id} with ruleset {ruleset.name} - "
                f"Source: {validation_result['source_count']} records, "
                f"Target: {validation_result['target_count']} records"
            )

            save_csv_files_to_job_directory(job, source_file, target_file)
            logger.debug(f"Saved CSV files for job {job.id}")

            if job_manager.submit_job(job.id):
                logger.info(f"Job {job.id} successfully queued for processing")
                response_data = {
                    "job_id": job.id,
                    "ruleset_id": str(job.ruleset.id),
                    "ruleset_name": job.ruleset.name,
                    "status": job.status,
                    "source_record_count": job.source_record_count,
                    "target_record_count": job.target_record_count,
                    "validation": validation_result["validation"],
                    "message": "Job queued for processing",
                }
                return Response(response_data, status=status.HTTP_201_CREATED)
            else:
                logger.error(f"Failed to submit job {job.id} to queue")
                job.status = "failed"
                job.error_message = "Failed to submit job to queue"
                job.save()
                return Response(
                    {"error": "Failed to submit job to processing queue"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )

        except Exception as e:
            logger.error(f"Failed to create reconciliation job: {e}")
            return Response(
                {"error": f"Failed to create job: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


class ReconciliationJobDetailView(APIView):
    @swagger_auto_schema(
        operation_description="Get reconciliation job details and results",
        responses={
            200: openapi.Response("Job details", ReconciliationJobDetailSerializer),
            404: "Job not found",
        },
    )
    def get(self, request, job_id):
        logger.debug(f"Job details requested for job {job_id}")
        try:
            job = ReconciliationJob.objects.get(id=job_id)
            logger.debug(f"Job {job_id} found with status: {job.status}")
            serializer = ReconciliationJobDetailSerializer(job)
            return Response(serializer.data)
        except ReconciliationJob.DoesNotExist:
            logger.warning(f"Job {job_id} not found")
            return Response({"error": "Job not found"}, status=status.HTTP_404_NOT_FOUND)


class ReconciliationJobListView(APIView):
    @swagger_auto_schema(
        operation_description="List all reconciliation jobs",
        responses={200: openapi.Response("List of jobs", ReconciliationJobSerializer(many=True))},
    )
    def get(self, request):
        jobs = ReconciliationJob.objects.all()
        serializer = ReconciliationJobSerializer(jobs, many=True)
        return Response(serializer.data)


class JobResultsView(APIView):
    @swagger_auto_schema(
        operation_description="Get paginated reconciliation results for a specific job",
        manual_parameters=[
            openapi.Parameter(
                "page", openapi.IN_QUERY, description="Page number", type=openapi.TYPE_INTEGER
            ),
            openapi.Parameter(
                "page_size",
                openapi.IN_QUERY,
                description="Number of results per page (max 100)",
                type=openapi.TYPE_INTEGER,
            ),
            openapi.Parameter(
                "result_type",
                openapi.IN_QUERY,
                description="Filter by result type (matched, unmatched_source, unmatched_target)",
                type=openapi.TYPE_STRING,
            ),
        ],
        responses={
            200: openapi.Response("job results", JobResultsSerializer(many=True)),
            404: "Job not found",
        },
    )
    def get(self, request, job_id):
        result_type = request.query_params.get("result_type", None)
        page_size = request.query_params.get("page_size", 50)
        page = request.query_params.get("page", 1)

        logger.debug(
            f"Job results requested for job {job_id} - "
            f"type: {result_type}, page: {page}, page_size: {page_size}"
        )

        try:
            job = ReconciliationJob.objects.get(id=job_id)
        except ReconciliationJob.DoesNotExist:
            logger.warning(f"Job {job_id} not found for results request")
            return Response({"error": "Job not found"}, status=status.HTTP_404_NOT_FOUND)

        queryset = ReconciliationResult.objects.filter(job=job).order_by("id")
        total_results = queryset.count()

        if result_type and result_type in [
            "matched",
            "unmatched_source",
            "unmatched_target",
            "duplicate",
        ]:
            queryset = queryset.filter(result_type=result_type)

        paginator = PageNumberPagination()
        paginator.page_size = page_size
        paginated_results = paginator.paginate_queryset(queryset, request)

        serializer = JobResultsSerializer(paginated_results, many=True)

        return paginator.get_paginated_response(serializer.data)


class RulesetViewSet(viewsets.ModelViewSet):
    queryset = Ruleset.objects.all()

    def get_serializer_class(self):
        if self.action == "list":
            return RulesetListSerializer
        return RulesetSerializer

    @swagger_auto_schema(
        operation_description="Create a new ruleset with field definitions",
        request_body=RulesetSerializer,
        responses={
            201: openapi.Response("Ruleset created", RulesetSerializer),
            400: "Bad Request - Invalid data",
        },
    )
    def create(self, request, *args, **kwargs):
        return super().create(request, *args, **kwargs)

    @swagger_auto_schema(
        operation_description="List all rulesets",
        responses={200: openapi.Response("List of rulesets", RulesetListSerializer(many=True))},
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)

    @swagger_auto_schema(
        operation_description="Get ruleset details including all field definitions",
        responses={
            200: openapi.Response("Ruleset details", RulesetSerializer),
            404: "Ruleset not found",
        },
    )
    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)

    @swagger_auto_schema(
        operation_description="Update a ruleset and its field definitions",
        request_body=RulesetSerializer,
        responses={
            200: openapi.Response("Ruleset updated", RulesetSerializer),
            400: "Bad Request - Invalid data",
            404: "Ruleset not found",
        },
    )
    def update(self, request, *args, **kwargs):
        return super().update(request, *args, **kwargs)

    @swagger_auto_schema(
        operation_description="Partially update a ruleset",
        request_body=RulesetSerializer,
        responses={
            200: openapi.Response("Ruleset updated", RulesetSerializer),
            400: "Bad Request - Invalid data",
            404: "Ruleset not found",
        },
    )
    def partial_update(self, request, *args, **kwargs):
        return super().partial_update(request, *args, **kwargs)

    @swagger_auto_schema(
        operation_description="Delete a ruleset and all its field definitions",
        responses={204: "Ruleset deleted", 404: "Ruleset not found"},
    )
    def destroy(self, request, *args, **kwargs):
        return super().destroy(request, *args, **kwargs)
