import csv
import io
import logging

from rest_framework import serializers

from .models import ReconciliationJob, ReconciliationResult, Ruleset, RulesetField

logger = logging.getLogger("reconciliation_app.api")


class ReconciliationJobSerializer(serializers.ModelSerializer):
    source_file = serializers.FileField()
    target_file = serializers.FileField()
    ruleset_id = serializers.UUIDField(write_only=True)
    ruleset_name = serializers.CharField(source="ruleset.name", read_only=True)

    class Meta:
        model = ReconciliationJob
        fields = [
            "id",
            "ruleset_id",
            "ruleset_name",
            "source_file",
            "target_file",
            "status",
            "created_at",
            "updated_at",
            "result_summary",
            "error_message",
        ]
        read_only_fields = [
            "id",
            "status",
            "created_at",
            "updated_at",
            "result_summary",
            "error_message",
            "ruleset_name",
        ]

    def validate_source_file(self, value):
        if not value.name.endswith(".csv"):
            raise serializers.ValidationError("Source file must be a CSV file")
        return value

    def validate_target_file(self, value):
        if not value.name.endswith(".csv"):
            raise serializers.ValidationError("Target file must be a CSV file")
        return value

    def validate_ruleset_id(self, value):
        try:
            Ruleset.objects.get(id=value)
            return value
        except Ruleset.DoesNotExist:
            raise serializers.ValidationError("Ruleset not found")

    @classmethod
    def validate_and_count_csv_files(cls, source_file, target_file, ruleset):
        logger.info(f"Starting CSV validation for ruleset: {ruleset.name}")

        try:
            logger.debug(f"Reading source file: {source_file.name}")
            source_content = source_file.read().decode("utf-8")
            source_file.seek(0)
            source_reader = csv.DictReader(io.StringIO(source_content))
            source_data = list(source_reader)

            logger.debug(f"Reading target file: {target_file.name}")
            target_content = target_file.read().decode("utf-8")
            target_file.seek(0)
            target_reader = csv.DictReader(io.StringIO(target_content))
            target_data = list(target_reader)

            if not source_data:
                logger.error("Source file is empty or invalid")
                raise ValueError("Source file is empty or invalid")
            if not target_data:
                logger.error("Target file is empty or invalid")
                raise ValueError("Target file is empty or invalid")

            source_headers = set(source_data[0].keys()) if source_data else set()
            target_headers = set(target_data[0].keys()) if target_data else set()

            logger.debug(f"Source headers: {sorted(source_headers)}")
            logger.debug(f"Target headers: {sorted(target_headers)}")

            if not source_headers or not target_headers:
                logger.error("CSV files must have headers")
                raise ValueError("CSV files must have headers")

            expected_fields = set(field.field_name for field in ruleset.fields.all())
            required_fields = set(
                field.field_name for field in ruleset.fields.filter(is_required=True)
            )

            logger.debug(f"Ruleset expected fields: {sorted(expected_fields)}")
            logger.debug(f"Ruleset required fields: {sorted(required_fields)}")

            if ruleset.match_key not in source_headers:
                logger.error(f"Match key '{ruleset.match_key}' not found in source file headers")
                raise ValueError(
                    f"Match key '{ruleset.match_key}' not found in source file headers"
                )
            if ruleset.match_key not in target_headers:
                logger.error(f"Match key '{ruleset.match_key}' not found in target file headers")
                raise ValueError(
                    f"Match key '{ruleset.match_key}' not found in target file headers"
                )

            source_missing_required = required_fields - source_headers
            target_missing_required = required_fields - target_headers

            validation_errors = []
            if source_missing_required:
                error_msg = (
                    f"Source file missing required fields: {', '.join(source_missing_required)}"
                )
                logger.error(error_msg)
                validation_errors.append(error_msg)
            if target_missing_required:
                error_msg = (
                    f"Target file missing required fields: {', '.join(target_missing_required)}"
                )
                logger.error(error_msg)
                validation_errors.append(error_msg)

            if validation_errors:
                raise ValueError("; ".join(validation_errors))

            source_unexpected = source_headers - expected_fields
            target_unexpected = target_headers - expected_fields
            warnings = []
            if source_unexpected:
                warning_msg = f"Source file has unexpected fields: {', '.join(source_unexpected)}"
                logger.warning(warning_msg)
                warnings.append(warning_msg)
            if target_unexpected:
                warning_msg = f"Target file has unexpected fields: {', '.join(target_unexpected)}"
                logger.warning(warning_msg)
                warnings.append(warning_msg)

            validation_result = {
                "ruleset_validation": True,
                "ruleset_name": ruleset.name,
                "match_key": ruleset.match_key,
                "expected_fields": list(expected_fields),
                "required_fields": list(required_fields),
                "source_headers": list(source_headers),
                "target_headers": list(target_headers),
                "headers_valid": True,
                "warnings": warnings,
            }

            logger.info(
                f"CSV validation successful - Source: {len(source_data)} records, "
                f"Target: {len(target_data)} records, Warnings: {len(warnings)}"
            )

            return {
                "source_count": len(source_data),
                "target_count": len(target_data),
                "validation": validation_result,
                "success": True,
            }

        except Exception as e:
            logger.error(f"CSV validation failed: {e}")
            return {"success": False, "error": str(e)}


class ReconciliationResultSerializer(serializers.ModelSerializer):
    class Meta:
        model = ReconciliationResult
        fields = [
            "id",
            "result_type",
            "source_row_data",
            "target_row_data",
            "match_key",
            "differences",
        ]


class ReconciliationJobDetailSerializer(serializers.ModelSerializer):
    ruleset_name = serializers.CharField(source="ruleset.name", read_only=True)
    ruleset_id = serializers.UUIDField(source="ruleset.id", read_only=True)
    match_key = serializers.CharField(source="ruleset.match_key", read_only=True)

    class Meta:
        model = ReconciliationJob
        fields = [
            "id",
            "ruleset_id",
            "ruleset_name",
            "match_key",
            "status",
            "created_at",
            "updated_at",
            "result_summary",
            "error_message",
        ]


class JobResultsSerializer(serializers.ModelSerializer):

    class Meta:
        model = ReconciliationResult
        fields = [
            "id",
            "result_type",
            "source_row_data",
            "target_row_data",
            "match_key",
            "differences",
        ]


class RulesetFieldSerializer(serializers.ModelSerializer):
    class Meta:
        model = RulesetField
        fields = ["id", "field_name", "data_type", "is_required", "description"]


class RulesetSerializer(serializers.ModelSerializer):
    fields = RulesetFieldSerializer(many=True)

    class Meta:
        model = Ruleset
        fields = ["id", "name", "description", "match_key", "created_at", "updated_at", "fields"]
        read_only_fields = ["id", "created_at", "updated_at"]

    def create(self, validated_data):
        fields_data = validated_data.pop("fields")
        ruleset = Ruleset.objects.create(**validated_data)

        for field_data in fields_data:
            if field_data.get("data_type") not in [
                "string",
                "integer",
                "float",
                "boolean",
                "date",
                "datetime",
                "email",
                "url",
                "phone",
            ]:
                raise serializers.ValidationError(f"Invalid data type: {field_data['data_type']}")
            RulesetField.objects.create(ruleset=ruleset, **field_data)

        return ruleset

    def update(self, instance, validated_data):
        fields_data = validated_data.pop("fields", None)

        for attr, value in validated_data.items():
            setattr(instance, attr, value)
        instance.save()

        if fields_data is not None:
            instance.fields.all().delete()
            for field_data in fields_data:
                RulesetField.objects.create(ruleset=instance, **field_data)

        return instance


class RulesetListSerializer(serializers.ModelSerializer):
    fields_count = serializers.SerializerMethodField()

    class Meta:
        model = Ruleset
        fields = [
            "id",
            "name",
            "description",
            "match_key",
            "created_at",
            "updated_at",
            "fields_count",
        ]
        read_only_fields = ["id", "created_at", "updated_at"]

    def get_fields_count(self, obj):
        return obj.fields.count()
