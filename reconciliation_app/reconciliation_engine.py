import re
import logging
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime
from collections.abc import Callable
from .models import ReconciliationJob

logger = logging.getLogger('reconciliation_app.reconciliation')

URL_PATTERN = r'^https?://[^\s/$.?#].[^\s]*$'
EMAIL_PATTERN = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
PHONE_PATTERN = r'^[\+]?[1-9]?[\d\s\-\(\)\.]{7,15}$'

DATE_FORMATS = [
    '%Y-%m-%d',
    '%m/%d/%Y',
    '%d/%m/%Y',
    '%d-%-m-%Y',
    '%Y%m%d',
    '%d.%m.%Y',
]

DATETIME_FORMATS = [
    '%Y-%m-%d %H:%M:%S', 
    '%Y-%m-%dT%H:%M:%S', 
    '%Y-%m-%d %H:%M:%S.%f'
]


class ReconciliationEngine:
    """
    Handles all reconciliation and normalization logic.
    Separates business logic from job processing concerns.
    """
    
    def __init__(self):
        self.logger = logger
    
    def normalize_string_field(self, value: str, field_type: str = 'string') -> str:
        """
        Normalize string fields for comparison.
        
        Args:
            value: The string value to normalize
            field_type: The type of field ('email', 'phone', 'string')
        
        Returns:
            Normalized string value
        """
        if not value or not isinstance(value, str):
            return value
        
        normalized = value.strip().replace('\n', '').replace('\r', '').lower()
        
        if field_type == 'email':
            return normalized
        elif field_type == 'phone':
            normalized = re.sub(r'[\s\-\(\)\.]+', '', normalized)
            return normalized
        else:
            normalized = re.sub(r'\s+', ' ', normalized)
            return normalized

    def validate_datetime(self, date: str, formats: List[str], formatter: Callable) -> Tuple[bool, str]:
        """Validate datetime against multiple formats"""
        parsed = False
        for fmt in formats:
            try:
                formatter(date.strip(), fmt)
                parsed = True
                break
            except ValueError:
                continue
        
        if not parsed:
            return parsed, f"Invalid datetime format '{date}'"
        return parsed, ""

    def validate_field_value(self, value: Any, data_type: str, file_name: str) -> Optional[str]:
        """
        Validate a field value against its expected data type.
        
        Args:
            value: The value to validate
            data_type: Expected data type
            file_name: Source file name for error reporting
            
        Returns:
            Error message if validation fails, None if valid
        """
        if not value and data_type in ['integer', 'float']:
            return None  # Allow empty values for numeric fields
        if not value:
            return None  # Allow empty values for other fields too
        
        value = value.strip()
        
        try:
            if data_type == 'integer':
                int(value)
            elif data_type == 'float':
                float(value)
            elif data_type == 'boolean':
                if value.lower() not in ['true', 'false', '1', '0', 'yes', 'no']:
                    return f"Invalid boolean value '{value}'"
            elif data_type in ['date', 'datetime']:
                parsed, error = self.validate_datetime(value, DATE_FORMATS, datetime.strptime)
                if not parsed:
                    return error
            elif data_type == 'email':
                if not re.match(EMAIL_PATTERN, value):
                    return f"Invalid email format '{value}'"
            elif data_type == 'phone':
                if not re.match(PHONE_PATTERN, value):
                    return f"Invalid phone format '{value}'"
            elif data_type == 'url':
                if not re.match(URL_PATTERN, value):
                    return f"Invalid URL format '{value}'"
        except ValueError:
            return f"Invalid {data_type} value '{value}'"
        
        return None

    def validate_csv_data(self, job: ReconciliationJob, source_data: List[Dict], target_data: List[Dict]) -> List[str]:
        """
        Validate CSV data against ruleset definitions.
        Limits errors to 100 to prevent overwhelming output.
        
        Args:
            job: ReconciliationJob instance with ruleset
            source_data: Source CSV data as list of dictionaries
            target_data: Target CSV data as list of dictionaries
            
        Returns:
            List of validation error messages
        """
        validation_errors = []
        
        if not job.ruleset:
            self.logger.warning(f"Job {job.id} has no ruleset - skipping data validation")
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

    def get_field_type_map(self, job: ReconciliationJob) -> Dict[str, str]:
        """Get field type mapping from ruleset"""
        if not job.ruleset:
            return {}
        return {field.field_name: field.data_type for field in job.ruleset.fields.all()}

    def normalize_value_for_comparison(self, value: str, field_name: str, field_type_map: Dict[str, str]) -> str:
        """Normalize value based on field type for comparison"""
        if not value or not isinstance(value, str):
            return value
        
        field_type = field_type_map.get(field_name, 'string')
        return self.normalize_string_field(value, field_type)

    def reconcile_data(self, job: ReconciliationJob, source_data: List[Dict], target_data: List[Dict]) -> Dict[str, List]:
        """
        Perform data reconciliation between source and target datasets.
        
        Args:
            job: ReconciliationJob instance with ruleset and match key
            source_data: Source CSV data as list of dictionaries
            target_data: Target CSV data as list of dictionaries
            
        Returns:
            Dictionary containing matched, unmatched_source, and unmatched_target records
            
        Raises:
            ValueError: If data is invalid or match key is missing
        """
        if not source_data or not target_data:
            raise ValueError("Empty data sets")

        if not job.ruleset:
            self.logger.warning(f"Job {job.id} has no ruleset - cannot perform reconciliation")
            raise ValueError("Job doesn't have a specified ruleset")

        source_keys = set(source_data[0].keys()) if source_data else set()
        target_keys = set(target_data[0].keys()) if target_data else set()

        match_key = job.ruleset.match_key
        if match_key not in source_keys:
            raise ValueError(f"Match key '{match_key}' not found in source data")
        if match_key not in target_keys:
            raise ValueError(f"Match key '{match_key}' not found in target data")
        
        self.logger.info(f"Using match key '{match_key}' for reconciliation")
        
        field_type_map = self.get_field_type_map(job)
        
        source_dict = {}
        target_dict = {}
        
        for row in source_data:
            normalized_key = self.normalize_value_for_comparison(row[match_key], match_key, field_type_map)
            source_dict[normalized_key] = row
            
        for row in target_data:
            normalized_key = self.normalize_value_for_comparison(row[match_key], match_key, field_type_map)
            target_dict[normalized_key] = row
        
        results = {
            'matched': [],
            'unmatched_source': [],
            'unmatched_target': []
        }
        
        all_keys = set(source_dict.keys()) | set(target_dict.keys())
        
        for key in all_keys:
            source_row = source_dict.get(key)
            target_row = target_dict.get(key)
            
            if source_row and target_row:
                # Found in both - check for differences
                differences = self._compare_records(source_row, target_row, source_keys.union(target_keys), field_type_map)
                
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

    def _compare_records(self, source_row: Dict, target_row: Dict, all_fields: set, field_type_map: Dict[str, str]) -> Dict[str, Dict]:
        """
        Compare two records field by field and return differences.
        
        Args:
            source_row: Source record data
            target_row: Target record data
            all_fields: All possible fields from both records
            field_type_map: Mapping of field names to data types
            
        Returns:
            Dictionary of differences by field name
        """
        differences = {}
        
        for field in all_fields:
            source_val = source_row.get(field, '')
            target_val = target_row.get(field, '')
            
            normalized_source = self.normalize_value_for_comparison(source_val, field, field_type_map)
            normalized_target = self.normalize_value_for_comparison(target_val, field, field_type_map)
            
            if normalized_source != normalized_target:
                differences[field] = {'source': source_val, 'target': target_val}
        
        return differences

    def calculate_summary(self, job: ReconciliationJob, results: Dict[str, List]) -> Dict[str, Any]:
        """
        Calculate reconciliation summary statistics.
        
        Args:
            job: ReconciliationJob instance
            results: Reconciliation results
            
        Returns:
            Dictionary containing summary statistics
        """
        return {
            'total_source_records': job.source_record_count,
            'total_target_records': job.target_record_count,
            'matched_records': len(results['matched']),
            'unmatched_source_records': len(results['unmatched_source']),
            'unmatched_target_records': len(results['unmatched_target']),
            'match_percentage': round(
                (len(results['matched']) / max(job.source_record_count, job.target_record_count)) * 100, 2
            ) if job.source_record_count and job.target_record_count else 0
        }