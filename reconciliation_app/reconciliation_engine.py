import logging
import re
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional

from price_parser import Price

from .models import ReconciliationJob

logger = logging.getLogger('reconciliation_app.reconciliation')

URL_PATTERN = r'^https?://[^\s/$.?#].[^\s]*$'
EMAIL_PATTERN = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
PHONE_PATTERN = r'^[\+]?[1-9]?[\d\s\-\(\)\.]{7,15}$'

DATE_FORMATS = [
    '%Y-%m-%d',
    '%Y/%m/%d',
    '%m/%d/%Y',
    '%d/%m/%Y',
    '%d-%-m-%Y',
    '%Y%m%d',
    '%d.%m.%Y',
    '%d-%m-%Y',
    '%Y-%m-%d %H:%M:%S',
    '%d-%m-%Y %H:%M:%S',
    '%Y-%m-%dT%H:%M:%S',
    '%Y-%m-%d %H:%M:%S.%f'
]


BOOLEAN_OPTIONS = ['true', 'false', '1', '0', 'yes', 'no']

class ReconciliationEngine:

    def __init__(self):
        self.logger = logger
    
    def normalize_number_field(self, value: str, field_type: str = 'float') -> float | int | str:
        """
        Normalize numeric fields to handle different formatting conventions and return actual numeric values.
        - Remove currency notations
        - Handle scientific notation, decimal notation, and locale-specific separators
        - Return actual numeric type (int/float) for consistent comparison
        
        Args:
            value: The string value to normalize
            field_type: The type of field ('integer', 'float')
        
        Returns:
            Normalized numeric value as int, float, or original string if not numeric
        """
        if not value or not isinstance(value, str):
            return value
        
        normalized = value.strip()
        
        if not normalized:
            return normalized
        
        try:
            price = Price.fromstring(normalized)
            if price.amount is not None:
                if field_type == 'integer':
                    return int(price.amount)
                else:
                    return float(price.amount)
        except (ValueError, AttributeError):
            pass
        
        try:
            fallback = re.sub(r'[$£€¥₹₽¢₩₪₨₦₡%]', '', normalized)
            fallback = fallback.replace(' ', '')
            
            if field_type == 'integer':
                return int(float(fallback))
            else:
                return float(fallback)
        except (ValueError, OverflowError):
            return normalized


    def normalize_boolean(self, value: str) -> Optional[bool]:
        if not value:
            return None
        
        normalized = str(value).strip().lower()
        
        true_values = {
            'true', 't', 'yes', 'y', '1', 'on', 'enable', 'enabled', 
            'active', 'positive', 'ok', 'okay', 'correct', 'right',
            'valid', 'good', 'success', 'pass', 'passed', 'approve', 
            'approved', 'accept', 'accepted', 'confirm', 'confirmed'
        }
        
        false_values = {
            'false', 'f', 'no', 'n', '0', 'off', 'disable', 'disabled',
            'inactive', 'negative', 'wrong', 'incorrect', 'invalid', 
            'bad', 'fail', 'failed', 'failure', 'error', 'reject', 
            'rejected', 'deny', 'denied', 'cancel', 'cancelled'
        }
        
        if normalized in true_values:
            return True
        elif normalized in false_values:
            return False
        else:
            return None

    def normalize_string_field(self, value: str, field_type: str = 'string') -> Any:
        """
        Normalize string fields for comparison. We convert everything to lower case and remove
        trailing and leading whitespaces including non visible special characters like \r
        
        Args:
            value: The string value to normalize
            field_type: The type of field ('email', 'phone', 'string', 'integer', 'float')
        
        Returns:
            Normalized string value
        """
        if not value or not isinstance(value, str):
            return value
        
        # Handle numeric types with special normalization
        if field_type in ['integer', 'float']:
            return self.normalize_number_field(value, field_type)
        
        normalized = value.replace('\n', ' ').replace('\r', '').lower().strip()
        
        if field_type == 'email':
            return normalized
        elif field_type == 'phone':
            normalized = re.sub(r'[\s\-\(\)\.\+]+', '', normalized)
            return normalized
        elif field_type in ['date', 'datetime']:
            _, _, date = self.validate_datetime(value, field_type)
            return date
        elif field_type == 'boolean':
            return self.normalize_boolean(value)
        else:
            normalized = re.sub(r'\s+', ' ', normalized)
            return normalized

    def validate_datetime(self, date: str | datetime, data_type: str) -> Tuple[bool, str, datetime]:
        """Validate datetime against multiple formats"""
        if isinstance(date, datetime):
            return True, "", date
        parsed = False
        value = None
        for fmt in DATE_FORMATS:
            try:
                value = datetime.strptime(date, fmt)
                parsed = True
                break
            except ValueError:
                continue
        
        if not parsed:
            return parsed, f"Invalid datetime format '{date}'", value
        return parsed, "", value

    def validate_field_value(self, value: Any, data_type: str) -> Optional[str]:
        """
        Validate a field value against its expected data type.
        Expects normalized values for proper validation.
        
        Args:
            value: The normalized value to validate
            data_type: Expected data type
            
        Returns:
            Error message if validation fails, None if valid
        """
        # Allow empty values for all field types
        if not value:
            return None
        
        if isinstance(value, str):
            value = value.strip()
        
        if not value:
            return None
        
        try:
            if data_type == 'integer':
                int(value)
            elif data_type == 'float':
                float(value)
            elif data_type == 'boolean':
                if value is None:
                    return f"Invalid boolean '{value}'"
            elif data_type in ['date', 'datetime']:
                parsed, error, _ = self.validate_datetime(value, data_type)
                if not parsed:
                    return error
            elif data_type == 'email':
                if not re.match(EMAIL_PATTERN, value):
                    return f"Invalid email format '{value}'"
            elif data_type == 'phone':
                if not re.match(r'^\d+$', value):
                    return f"Invalid phone format '{value}' (should contain only digits after normalization)"
            elif data_type == 'url':
                if not re.match(URL_PATTERN, value):
                    return f"Invalid URL format '{value}'"
        except ValueError as e:
            return f"Invalid {data_type} value '{value}': {str(e)}"
        except OverflowError:
            return f"Number too large for {data_type}: '{value}'"
        
        return None

    def validate_csv_data(self, job: ReconciliationJob, source_data: List[Dict], target_data: List[Dict]) -> List[Dict[str, str]]:
        """
        Validate CSV data against ruleset definitions with value normalization.
        Limits errors to 100 to prevent too much output.
        
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
                    data_type = field_type_map[field_name]
                    
                    if value and isinstance(value, str):
                        normalized_value = self.normalize_string_field(value, data_type)
                    else:
                        normalized_value = value
                    
                    error = self.validate_field_value(normalized_value, data_type)
                    if error:
                        d = {
                            "row": i,
                            "field": field_name,
                            "error": error,
                            "original_value": value,
                            "normalized_value": normalized_value,
                            "where": "source file"
                        }
                        validation_errors.append(d)
                        error_count += 1
                        if error_count >= 100:
                            break
        
        # Validate target data with normalization
        for i, row in enumerate(target_data, 1):
            if error_count >= 100:
                break
            
            for field_name, value in row.items():
                if field_name in field_type_map:
                    data_type = field_type_map[field_name]
                    
                    # Normalize value before validation
                    if value and isinstance(value, str):
                        normalized_value = self.normalize_string_field(value, data_type)
                    else:
                        normalized_value = value
                    
                    error = self.validate_field_value(normalized_value, data_type)
                    if error:
                        d = {
                            "row": i,
                            "field": field_name,
                            "error": error,
                            "original_value": value,
                            "normalized_value": normalized_value,
                            "where": "target file"
                        }
                        validation_errors.append(d)
                        error_count += 1
                        if error_count >= 100:
                            break
        
        return validation_errors

    def get_field_type_map(self, job: ReconciliationJob) -> Dict[str, str]:
        """Get field type mapping from ruleset"""
        if not job.ruleset:
            return {}
        return {field.field_name: field.data_type for field in job.ruleset.fields.all()}

    def normalize_value_for_comparison(self, value: str, field_name: str, field_type_map: Dict[str, str]) -> str | datetime:
        """Normalize value based on field type"""
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
        
        field_and_datatype_map = self.get_field_type_map(job)
        
        source_dict = {}
        target_dict = {}
        
        for row in source_data:
            normalized_key = self.normalize_value_for_comparison(row[match_key], match_key, field_and_datatype_map)
            source_dict[normalized_key] = row
            
        for row in target_data:
            normalized_key = self.normalize_value_for_comparison(row[match_key], match_key, field_and_datatype_map)
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
                differences = self._compare_records(source_row, target_row, source_keys.union(target_keys), field_and_datatype_map)
                
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

    def summary(self, job: ReconciliationJob, results: Dict[str, List]) -> Dict[str, Any]:
        """
        Calculate and return a summary of the reconciliation.
        
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
