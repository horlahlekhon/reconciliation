import os
import re

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "reconciliation.settings")
django.setup()
from reconciliation_app.reconciliation_engine import ReconciliationEngine

if __name__ == "__main__":
    value = "Jane\nSmith"
    rec = ReconciliationEngine()
    result = rec.normalize_string_field(value=value, field_type="string")
    print(f"before: |{value}|\t -> after |{result}|")

    # numerics = [
    #     '$1,234.56',
    #     '€1.234,56',
    #     '45%',
    #     '1.23e4',
    #     '1.23E+6',
    #     '1,234,567.89',
    #     '(1,234.56)',
    # ]
    # for test_case in numerics:
    #     result = rec.normalize_string_field(value=test_case, field_type='float')
    #     print(f"before: |{test_case}|\t -> after |{result}|")

    date = "15-01-2024"
    result = rec.validate_datetime(date=date, data_type="date")
    print(f"before: |{date}|\t -> after |{result}|")
