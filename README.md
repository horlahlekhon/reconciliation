# CSV Reconciliation API

Django REST API for comparing CSV files with asynchronous processing.

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Setup Database
```bash
python manage.py migrate
```

### 3. Start Server
```bash
python manage.py runserver
```

Server runs at `http://127.0.0.1:8000/`

## API Documentation
- **Swagger UI**: http://127.0.0.1:8000/swagger/
- **ReDoc**: http://127.0.0.1:8000/redoc/

## How it works

For Every reconciliation Job, we must create a ruleset which is a configuration of field types and their datatypes. 
The ruleset is then sent along with the source and target files for validation.
the /reconcile endpoint returns immediately after validating the the headers match the ruleset then submit the job to a background worker for processing


## Assumptions
- Python 3.8+
- Both CSV files must have identical headers
- The Ruleset matchkey must be primary key as duplicates will be filtered out and the last occurence is selected. this will cause data loss. potential solution is composite keys
- CSV uses standard utf-8 encoding


## Known issues
- Memory issues for very large files - potential solution is chunked processing as opposed to loading the whole file in memory
