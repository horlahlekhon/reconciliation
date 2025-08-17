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


## Requirements
- Python 3.8+
- Both CSV files must have identical headers
