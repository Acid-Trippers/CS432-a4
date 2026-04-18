# Installation and Run Guide

This project is packaged to run in a Docker-first workflow.

## 1. Prerequisites

- Python 3.11+
- Docker Desktop (or Docker Engine + Docker Compose)
- Free ports: 5432, 27017, 8000, 8080

## 2. Clone and Install Dependencies

```bash
git clone <your-repo-url>
cd CS432-a4
pip install -r requirements.txt
```

Optional virtual environment:

```bash
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
# Linux/macOS
source .venv/bin/activate
```

## 3. Configure Environment (Optional)

Defaults work for assignment use.

```bash
cp .env.example .env
```

Update values in .env only if your host uses different credentials/ports.

## 4. Start Infrastructure

```bash
docker-compose up -d
```

Check containers:

```bash
docker-compose ps
```

## 5. Launch Dashboard

```bash
python dashboard/run.py
```

Open http://127.0.0.1:8080/

## 6. Initialize Pipeline

1. Open setup page at http://127.0.0.1:8080/setup
2. Login with admin password (default: admin123 unless overridden)
3. Paste schema from examples/sample_schema.json and save
4. Trigger initialize and fetch from dashboard controls, or call endpoints directly:

```bash
curl -c cookies.txt -X POST "http://127.0.0.1:8080/api/auth/admin/login" -H "Content-Type: application/json" -d '{"password":"admin123"}'
curl -b cookies.txt -X POST "http://127.0.0.1:8080/api/pipeline/initialise?count=1000"
curl -X POST "http://127.0.0.1:8080/api/pipeline/fetch?count=100"
```

## 7. Run Logical Query Interface

Main endpoint:

```text
POST /api/query
```

Example:

```bash
curl -X POST "http://127.0.0.1:8080/api/query" \
  -H "Content-Type: application/json" \
  -d '{"operation":"READ","entity":"main_records","filters":{"city":"Seattle"},"columns":["username","city"]}'
```

## 8. Run Ingestion API Directly (Optional)

Docker mode is started by docker-compose. Local fallback:

```bash
python external/app.py
```

## 9. Stop Services

```bash
docker-compose down
```
