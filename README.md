# CS432 Hybrid SQL + MongoDB Framework

This repository packages the full system as a reproducible software framework with:

- SQL backend (PostgreSQL)
- MongoDB backend
- External ingestion API
- Logical query interface
- Dashboard UI

## Start Here (Fastest Path)

These commands are the primary Docker-first setup flow.

1. Install Python dependencies.

```powershell
pip install -r requirements.txt
```

2. Start backend services (PostgreSQL, MongoDB, ingestion API, pipeline container).

```powershell
docker-compose up -d
```

3. Launch dashboard.

```powershell
python dashboard/run.py
```

4. Open:

```text
http://127.0.0.1:8080/
```

## Assignment Deliverables Coverage

This repository includes what the assignment requires:

- Source code repository (this GitHub repo)
- Setup instructions for dependencies
- SQL and MongoDB backend configuration instructions
- Ingestion API run instructions
- Logical query interface run instructions
- Dashboard launch instructions

## Prerequisites

- Python 3.11+
- Docker Desktop (or Docker Engine + Docker Compose)
- Ports available: 5432, 27017, 8000, 8080

## One-Command Helpers

Use these optional helper scripts if you want guided startup.

Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/start.ps1
```

Linux/macOS/Git Bash:

```bash
bash scripts/start.sh
```

## Dependency Setup (Manual)

Create and activate a virtual environment (recommended):

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Linux/macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Backend Configuration

Environment variables are read from the system environment. Defaults are defined in src/config.py.

- POSTGRES_URI (default: postgresql://admin:secret@localhost:5432/cs432_db)
- MONGO_URI (default: mongodb://admin:secret@localhost:27017/)
- MONGO_DB_NAME (default: cs432_db)
- API_HOST (default: http://127.0.0.1:8000)
- ADMIN_PASS (default: admin123)

Use .env.example as a template when you need non-default values.

## SQL Backend Instructions

1. Start containers:

```powershell
docker-compose up -d
```

2. Verify PostgreSQL service:

```powershell
docker-compose exec postgres psql -U admin -d cs432_db -c "SELECT 1;"
```

3. Pipeline initialization will create/populate SQL-backed tables when you run initialise.

## MongoDB Backend Instructions

1. Start containers:

```powershell
docker-compose up -d
```

2. Verify MongoDB service:

```powershell
docker-compose exec mongo mongosh --username admin --password secret --authenticationDatabase admin --eval "db.adminCommand({ ping: 1 })"
```

3. Hybrid routing writes high-frequency structured data to SQL and flexible/sparse parts to MongoDB.

## Ingestion API Instructions

Docker mode (recommended):

```powershell
docker-compose up -d api
```

Local mode:

```powershell
python external/app.py
```

You normally do not need to call this service directly from the README workflow. The dashboard and pipeline use it through the local stack.

If you want to confirm it is reachable, open:

```text
http://127.0.0.1:8000/
```

## Pipeline Initialization Instructions

Use the bundled sample schema at examples/sample_schema.json (copy from initial_schema.json).

The dashboard setup page can save schema, then initialize data.

Use the webapp flow:

1. Open http://127.0.0.1:8080/setup
2. Log in with the admin password
3. Paste the schema from examples/sample_schema.json and save it
4. Use the setup controls to run initialize and fetch

## Logical Query Interface Instructions

Use the dashboard query interface for day-to-day work. The backend endpoint is still `POST /api/query`, but the expected assignment flow is through the local webapp.

Sample READ request:

```json
{
  "operation": "READ",
  "entity": "main_records",
  "filters": {
    "city": "Seattle"
  },
  "columns": ["username", "city", "subscription"]
}
```

Use that JSON in the dashboard query form to test READ, CREATE, UPDATE, and DELETE.

## Dashboard Instructions

Start dashboard:

```powershell
python dashboard/run.py
```

Open:

```text
http://127.0.0.1:8080/
```

Admin setup page:

```text
http://127.0.0.1:8080/setup
```

## Repository Structure

```text
ACID/           ACID tests and validators
dashboard/      FastAPI app, routers, templates, static assets
docs/           Setup, architecture, and assignment documentation
external/       External ingestion API source
legacy/         Pipeline orchestration entrypoints
src/            Hybrid SQL + MongoDB backend implementation
```

## Additional Documentation

- docs/INSTALL.md
- docs/examples.md
- docs/TROUBLESHOOTING.md
- docs/DOCKER_GUIDE.md
- docs/assignment-3-guidelines.md

## Reproducibility Verification Checklist

1. Clone repository.
2. Install requirements.
3. Start containers with docker-compose up -d.
4. Start dashboard and open port 8080.
5. Confirm SQL and MongoDB connectivity.
6. Run pipeline initialise/fetch.
7. Execute one logical READ query through /api/query.

## Notes

- If you change credentials/hostnames, update environment variables before startup.
- Default admin password is for assignment reproducibility. Change ADMIN_PASS for non-assignment usage.
