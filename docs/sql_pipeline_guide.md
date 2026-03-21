# Docker Setup & SQL Pipeline Guide

## Prerequisites
- Docker Desktop installed and running
- Python 3.11+ with dependencies installed (`pip install -r requirements.txt`)

---

## Step 1 — Run the Pipeline Locally

This generates all the data files needed before inserting into the database.

```powershell
python main.py initialise 500
```

When prompted for schema, enter `1` to use the existing schema or `2` to paste a new one.

---

## Step 2 — Start Docker Containers

```powershell
docker compose up -d
```

This starts all 4 containers:
| Container | Role | Port |
|---|---|---|
| `cs432_postgres` | PostgreSQL database | 5432 |
| `cs432_mongo` | MongoDB database | 27017 |
| `cs432_api` | Faker data API | 8000 |
| `cs432_pipeline` | Python pipeline runner | — |

Wait about 10 seconds for PostgreSQL to finish initializing before proceeding.

---

## Step 3 — Insert Data into PostgreSQL

```powershell
docker compose exec pipeline python -m src.sql_pipeline run
```

Expected output:
```
[STEP 1] Initializing SQL Schema...
[STEP 2] Loading SQL Data...
[STEP 3] Bulk Inserting Records...
[STEP 4] Archiving Processed Data...

SQL PIPELINE SUMMARY
  Successful Inserts : 500
  Failed Inserts     : 0
  Total Records in Database: 500
```

---

## Step 4 — Verify Data in PostgreSQL

### Connect to psql
```powershell
docker compose exec postgres psql -U admin -d cs432_db
```

### Useful commands (run one at a time inside psql)

List all tables:
```sql
\dt
```

View table schema:
```sql
\d main_records
```

Count total records:
```sql
SELECT COUNT(*) FROM main_records;
```

View first 5 records:
```sql
SELECT * FROM main_records LIMIT 5;
```

View specific columns:
```sql
SELECT record_id, username, city, subscription FROM main_records LIMIT 10;
```

Exit psql:
```sql
\q
```

---

## Step 5 — Check Pipeline Status

```powershell
docker compose exec pipeline python -m src.sql_pipeline status
```

---

## Fetching More Records

To ingest additional records into the existing database:

```powershell
# Fetch 500 more records locally
python main.py fetch 500

# Insert the new batch into PostgreSQL
docker compose exec pipeline python -m src.sql_pipeline run
```

---

## Clearing All Records

### Option 1 — Wipe database only (keeps containers running)
```powershell
docker compose exec postgres psql -U admin -d cs432_db -c "TRUNCATE main_records CASCADE;"
```

### Option 2 — Full reset (wipes database volume and restarts everything)
```powershell
docker compose down -v
docker compose up -d
```

After a full reset, re-run Step 3 to re-insert data.

---

## Stopping Containers

Stop containers but keep data:
```powershell
docker compose down
```

Stop containers and wipe all data:
```powershell
docker compose down -v
```