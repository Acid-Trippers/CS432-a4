# CS432 Assignment 3 Dashboard

_(CS432 - Databases Assignment 3: Logical Dashboard & Transactional Validation)_

This repository exposes the hybrid SQL + MongoDB system through a FastAPI dashboard. The dashboard presents logical entities, query results, and ACID validation output without showing backend-specific storage details.

Demo video: add your submission link here.

## What This Project Includes

- A logical dashboard at the application root.
- Dashboard APIs for running ACID validation tests.
- The underlying hybrid database engine from Assignment 2, reused as the data layer.

## How To Run The Dashboard

1. Install dependencies.

```powershell
pip install -r requirements.txt
```

2. Start backend services first (required before pipeline endpoints).

```powershell
docker-compose up -d
```

This starts PostgreSQL, MongoDB, and the external ingestion API. The pipeline endpoints (`/api/pipeline/schema`, `/api/pipeline/initialise`, `/api/pipeline/fetch`) depend on those services being reachable.

3. Start the dashboard server.

```powershell
.venv/bin/python dashboard/run.py
```

If you are using an activated virtual environment, `python dashboard/run.py` works as well.

4. Open the dashboard in your browser.

```text
http://127.0.0.1:8080/
```

If the external API is unreachable, the dashboard displays a warning banner. The same signal is available in `GET /api/stats` via `external_api_reachable`.

## ACID Test Endpoints

The dashboard also exposes ACID validation routes under `/api/acid`:

- `GET /api/acid/all` runs the full ACID suite.
- `GET /api/acid/{test_name}` runs a single basic ACID test such as `atomicity` or `consistency`.
- `GET /api/acid/advanced/{test_name}` runs a single advanced validation test.

## Project Structure

```text
cs432-assignment3/
├── ACID/               # ACID validation runners and validators
├── dashboard/          # FastAPI app, templates, static assets, and routes
├── data/               # Pipeline and transaction artifacts
├── docs/               # Assignment and architecture documentation
├── src/                # Hybrid SQL + MongoDB engine
├── main.py             # Core pipeline entrypoint
├── starter.py          # Environment bootstrapper for the backend stack
└── requirements.txt    # Python dependencies
```

## Documentation

- [Assignment 3 Guidelines](docs/assignment-3-guidelines.md)
- [ACID Testing Report](ACID/TEST_REPORT.md)
- [SQL Engine Architecture](docs/SQL_ENGINE_ARCHITECTURE.md)

## Notes

The dashboard is served by `dashboard/run.py`, which launches Uvicorn on port `8080` and loads the FastAPI app defined in `dashboard/app.py`.

## READ Query Filter Syntax (Advanced)

The `READ` operation now supports both simple equality filters and advanced typed filters with logical grouping.

### Base Request Shape

```json
{
	"operation": "READ",
	"entity": "main_records",
	"filters": {},
	"columns": ["username", "city", "subscription"]
}
```

### 1. Simple Equality (Backward Compatible)

```json
{
	"operation": "READ",
	"entity": "main_records",
	"filters": {
		"city": "Seattle",
		"subscription": "premium"
	},
	"columns": ["record_id", "username", "city", "subscription"]
}
```

This means `city == "Seattle" AND subscription == "premium"`.

### 2. Numeric Comparisons (Integer/Float fields)

Use operator objects:

```json
{
	"filters": {
		"age": {"$gte": 18, "$lte": 65},
		"battery": {"$gt": 30}
	}
}
```

Or shorthand strings (numeric fields only):

```json
{
	"filters": {
		"age": ">= 80",
		"altitude": "< 500.5"
	}
}
```

### 3. String Filters

```json
{
	"filters": {
		"username": {"$contains": "sam"},
		"city": {"$starts_with": "San"},
		"email": {"$ends_with": "@example.com"},
		"country": {"$regex": "^Uni"}
	}
}
```

### 4. Logical Grouping (`$and`, `$or`, `$not`)

```json
{
	"operation": "READ",
	"entity": "main_records",
	"filters": {
		"$and": [
			{"age": {"$gte": 21}},
			{
				"$or": [
					{"city": "Boston"},
					{"city": "Chicago"}
				]
			},
			{
				"$not": {
					"subscription": "free"
				}
			}
		]
	},
	"columns": ["username", "age", "city", "subscription"]
}
```

### 5. Set and Presence Operators

```json
{
	"filters": {
		"country": {"$in": ["India", "USA"]},
		"status": {"$nin": ["inactive", "blocked"]},
		"phone": {"$exists": true}
	}
}
```

### Supported Operators

- `$eq`, `$ne`
- `$gt`, `$gte`, `$lt`, `$lte` (numeric fields only)
- `$in`, `$nin` (array value required)
- `$contains`, `$starts_with`, `$ends_with`, `$regex` (string fields only)
- `$exists` (boolean value required)

### Validation Rules

- `filters` must be a JSON object.
- Do not mix logical operators and direct field keys at the same object level.
- Only one logical operator key is allowed per object level.
- `$and` and `$or` require non-empty arrays.
- `$not` requires one nested filter object.
- Numeric operators are only valid on numeric fields.
- String operators are only valid on string fields.
- Invalid filter syntax returns a failed `READ` response with an `error` message.
