# A2_BASELINE: Complete Systems Reference

**Document Purpose:** Agents implementing A3 must use this document instead of reading source code directly. This is the ground truth.

**Methodology:** All statements derived from code analysis, not inference. Ambiguities explicitly marked `[AMBIGUOUS]`.

---

## 1. Global Facts

### 1.1 Ports in Use

| Port | Service | Container | Source |
|------|---------|-----------|--------|
| 5432 | PostgreSQL | `cs432_postgres` | docker-compose.yml |
| 27017 | MongoDB | `cs432_mongo` | docker-compose.yml |
| 8000 | FastAPI (external/app.py) | `cs432_api` | docker-compose.yml |

**Important:** Docker compose runs all three. `Pipeline` container depends_on all three before starting.

### 1.2 Environment Variables

| Variable | Default | Where Read | Type |
|----------|---------|-----------|------|
| `POSTGRES_URI` | `postgresql://admin:secret@localhost:5432/cs432_db` | src/config.py line 23 | Env or hardcoded default |
| `MONGO_URI` | `mongodb://admin:secret@localhost:27017/` | src/config.py line 25 | Env or hardcoded default |
| `MONGO_DB_NAME` | `cs432_db` | src/config.py line 26 | Env or hardcoded default |
| `API_HOST` | `http://127.0.0.1:8000` | src/config.py line 29 | Env or hardcoded default |

**Reading Pattern:** `os.environ.get("VAR_NAME", "default")`

**Docker Container Overrides:**
- Pipeline container sets: `POSTGRES_URI=postgresql://admin:secret@postgres:5432/cs432_db` (uses container DNS)
- Pipeline container sets: `MONGO_URI=mongodb://admin:secret@mongo:27017/`
- Pipeline container sets: `API_HOST=http://api:8000` (uses container DNS)

### 1.3 All File Paths (I/O Summary)

| File Path | Purpose | Written By | Read By | Format |
|-----------|---------|-----------|---------|--------|
| `data/counter.txt` | Global record ID counter | main.py `process_in_memory()` | 01_ingestion.py, main.py, CRUD_operations.py | Integer (newline-terminated) |
| `data/received_data.json` | Raw API output before cleaning | 01_ingestion.py | main.py `process_in_memory()`, 02_cleaner.py | JSON array |
| `data/cleaned_data.json` | Records post-cleaning | main.py `process_in_memory()` | 03_analyzer.py, 06_router.py, main.py | JSON array |
| `data/buffer.json` | Unmapped/quarantined fields | 02_cleaner.py | 06_router.py, main.py | JSON array of `{parent_record_id, field, value}` |
| `data/analyzed_schema.json` | Statistical profiling output | 03_analyzer.py | 04_metadata_builder.py, 05_classifier.py | JSON with field frequency/type/stability/cardinality |
| `data/metadata.json` | Final routing decisions (SQL/MONGO/UNKNOWN) | 04_metadata_builder.py, 05_classifier.py | 06_router.py, CRUD_operations.py, CRUD_runner.py | JSON with decision/confidence/reason per field |
| `data/sql_data.json` | Records routed to SQL | 06_router.py | sql_engine.py, sql_pipeline.py | JSON array |
| `data/mongo_data.json` | Records routed to MongoDB | 06_router.py | mongo_engine.py | JSON array |
| `data/unknown_data.json` | Records in UNKNOWN buffer | 06_router.py, CRUD_operations.py | CRUD_operations.py | JSON array |
| `data/pipeline_checkpoint.json` | Last completed pipeline stage | main.py `set_checkpoint()` | main.py `get_last_checkpoint()` | `{last_step: string, timestamp: float}` |
| `data/query.json` | User-supplied CRUD request | CRUD_json_reader.py | CRUD_runner.py | JSON with `{operation, entity, filters, payload}` |
| `data/query_output.json` | Result of query execution | CRUD_runner.py | User (read-only) | JSON with operation result |
| `data/transaction_log.json` | Saga transaction events | transaction_coordinator.py | (read-only audit) | JSON array of transactions |
| `data/pipeline_failures.json` | Error recovery log | main.py `append_pipeline_failure()` | (read-only audit) | JSON array of failure events |
| `data/initial_schema.json` | User-defined schema template | starter.py | 02_cleaner.py | JSON tree (mirror structure) |
| `data/data_till_now_sql.json` | Archive of successfully inserted SQL records | sql_pipeline.py | (read-only archive) | JSON array |

### 1.4 Python Version & Dependencies

**Version:** Python 3.11 (from Dockerfile: `python:3.11-slim`)

**All Dependencies** (from requirements.txt):
```
annotated-types==0.7.0
anyio==4.12.1
certifi==2026.2.25
click==8.3.1
colorama==0.4.6
Faker==40.11.0                  # Data generation for external/app.py
fastapi==0.135.1                # FastAPI server for external/app.py
greenlet==3.3.2                 # SQLAlchemy async support
h11==0.16.0
httpcore==1.0.9
httpx==0.28.1                   # Async HTTP client (ingestion.py)
idna==3.11
psycopg2-binary==2.9.9          # PostgreSQL adapter (sql_engine.py)
pymongo==4.7.2                  # MongoDB client (mongo_engine.py, CRUD_operations.py)
pydantic==2.12.5                # FastAPI validation
pydantic_core==2.41.5
SQLAlchemy==2.0.48              # SQL ORM (sql_schema_definer.py, sql_engine.py)
sse-starlette==3.3.2            # Server-sent events for /record/{count} streaming
starlette==0.52.1               # ASGI framework (FastAPI dependency)
typing-inspection==0.4.2
typing_extensions==4.15.0
tzdata==2025.3
uvicorn==0.42.0                 # ASGI server (external/app.py, main.py via subprocess)
```

**Critical Import at Runtime:**
- sql_engine.py imports SQLAlchemy — if unavailable, prints error and raises
- mongo_engine.py imports pymongo — instantiates MongoClient at module level (side effect)
- ingestion.py imports httpx — async HTTP calls to API

---

## 2. Entry Points

### 2.1 Invocation Methods

**Method 1: Main Pipeline**
```bash
python main.py initialise 1000    # Run full pipeline with 1000 records
python main.py fetch 100          # Incremental ingest of 100 records
python main.py query              # Launch CRUD query interpreter
python main.py resume             # Resume from last checkpoint
```

**Method 2: Docker**
```bash
python starter.py start           # Start PostgreSQL, MongoDB, API
python starter.py end             # Stop containers
docker-compose up -d              # Direct docker-compose command (not recommended)
```

**Method 3: External API**
```bash
python external/app.py            # Starts FastAPI on port 8000
# GET http://localhost:8000/ → single record
# GET http://localhost:8000/record/100 → stream 100 records (Server-Sent Events)
```

**Method 4: ACID Tests (A3 only, not A2)**
```bash
python -m ACID.runner --test atomicity
python -m ACID.runner --test all
python -m ACID.runner --test advanced
```

### 2.2 Full Initialise Flow (`main.py initialise count`)

1. **API Startup** (main.py `start_api()`)
   - Spawns `external/app.py` in subprocess
   - Polls http://localhost:8000/ until responds (timeout: 30 sec default, set by project_config.API_STARTUP_TIMEOUT)
   - If timeout → prints "[X] API server failed to start" → terminates subprocess → exits

2. **File Cleanup** (main.py `initialise()`)
   - Deletes: counter.txt, received_data.json, cleaned_data.json, buffer.json, analyzed_schema.json, metadata.json, sql_data.json, mongo_data.json, query.json, pipeline_checkpoint.json, pipeline_failures.json
   - Recreates counter.txt with "0"
   - Calls `clean_databases()`: drops MongoDB database, drops PostgreSQL public schema

3. **Schema Definition** (main.py calls schema_definition.main())
   - If initial_schema.json exists → validates it
   - If not → enters paste mode (stdin.read() → blocks for user input)
   - On exit: initial_schema.json written
   - Code at: `src/phase_1_to_4/00_schema_definition.py`, function `main()`

4. **Data Ingestion** (main.py calls ingestion.fetch_data(count))
   - Makes async HTTP call to `http://127.0.0.1:8000/record/{count}` (from project_config.API_HOST)
   - Parses Server-Sent Events: splits on "data: " prefix, JSON-decodes each record
   - **CRITICAL SIDE EFFECT:** Records ingested timestamp: `record['sys_ingested_time'] = datetime.now().isoformat()`
   - Increments counter file for each 100 records fetched
   - Saved to: received_data.json
   - Code at: `src/phase_1_to_4/01_ingestion.py`, function `fetch_data(num)`
   - **Blocks on:** httpx.AsyncClient timeout (INGESTION_TIMEOUT = 30 sec)

5. **In-Memory Processing** (main.py `process_in_memory(raw_records, is_fetch=False)`)
   - **Phase 5a: Data Cleaning** (02_cleaner.py)
     - For each raw record: `cleaner.clean_recursive(record, schema, ref_id)`
     - Assigns sequential `record_id = offset + i` (offset read from counter.txt)
     - Saves to cleaned_data.json, appends to buffer.json
   - **Phase 5b: Data Analysis** (03_analyzer.py)
     - Analyzes cleaned records for frequency, type, cardinality, nesting depth
     - Saves to analyzed_schema.json
   - **Side Effect:** Flushes received_data.json to empty array
   - **Side Effect:** Updates counter.txt with new total

6. **Metadata Building** (main.py calls metadata_builder.merge_metadata())
   - Flattens initial_schema.json to dot-notation paths
   - Merges with analyzed_schema.json (call with `is_update=False` for initialise)
   - Flags rare fields (freq < 0.1) as `is_discovered_buffer: true` if not in initial schema
   - Saves to metadata.json
   - Code at: `src/phase_1_to_4/04_metadata_builder.py`, function `merge_metadata(is_update=False)`

7. **Classification** (main.py calls classifier.run_classification(verbose=True))
   - **Pass 1:** Each field evaluated statistically
     - If `is_discovered_buffer=true` → routed to BUFFER
     - If frequency < 0.01 → routed to UNKNOWN
     - Else if stability ≥ 1.0 AND frequency ≥ 0.5 → SQL
     - Else → MONGO
   - **Pass 2:** Deep nesting rule applied
     - If internal nesting > 2 levels → forced to MONGO
     - Children inherit parent's Mongo exile
   - Prints verbose table, updates metadata.json
   - Code at: `src/phase_1_to_4/05_classifier.py`, function `runPipeline(verbose=True)`

8. **Routing** (main.py calls data_router.route_data())
   - Builds field routes from metadata decisions
   - Iterates cleaned_data.json, splits each record into sql_rec, mongo_rec, unknown_rec
   - Flushes cleaned_data.json to empty array
   - Saves: sql_data.json, mongo_data.json, buffer.json (unknown)
   - Prints stats (sql count, mongo count, buffer count)
   - Code at: `src/phase_1_to_4/06_router.py`, function `route_data()`

9. **SQL Storage** (main.py calls run_storage_with_safety → sql_pipeline.run_sql_pipeline(engine))
   - Initializes SQLEngine: calls sql_schema_definer.analyze_and_build()
     - Loads metadata.json
     - Creates SQLAlchemy models (main_records + nested/array tables)
     - Creates PostgreSQL tables
   - Bulk inserts from sql_data.json
   - On failure: calls compensate_sql_batch(record_ids) → deletes rows by record_id
   - Archives to data_till_now_sql.json (only if ALL records succeed)
   - Clears sql_data.json on success
   - Code at: `src/phase_5/sql_pipeline.py`, function `run_sql_pipeline(engine)`

10. **MongoDB Storage** (main.py calls run_storage_with_safety → mongo_engine.runMongoEngine())
    - Connects to MongoDB
    - For each record in mongo_data.json:
      - Calls `processNode(record, "", db, strategyMap)` to decompose nested fields
      - Performs UPSERT (not insert) by `_id = record_id` to handle fetch runs
      - Extracts `record_id` field, stores as `_id` in MongoDB
    - On failure: calls compensate_mongo_batch(record_ids) → deletes docs
    - Returns (success_count, fail_count)
    - Code at: `src/phase_5/mongo_engine.py`, function `runMongoEngine()`

11. **Completion**
    - API subprocess terminated

**Data Flow Summary for Initialise:**
```
API → received_data.json → cleaned_data.json → analyzed_schema.json (+buffer.json)
→ metadata.json → sql_data.json + mongo_data.json → SQL tables + MongoDB collections
```

### 2.3 Full Fetch Flow (`main.py fetch count`)

Requires: metadata.json must exist (from prior initialise)

1. **Batch Loop** (main.py `fetch(count)`)
   - Splits `count` into batches of 100
   - For each batch:

2. **Read Counter** (before ingestion)
   - `n_old = counter.txt` value

3. **Ingest Batch** (same as initialise step 4)
   - HTTP GET /record/{batch_size}
   - Saves to received_data.json (cumulative, not flushed)

4. **Process Batch** (main.py `process_in_memory(raw_records, is_fetch=True)`)
   - Assignments: `record_id = offset + i` (where offset = `n_old - len(raw_records)`)
   - **APPEND MODE:** saves to cleaned_data.json with `append=True`
   - **APPEND MODE:** appends to buffer.json with `append=True`

5. **Update Metadata** (metadata_builder.merge_metadata(is_update=True, n_old, n_new))
   - Loads existing metadata.json
   - For each new field:
     - If already exists: weighted average frequency/stability using (`n_old/(n_old+n_new), n_new/(n_old+n_new)`)
     - If new: added with `is_discovered_buffer` flag if freq < 0.1
   - **Graduation Logic:** If freq >= 0.1, `is_discovered_buffer=False`

6. **Reclassify** (classifier.run_classification(verbose=False))
   - Same rules as step 7 of initialise
   - Prints evolution report (fields that changed routing)
   - Not verbose in fetch mode

7. **Route** (data_router.route_data() — appends to existing shard files)
   - Loads existing sql_data.json, mongo_data.json, buffer.json
   - Appends new shards
   - Returns stats

8. **Store** (same as initialise steps 9–10)
   - SQL + MongoDB inserts/upserts
   - Compensation on failure

**Key Difference from Initialise:** Fetch appends to metadata and output files; initialise overwrites.

### 2.4 Full Query Flow (`main.py query`)

Requires: metadata.json must exist

1. **UI Input** (CRUD_json_reader.py `main()`)
   - If query.json exists: offer validation or paste
   - Else: force paste mode (stdin.read() blocks)
   - Validates structure: must contain `{operation, entity, filters, payload}`
   - Saves to query.json

2. **Parse Query** (CRUD_runner.py `query_parser()`)
   - Reads query.json
   - Extracts: operation, entity, filters, payload
   - Returns dict

3. **Analyze Databases** (CRUD_runner.py `analyze_query_databases(parsed_query)`)
   - For CREATE: analyzes payload fields
   - For READ/DELETE: analyzes filter fields
   - For UPDATE: analyzes both filters and payload
   - Maps each field to database location (SQL, MONGO, Unknown) via metadata.json
   - Returns: `{field_locations: {field→db}, databases_needed: [...]}`

4. **Route Operation** (CRUD_runner.py `query_runner()`)
   - Calls appropriate operation handler based on operation type

5. **CREATE Operation** (CRUD_operations.py `create_operation()`)
   - **Phase 1: Get next record_id** from counter.txt, increment it
   - **Phase 2: Route fields** based on metadata decisions
     - SQL payload: {record_id, SQL fields}
     - Mongo payload: {record_id, MONGO fields}
     - Unknown payload: {record_id, unknown fields}
   - **Phase 3: Execute Transaction**
     - TransactionCoordinator runs saga with compensation
     - SQL: calls sql_engine.insert_record(sql_payload)
     - MongoDB: calls processNode() then collection.insert_one()
     - Unknown: appends to unknown_data.json (atomic write)
     - If all succeed: returns success
     - If ANY fails: compensation rolls back all (in reverse order)
   - **Result:** Returns `{operation: CREATE, entity, status, transaction_id, details}`

6. **READ Operation** (CRUD_operations.py `read_operation()`)
   - **Phase 1: Two-phase read**
     - Filters SQL fields: query Model.filter(...) to get record_ids
     - Filters MONGO fields: query collection.find() to get _ids
     - Filters Unknown fields: query json file
   - **Phase 2: Fetch full records** by record_id/\_id
     - SQL: SELECT * WHERE record_id IN (...)
     - MongoDB: collection.find({_id: {$in: [...]}})
     - Unknown: json array filter
   - **Phase 3: Merge results**
     - Calls `merge_results_by_record_id()`: combines SQL, MONGO, Unknown by record_id
     - Adds `_source: [SQL, MONGO, Unknown]` field to track origin
   - **Result:** Returns `{operation: READ, entity, data: {record_id→merged}}`

7. **UPDATE Operation** (CRUD_operations.py `update_operation()`)
   - **Phase 1:** Find matching record_ids (same as READ phase 1)
   - **Phase 2:** Update records with new payload
     - SQL: UPDATE WHERE record_id IN (...)
     - MongoDB: collection.update_many(filters, {$set: payload})
     - Unknown: reload json, filter match, update, re-save
   - **Result:** Returns `{operation: UPDATE, entity, updated_count, transaction_id}`

8. **DELETE Operation** (CRUD_operations.py `delete_operation()`)
   - **Phase 1:** Find matching record_ids
   - **Phase 2:** Delete from databases
     - SQL: DELETE WHERE record_id IN (...)
     - MongoDB: collection.delete_many({_id: {$in: [...]}})
     - Unknown: reload json, filter out matches, re-save
   - **Result:** Returns `{operation: DELETE, entity, deleted_count, transaction_id}`

9. **Save Result** (CRUD_runner.py)
   - Writes result dict to query_output.json
   - Prints formatted JSON to console

**[AMBIGUOUS] Unknown Operation Recovery:**  
DELETE_operation() has no explicit transaction coordinator; only SQL/MongoDB get transactional guarantee. Unknown records are deleted directly without saga rollback capability. A3 should define recovery semantics.

---

## 3. Module-by-Module Breakdown

### 3.1 main.py

**Purpose:**  
Master entry point orchestrating the 6-phase pipeline: ingestion → cleaning → analysis → classification → routing → storage. Manages Docker API startup, checkpoint recovery, and CLI dispatch.

**Public Functions:**

| Function | Signature | Input | Output | Side Effects |
|----------|-----------|-------|--------|--------------|
| `initialise(count=1000)` | → None | count: int | Prints progress | Clears files, drops DBs, runs full pipeline |
| `fetch(count=100)` | → None | count: int | Prints progress | Appends to metadata, executes batch pipeline |
| `query()` | → None | (none) | Prints query result | Interactive CLI, reads/writes query.json |
| `main()` | → None | (sys.argv) | Exit code | Dispatches command, starts/stops API |

**Data Reads:**
- Reads: RECEIVED_DATA_FILE, CLEANED_DATA_FILE, COUNTER_FILE, CHECKPOINT_FILE, QUERY_FILE, METADATA_FILE
- Reads environment (implicitly, via src/config imports)

**Data Writes:**
- Writes: All file paths listed in Section 1.3
- Creates/deletes data directory contents

**Hardcoded Values:**
- `CHECKPOINT_FILE = os.path.join(DATA_DIR, "pipeline_checkpoint.json")` (line 21)
- Default batch size in fetch: 100 records (line 283)
- Command choices: "initialise", "fetch", "query", "resume" (line 421)

**Known Issues:**
- **Blocks on stdin():** If no initial_schema.json, calls schema_definition.main() → blocks on user paste input (sys.stdin.read()). Web worker will hang.
- **Subprocess API:** Spawns external/app.py as subprocess; if subprocess crashes, doesn't detect it until making API call. No heartbeat monitoring.
- **Exception Handling:** try/finally in main() catches KeyboardInterrupt but does not log it; just terminates API subprocess.
- **Side Effect at Import:** Line 17–37 uses importlib to dynamically import all phase modules. If any import fails, exit is immediate (no graceful degradation).

### 3.2 project_config.py

**Purpose:**  
CLI and Docker configuration constants.

**Public Variables:**

| Variable | Value | Usage |
|----------|-------|-------|
| DEFAULT_COMMAND | "initialise" | Fallback if no argv[1] |
| INITIALISE_COUNT | 1000 | Default record count for initialise |
| FETCH_COUNT | 100 | Default record count for fetch |
| API_HOST | "127.0.0.1" | Hardcoded for starter.py, Docker overrides via env |
| API_PORT | 8000 | Hardcoded port |
| API_STARTUP_TIMEOUT | 30 | Seconds to wait for API readiness |
| DOCKER_COMPOSE_FILE | "docker-compose.yml" | Relative path for starter.py |
| MONGO_CONTAINER | "mongo" | Container name (for starter.py) |
| POSTGRES_CONTAINER | "postgres" | Container name (for starter.py) |
| DOCKER_STARTUP_TIMEOUT | 20 | Seconds to wait for containers |
| MONGO_HOST | "localhost" | Hostname (not used in docker/code, only docs) |
| MONGO_PORT | 27017 | Port number |
| MONGO_DB_NAME | "hybrid_db" | Database name (legacy, overridden by src/config) |
| PG_HOST | "localhost" | Hostname (not used in code) |
| PG_PORT | 5432 | Port number (not used in code) |
| PG_DB_NAME | "hybrid_db" | Database name (legacy, overridden by src/config) |
| PG_USER | "admin" | Username (legacy, overridden by src/config) |
| PG_PASSWORD | "admin" | Password (legacy, hardcoded in DATABASE_URL env) |

**Known Inconsistencies:**
- MONGO_DB_NAME and PG_DB_NAME are defined here but NOT used; `src/config.py` has the canonical definitions
- Docker compose uses `cs432_db`, project_config uses `hybrid_db` (conflict)

### 3.3 src/config.py

**Purpose:**  
Canonical environment variable and file path configuration.

**Public Variables:**

| Variable | Value (or Default) | Source |
|----------|-------------------|--------|
| ROOT_DIR | `os.path.dirname(os.path.dirname(os.path.abspath(__file__)))` | Computed |
| DATA_DIR | `ROOT_DIR/data` | Computed |
| INITIAL_SCHEMA_FILE | `data/initial_schema.json` | Path |
| RECEIVED_DATA_FILE | `data/received_data.json` | Path |
| CLEANED_DATA_FILE | `data/cleaned_data.json` | Path |
| BUFFER_FILE | `data/buffer.json` | Path |
| ANALYZED_SCHEMA_FILE | `data/analyzed_schema.json` | Path |
| METADATA_FILE | `data/metadata.json` | Path |
| SQL_DATA_FILE | `data/sql_data.json` | Path |
| MONGO_DATA_FILE | `data/mongo_data.json` | Path |
| QUERY_FILE | `data/query.json` | Path |
| QUERY_OUTPUT_FILE | `data/query_output.json` | Path |
| CHECKPOINT_FILE | `data/checkpoint.json` | Path |
| TRANSACTION_LOG_FILE | `data/transaction_log.json` | Path |
| PIPELINE_FAILURE_LOG_FILE | `data/pipeline_failures.json` | Path |
| COUNTER_FILE | `data/counter.txt` | Path |
| DATABASE_URL | Env: `POSTGRES_URI`, default: `postgresql://admin:secret@localhost:5432/cs432_db` | Env or default |
| MONGO_URI | Env: `MONGO_URI`, default: `mongodb://admin:secret@localhost:27017/` | Env or default |
| MONGO_DB_NAME | Env: `MONGO_DB_NAME`, default: `cs432_db` | Env or default |
| API_HOST | Env: `API_HOST`, default: `http://127.0.0.1:8000` | Env or default |

**Side Effect at Import:**
```python
os.makedirs(DATA_DIR, exist_ok=True)  # Line 39
```
Creates /data directory if missing. Runs every time config is imported.

### 3.4 starter.py

**Purpose:**  
Manage Docker container lifecycle independently of pipeline.

**Public Functions:**

| Function | Does |
|----------|------|
| `start()` | Checks Docker running, starts docker-compose, polls ports 5432/27017/8000 for readiness |
| `end()` | Stops docker-compose containers with 5-second graceful timeout |
| `wait_for_port(port, host='localhost', timeout=30)` | Polls TCP socket until open or timeout |

**Hardcoded Paths:**
- `docker-compose.yml` via project_config.DOCKER_COMPOSE_FILE
- Windows only: `C:\Program Files\Docker\Docker\Docker Desktop.exe` (hardcoded, will fail on Mac/Linux)

**Known Issues:**
- **Windows-Specific:** Line 41 hardcodes Windows Docker path; will fail on macOS/Linux if Docker isn't running
- **Blocking Polls:** Calls `time.sleep(1)` in loop (blocking, not async)
- **stderr Piped to stdout:** docker-compose output suppressed completely; errors invisible to user

### 3.5 src/phase_1_to_4/00_schema_definition.py

**Purpose:**  
Validate and persist user-defined JSON schema template.

**Public Functions:**

| Function | Signature | Does |
|----------|-----------|------|
| `validate_structure(data, path="root")` | → raises ValueError | Recursively checks schema adheres to mirror structure rules: dict/list/string only |
| `get_pasted_json()` | → dict or None | Blocks on stdin.read() for JSON paste |
| `main()` | → None | Interactive CLI: offers validation or paste mode |

**Data Operations:**
- Reads: initial_schema.json (if exists)
- Writes: initial_schema.json

**Hardcoded Rules:**
- PRIMITIVES = ["string", "int", "float", "bool"]
- Array must have exactly 1 template element: `len(data) != 1` → raises error

**Blocking Calls:**
- `sys.stdin.read()` (line 36) — blocks indefinitely waiting for Ctrl+D/Ctrl+Z

### 3.6 src/phase_1_to_4/01_ingestion.py

**Purpose:**  
Fetch raw JSON records from external API via HTTP streaming.

**Public Functions:**

| Function | Signature | Returns | Does |
|----------|-----------|---------|------|
| `fetch_data(num)` | async → list[dict] | List of N records | Makes async GET to API_HOST/record/{num}, parses SSE, increments counter |
| `get_counter()` | → int | Current record count | Reads counter.txt |
| `increment_counter(new_counter)` | → None | None | Writes new count to counter.txt |

**HTTP Details:**
- Endpoint: `{API_HOST}/record/{num}` where API_HOST defaults to "http://127.0.0.1:8000"
- Method: GET
- Response: Server-Sent Events, lines prefixed with "data: "
- Parser: `line[6:]` strips "data: " prefix, then `json.loads()`
- Timeout: INGESTION_TIMEOUT = 30 seconds

**Side Effects:**
- Reads API_HOST from env or src/config default
- Increments counter.txt for each 100 records fetched
- **MUTATION:** Adds `sys_ingested_time` to each record: `record['sys_ingested_time'] = datetime.now().isoformat()`

**Data Written:**
- counter.txt (incremented)
- received_data.json (via main.py's save_checkpoint)

**Blocking/Error Handling:**
- `httpx.AsyncClient(timeout=30)` — raises on timeout
- On error: prints warning, returns []

### 3.7 src/phase_1_to_4/02_cleaner.py

**Purpose:**  
Normalize raw JSON to user-defined schema, route unmapped fields to buffer.

**Public Classes:**

| Class | Method | Signature | Does |
|-------|--------|-----------|------|
| DataCleaner | `__init__` | (schema_file) | Loads schema from file, builds canonical_map |
| | `_build_canonical_map` | → None | Recursively maps lower-case keys to canonical keys |
| | `_to_snake_case` | → str | Converts CamelCase to snake_case |
| | `_is_similar` | (name1, name2) → bool | Fuzzy string matching (substring + length tolerance) |
| | `_find_canonical_match` | (field_name, schema_level) → str or None | Finds schema key matching data field (exact, case-insensitive, or fuzzy) |
| | `sanitize_value` | → value | Strips strings, converts empty-like to None |
| | `_try_cast` | (value, expected_type) → value | Casts value to type (int, float, bool, str) |
| | `clean_recursive` | (data, schema, parent_id) → dict | Main cleaning: maps, rips, pads, recurs |

**Data Operations:**
- Reads: INITIAL_SCHEMA_FILE
- Writes: self.buffer (in-memory list)

**Key Algorithm: clean_recursive**
1. For each data_key in data:
   - Find canonical_match in schema
   - If match: cast and recurse (for nested objects/arrays)
   - If no match: append to buffer with parent_record_id
2. For each schema_key NOT in data: skip (allows null fields)

**Buffer Entry Format:**
```python
{"parent_record_id": ref_id, "field": data_key, "value": data_val}
```

**Hardcoded:**
- Type casting thresholds: 'true'/'false'/'yes'/'no' for booleans

### 3.8 src/phase_1_to_4/03_analyzer.py

**Purpose:**  
Profile cleaned records to build frequency/type/cardinality statistics.

**Public Classes:**

| Class | Method | Does |
|-------|--------|------|
| DataAnalyzer | `__init__` | Initializes counters (field_counts, field_types, field_values, nesting_depths) |
| | `_get_type_name` | Returns: 'null', 'boolean', 'integer', 'float', 'string', 'array', 'object' |
| | `_detect_pattern` | Detects regex patterns in strings (e.g., "123-456-7890" → "ddd-ddd-dddd") |
| | `_analyze_recursive` | Walks data structure depth-first, records statistics per field path |
| | `analyze_records` | Iterates all records, calls _analyze_recursive for each |
| | `save_analysis` | Computes frequencies/stabilities, writes analyzed_schema.json |

**Output Format (analyzed_schema.json):**
```json
{
  "total_records": 1000,
  "fields": [
    {
      "field_name": "username",
      "parent_path": null,
      "nesting_depth": 0,
      "frequency": 1.0,
      "dominant_type": "string",
      "type_stability": 1.0,
      "cardinality": 0.001,
      "is_primary_key_candidate": false,
      "is_cardinality_capped": false,
      "is_nested": false,
      "is_array": false,
      "array_content_type": null
    },
    ...
  ]
}
```

**Hardcoded:**
- value_count_limit = 10000 (cardinality tracking limit)

### 3.9 src/phase_1_to_4/04_metadata_builder.py

**Purpose:**  
Merge user schema constraints with statistical analysis; flag probation fields.

**Public Functions:**

| Function | Signature | Does |
|----------|-----------|------|
| `merge_metadata` | (is_update=False, n_old=0, n_new=0) | Flattens initial schema, merges with analyzed, flags BUFFER fields |

**Input Files:**
- initial_schema.json (user-defined tree)
- analyzed_schema.json (statistical profile)
- metadata.json (if is_update=True, for evolutionary merge)

**Output:**
- metadata.json with fields enriched by user_constraints and is_discovered_buffer flag

**Key Logic:**

**Initialise Mode (is_update=False):**
- Flatten initial_schema to dot-notation
- For each analyzed field: add `user_constraints` (if in initial schema)
- Flag `is_discovered_buffer = (freq < 0.1) AND (name not in user_constraints)`

**Update Mode (is_update=True):**
- Reads existing metadata.json
- For each new analyzed field:
  - If already exists: proportional weighted average
    - frequency_new = old_freq * (n_old / total) + new_freq * (n_new / total)
    - type_stability_new = old_stability * (n_old / total) + new_stability * (n_new / total)
    - **Graduation:** If freq >= 0.1, set is_discovered_buffer = False
  - If brand new: added with is_discovered_buffer flag

**[AMBIGUOUS] Buffer Graduation Gap:**  
Historical data for fields that graduate out of buffer is never re-processed. If a field was in buffer with 5 records, then appears in 100 new records and graduates (freq >= 0.1), the original 5 records remain unclassified in buffer.json and are never retrieved. This is a known architectural gap.

### 3.10 src/phase_1_to_4/05_classifier.py

**Purpose:**  
routes fields to SQL, MongoDB, or UNKNOWN (buffer) based on statistical metrics and structural rules.

**Public Functions:**

| Function | Signature | Does |
|----------|-----------|------|
| `runPipeline` | (verbose=True) | Pass 1: statistical routing; Pass 2: deep nesting override; prints results |
| `run_classification` | (verbose=True) | Wraps runPipeline |

**Dataclass: FieldStats**
```python
fieldName: str
frequency: float
dominantType: str
typeStability: float
cardinality: float
isNested: bool
isArray: bool
is_discovered_buffer: bool = False
nestingDepth: int = 0
parentPath: Optional[str] = None
```

**Pass 1: Statistical Classification**

| Condition | Decision | Confidence |
|-----------|----------|-----------|
| is_discovered_buffer=True | BUFFER | 1.0 |
| frequency < 0.01 | UNKNOWN | 1.0 - frequency |
| stability ≥ 1.0 AND freq ≥ 0.5 | SQL | (stability + freq) / 2 |
| else | MONGO | 0.8 |

**Pass 2: Structural Rules (Deep Nesting)**
- Calculates max_descendant_depth for each field
- If internal_nesting_levels > 2: route to MONGO, confidence 1.0
- Children inherit parent's MONGO exile

**Hardcoded Config:**
```python
CLASSIFIER_CONFIG = {
    "rare_field_threshold": 0.01,
    "type_stability_threshold": 1.0,
    "density_threshold": 0.50,
    "max_internal_nesting": 2
}
```

**Output:**
- Adds decision, confidence, reason to each field in metadata.json
- Prints verbose table in initialise mode, evolution report in fetch mode

### 3.11 src/phase_1_to_4/06_router.py

**Purpose:**  
horizontally partition cleaned records into database-specific shards based on metadata routing.

**Public Functions:**

| Function | Signature | Does |
|----------|-----------|------|
| `route_data` | → dict | Builds routes, iterates cleaned records, splits into sql/mongo/unknown shards |

**Main Algorithm:**
1. Build field_routes: for each field, determine if SQL/MONGO/UNKNOWN/BOTH
   - If primary_key_candidate or field_name == "record_id": route to BOTH
   - Else use metadata decision
2. For each record in cleaned_data.json:
   - Create sql_rec, mongo_rec, unknown_rec each with record_id
   - For each field:
     - If SQL: sql_rec[field] = value
     - If MONGO: mongo_rec[field] = value
     - If UNKNOWN: append to buffer, unknown_rec[field] = value
     - If BOTH: append to both sql_rec and mongo_rec
3. Atomic write: sql_data.json, mongo_data.json, buffer.json (updated), cleared cleaned_data.json

**Output Format:**
```json
{
  "sql": count_success,
  "mongo": count_success,
  "buffer": count_buffered
}
```

**Hardcoded:**
- Loads existing data before appending (supports incremental routing)
- Atomic writes using temp file → replace pattern

### 3.12 src/phase_5/sql_schema_definer.py

**Purpose:**  
Analyze metadata and create SQLAlchemy ORM models, then bootstrap PostgreSQL tables.

**Public Classes:**

| Class | Method | Does |
|-------|--------|------|
| SchemaAnalyzer | `load_schemas` | Loads metadata.json, builds metadata dict |
| | `_map_type_to_sql` | Maps Python types (string/int/float/bool) to SQLAlchemy Columns |
| | `get_root_fields` | Returns fields with nesting_depth=0 and decision=SQL |
| | `get_nested_objects` | Returns non-array nested fields with decision=SQL |
| | `get_arrays` | Returns array fields with decision=SQL |
| | `build_table_hierarchy` | Populates table_hierarchy dict (main_records and children) |
| SQLSchemaBuilder | `__init__` | (database_url=None) → sets up analyzer, reads DATABASE_URL env |
| | `analyze_and_build` | Calls analyzer.load_schemas, _create_models, creates engine, _create_tables |
| | `_create_models` | Creates main_records model + nested/array child models using type() |
| | `_create_main_table` | SQLAlchemy model for main_records: record_id (PK), root fields |
| | `_create_nested_table` | Model for nested object table: id (PK), main_records_id (FK), sub-fields |
| | `_create_array_table` | Model for array table: id (PK), main_records_id (FK), position, value/sub-fields |
| | `_create_tables` | Calls Base.metadata.create_all(engine), prints schema debug info |
| | `get_session` | Returns sessionmaker(bind=engine)() |
| | `get_models` | Returns self.models dict |

**PK/FK Strategy:**
- **Root Table:** record_id (int, PK) from cleaner.py — guaranteed unique sequential integer
- **Nested/Array Tables:** auto-increment surrogate id (PK), main_records_id (FK to main_records.record_id)

**Column Constraints Applied:**
- NOT NULL if frequency == 1.0
- UNIQUE if cardinality == 1.0

**Hardcoded:**
- TABLE PREFIX: `'main_records_{field_name}'.replace('.', '_')`
- Base = declarative_base() (module-level singleton, line 25)

### 3.13 src/phase_5/sql_engine.py

**Purpose:**  
Handle SQL data normalization, CRUD operations, bulk inserts via SQLAlchemy.

**Public Classes:**

| Class | Method | Does |
|-------|--------|------|
| DataNormalizer | `__init__` | Initializes empty metadata dict, buffer |
| | `load_metadata` | Reads metadata.json, builds field info dict |
| | `normalize_record` | Decomposes flat record into root_data + nested_data_by_table |
| SQLEngine | `__init__` | (database_url=None) → creates SQLSchemaBuilder, DataNormalizer, no session yet |
| | `initialize` | Calls schema_builder.analyze_and_build(), loads metadata, creates session, returns bool |
| | `_build_relationships` | Maps nested table names to parent (main_records) |
| | `insert_record` | Normalizes record, inserts root + nested rows, commits, returns record_id |
| | `bulk_insert_from_file` | Opens sql_data.json, loops insert_record(), returns (success, fail) counts |
| | `query_all` | SELECT * FROM table LIMIT limit, returns list[dict] |
| | `get_table_count` | COUNT(*) on table |
| | `get_database_stats` | Returns {table→count} dict |
| | `close` | Closes session |

**Normalization Algorithm (normalize_record):**
1. For each field in record:
   - If field_name == "record_id": add to root_data (PK)
   - If not in metadata: skip (recoverable error, logs warning)
   - If is_array:
     - If array_content_type == 'object': flatten object fields to columns with position index
     - Else: store value + value_type + position
   - If is_nested:
     - Store as single row in nested table
   - Else (scalar):
     - Add to root_data
2. Returns: (root_data, nested_data_by_table_name)

**Session Management:**
- SQLAlchemy session created once in initialize()
- auto-commit disabled; manual session.commit() called after each insert
- Rollback on exception: session.rollback()
- No external transaction session support (A3 will need to add)

**Known Issues:**
- **Import Side Effect:** Initializing SQLSchemaBuilder requires sqlalchemy import; if missing, raises immediately (line 12 try/except)
- **Session Persistence:** One session shared across all operations in a SQLEngine instance; not thread-safe in multi-threaded context
- **No Transaction Exposure:** No way to pass external session or connection for cross-database transactions

### 3.14 src/phase_5/mongo_engine.py

**Purpose:**  
Store and upsert records in MongoDB, handle nested field decomposition via reference collections.

**Public Functions:**

| Function | Signature | Does |
|----------|-----------|------|
| `loadJsonData` | (filePath) → dict or None | Reads JSON file, returns parsed; None if missing |
| `determineMongoStrategy` | (fieldMetadata) → dict | Builds field_path→"reference" or "embed" map |
| `processNode` | (dataNode, currentPath, dbInstance, strategyMap) → value | Recursively processes node: embeds scalars, references nested/arrays |
| `processMongoData` | (mongoData, strategyMap, dbInstance) → (success, fail) | Loops records, processes, upserts by _id |
| `runMongoEngine` | () → (success, fail) | Loads files, determines strategy, calls processMongoData, prints summary |

**Strategy Determination:**
```python
if isArray or isNested: strategy = "reference"
else: strategy = "embed"
```

**processNode Algorithm:**
1. If dict:
   - For each key-value:
     - Determine strategy (reference or embed)
     - Recurse on value
     - If reference: insert {data: recursed_value} into collection {path}, replace with inserted _id
     - If embed: keep value as-is
2. If list:
   - For each item: recurse with path = "{currentPath}[]"
3. If scalar: return as-is

**Upsert Pattern:**
```python
collection.update_one(
    {"_id": record["_id"]},
    {"$set": record},
    upsert=True
)
```
This means:
- First run (initialise): inserts all records
- Second run (fetch): updates existing by _id, inserts new

**Data Operations:**
- Reads: mongo_data.json, metadata.json
- Writes: MongoDB collections

**Reference Collection Names:**
- Created with underscore replacement: `path.replace(".", "_")`
- Example: "metadata.sensor_data" → "metadata_sensor_data" collection

**Known Issues:**
- **Import Side Effect:** MongoClient connection at module level (line 7: `mongo_client = MongoClient(MONGO_URI, ...)`), but this line is NOT executed because runMongoEngine() creates its own instance. Could be misleading.
- **Document Count Inflation:** Each nested field creates separate collection; document count reported per collection. Total document volume across all collections >> record_id count.
- **No Session Parameter:** MongoClient created internally; cannot pass external session for multi-operation transactions (A3 will need to add)

### 3.15 src/phase_5/sql_pipeline.py

**Purpose:**  
Orchestrate SQL schema creation and bulk data loading; archive on success; compensate on failure.

**Public Functions:**

| Function | Signature | Does |
|----------|-----------|------|
| `archive_processed_data` | (source, archive, success, fail) → None | If fail > 0: skip; else: append to archive, clear source |
| `run_sql_pipeline` | (engine: SQLEngine) → (success, fail) | Initialize schema, bulk load, archive, print summary |
| `main` | → None | CLI dispatcher for init/status/run commands |

**Main Orchestration (run_sql_pipeline):**
1. Initialize engine (schema creation, table creation)
2. Check SQL_DATA_FILE exists
3. Bulk insert from sql_data.json
4. If success > 0:
   - Call archive_processed_data → appends to data_till_now_sql.json, clears sql_data.json
5. Print database statistics

**Archive Logic:**
```python
if fail_count > 0:
    print("Skipping archive — records failed. Fix and re-run.")
    return
```
This ensures failed batches are retryable (source file not cleared).

**Data Reads:**
- sql_data.json
- metadata.json

**Data Writes:**
- SQL tables (via sql_engine)
- data_till_now_sql.json (archive)

**Hardcoded:**
- Archive path: `os.path.join(DATA_DIR, "data_till_now_sql.json")`

### 3.16 src/phase_6/CRUD_json_reader.py

**Purpose:**  
Interactive CLI to collect CRUD request JSON from user, validate, and persist to query.json.

**Public Functions:**

| Function | Signature | Does |
|----------|-----------|------|
| `validate_structure` | (data, path="root") → raises ValueError | Validates CRUD request has required fields and structure |
| `store_query_to_json` | (request) → bool | Writes query to QUERY_FILE |
| `get_pasted_json` | () → dict or None | Prompts user to paste, validates, stores if valid |
| `main` | () → None | Interactive CLI: offers validate-existing or paste-new |

**Required CRUD Structure:**
```json
{
  "operation": "CREATE|READ|UPDATE|DELETE",
  "entity": "main_records",
  "filters": {},
  "payload": {}
}
```

**Operation-Specific Validation:**
- CREATE: requires non-empty payload
- READ: filters optional (empty dict means read all)
- UPDATE: requires non-empty filters AND non-empty payload
- DELETE: filters optional

**Blocking Input:**
- `sys.stdin.read()` (paste mode) blocks indefinitely until Ctrl+D/Ctrl+Z

**Data Written:**
- query.json (overwritten each time)

### 3.17 src/phase_6/CRUD_operations.py

**Purpose:**  
Execute CREATE/READ/UPDATE/DELETE operations across hybrid SQL+MongoDB databases using saga-style transaction coordination.

**Public Functions:**

| Function | Signature | Returns | Does |
|----------|-----------|---------|------|
| `read_operation` | (parsed_query, db_analysis) → dict | {operation, entity, data} | 2-phase read: find record_ids, fetch full records, merge by record_id |
| `create_operation` | (parsed_query, db_analysis) → dict | {operation, entity, status, transaction} | 3-phase: assign record_id, route fields, execute saga |
| `update_operation` | (parsed_query, db_analysis) → dict | {operation, entity, updated_count, transaction} | 2-phase: find record_ids, update in each DB |
| `delete_operation` | (parsed_query, db_analysis) → dict | {operation, entity, deleted_count, transaction} | 2-phase: find record_ids, delete from each DB |
| `merge_results_by_record_id` | (results_by_db) → dict | {record_id→merged} | Merges SQL/MONGO/Unknown results by record_id, adds _source field |

**Global State at Module Level (Side Effects):**
```python
sql_engine = SQLEngine()
sql_available = False
try:
    sql_engine.initialize()
    sql_available = True
except: ...

mongo_client = MongoClient(MONGO_URI, ...)
mongo_available = False
try: ... except: ...

tx_coordinator = TransactionCoordinator(TRANSACTION_LOG_FILE)
```

**Read Operation (2-Phase):**

**Phase 1:** Find matching record_ids
- SQL: query Model.record_id WHERE {filters}
- MONGO: collection.find({filters}, {_id: 1})
- Unknown: iterate json, filter matches

**Phase 2:** Fetch full records by record_id
- SQL: query Model WHERE record_id IN (...)
- MONGO: collection.find({_id: {$in: [...]}})
- Unknown: iterate json, match record_ids

**Phase 3:** Merge
- Calls merge_results_by_record_id(): combines by record_id, adds _source field

**Create Operation (3-Phase):**

**Phase 1:** Assign record_id
- Reads counter.txt, increments, writes back

**Phase 2:** Route fields
- SQL payload: {record_id, SQL fields}
- MONGO payload: {record_id, MONGO fields}
- Unknown payload: {record_id, unknown fields}

**Phase 3:** Execute Saga
- Creates TransactionStep list
- Calls tx_coordinator.run() with apply/compensate/verify functions
- If all succeed: returns success
- If ANY fail: compensation rolls back all in reverse order

**Saga Steps for CREATE:**
1. create_sql_record: insert into SQL, compensation: delete by record_id, verify: result is not None
2. create_mongo_document: insert into MongoDB (set _id = record_id), compensation: delete by _id, verify: acknowledged
3. create_unknown_record: append to unknown_data.json, compensation: restore from before-state, verify: true

**Update/Delete:** Similar 2-phase approach; no saga coordination (UPDATE/DELETE do not have transactional guarantees in current implementation)

**[AMBIGUOUS] No Saga for UPDATE/DELETE:**  
DELETE_operation() and UPDATE_operation() do NOT use TransactionCoordinator. If SQL succeeds and MongoDB fails, records are inconsistent. A3 should add saga coordination.

**Blocking/Error Handling:**
- SQL connection errors: caught, prints "[WARNING]", returns empty results
- MongoDB connection errors: caught, prints "[WARNING]", returns empty results

**Data Writes:**
- counter.txt (incremented on CREATE)
- transaction_log.json (via TransactionCoordinator)
- Unknown records: unknown_data.json (atomic write via _atomic_write_json)

### 3.18 src/phase_6/CRUD_runner.py

**Purpose:**  
Parse query.json, analyze which databases to query, dispatch to appropriate operation handler.

**Public Functions:**

| Function | Signature | Returns | Does |
|----------|-----------|---------|------|
| `query_parser` | () → dict | {operation, entity, filters, payload} | Reads query.json, extracts fields |
| `get_field_locations` | () → dict | {field_name→decision} | Reads metadata.json, maps field→SQL/MONGO/Unknown |
| `analyze_query_databases` | (parsed_query) → dict | {field_locations, databases_needed} | Maps query fields to DB locations |
| `query_runner` | () → None | (prints result, writes query_output.json) | Parses, analyzes, dispatches to operation handler |

**analyze_query_databases Logic:**
- For CREATE: analyzes payload fields
- For READ/DELETE: analyzes filter fields
- For UPDATE: analyzes BOTH filter AND payload fields
- If no fields specified: queries all databases (SQL, MONGO, Unknown)

**Output Dispatch:**
| Operation | Handler |
|-----------|---------|
| CREATE | create_operation |
| READ | read_operation |
| UPDATE | update_operation |
| DELETE | delete_operation |

**Result Persistence:**
- Writes query_output.json (atomic write)
- Prints to stdout (JSON formatted)

### 3.19 src/phase_6/transaction_coordinator.py

**Purpose:**  
Implement saga-style distributed transaction with compensation and event logging.

**Public Classes:**

| Class | Method | Does |
|-------|--------|------|
| TransactionStep | `__init__` | (name, participant, apply_fn, compensate_fn, verify_fn) → stores all |
| TransactionCoordinator | `__init__` | (log_file) → creates file if missing |
| | `run` | (operation, entity, participants, steps, metadata) → runs saga |
| | `_append_log` | (transaction) → appends to log file atomically |

**Saga Algorithm (run method):**
1. Create transaction record: {transaction_id, operation, entity, participants, state: in_progress, started_at, events: []}
2. For each step:
   - Record: step_apply_started
   - Call step.apply_fn()
   - If verify_fn fails: raise RuntimeError
   - Record: step_apply_succeeded
3. If all succeed:
   - Set state: committed, record: transaction_committed
   - Append to log
   - Return {success: True, ...}
4. If ANY fails:
   - Record: transaction_apply_failed
   - For each applied_step in reverse:
     - Record: step_compensate_started
     - Call step.compensate_fn()
     - Record: step_compensate_succeeded
   - If ANY compensation fails: state = failed_needs_recovery, record error
   - Else: state = rolled_back
   - Append to log
   - Return {success: False, state, error, compensation_errors}

**Event Format:**
```json
{
  "time": "2026-04-04T12:00:00Z",
  "type": "transaction_started|step_apply_started|step_apply_succeeded|...",
  "payload": {}
}
```

**Atomic Write Pattern:**
- Write to `.tmp` file
- Call `os.replace(tmp, final)` (atomic file rename)

**Thread Safety:**
- Uses threading.Lock() for log append (line 15: `_log_lock = threading.Lock()`)

**Data Written:**
- transaction_log.json (cumulative, appended)

### 3.20 external/app.py

**Purpose:**  
Mock external API generating synthetic data records with realistic fields and nested structure.

**Public Endpoints:**

| Route | Method | Returns | Does |
|-------|--------|---------|------|
| `/` | GET | `{dict}` | Single generated record |
| `/record/{count}` | GET | Server-Sent Events | Streams `count` records, 1 per event, 10ms delay |

**Data Generation:**
- USER_POOL: 1000 unique usernames (40-byte seed: 42)
- FIELD_POOL: 50 realistic fields (faker + random)
- FIELD_WEIGHTS: each field has independent appearance probability (0.05–0.95, random)
- Nested: 60% chance to include "metadata" object with 0–3 fields selected

**Record Structure (Typical):**
```json
{
  "username": "selected_from_USER_POOL",
  "name": "faker.name()",
  "age": 18-70,
  ...,
  "metadata": {
    "sensor_data": {"version": "2.1", "calibrated": bool, "readings": [1-10 for 3]},
    "tags": ["word" for 1-3],
    "is_bot": bool,
    "internal_id": "ID-####-??"
  }
}
```

**Server-Sent Events Format:**
```
event: record
data: {"json": "record"}

event: record
data: {"json": "record"}
```

**Hardcoded:**
- Host: 0.0.0.0 (allows Docker container access)
- Port: 8000
- Sleep: 10ms per record (0.01 seconds)
- Random seed: 42 (deterministic for testing)

---

## 4. Data Flow

### 4.1 Initialise Flow (Complete Execution Path)

```
User Input: python main.py initialise 1000
                ↓
        main() in main.py
                ↓
        start_api() → subprocess external/app.py on port 8000
                ↓
        wait_for_api() → polls http://127.0.0.1:8000 until responds
                ↓
        initialise(1000)
                ├─→ initialise() cleanup: delete all data files
                │
                ├─→ clean_databases():
                │   ├─→ MongoClient.drop_database(MONGO_DB_NAME)
                │   └─→ PostgreSQL: DROP SCHEMA public CASCADE; CREATE SCHEMA public
                │
                ├─→ schema_definition.main():
                │   └─→ (interactive) collect & validate initial_schema.json
                │       └─→ save to initial_schema.json
                │
                ├─→ ingestion.fetch_data(1000):
                │   └─→ GET http://127.0.0.1:8000/record/1000 (SSE stream)
                │       └─→ for each record: ADD sys_ingested_time field
                │           └─→ accumulate in raw_records list
                │               └─→ increment counter.txt every 100 records
                │   ├─→ save to received_data.json
                │   └─→ set checkpoint: "ingest"
                │
                ├─→ process_in_memory(raw_records, is_fetch=False):
                │   ├─→ cleaner.clean_recursive() for each record
                │   │   ├─→ assign record_id = offset + index
                │   │   ├─→ map fields against schema (exact/fuzzy/snake_case)
                │   │   ├─→ cast types (string→int, "true"→True, etc.)
                │   │   ├─→ quarantine unmapped fields → buffer.json
                │   │   └─→ collect cleaned_records list
                │   ├─→ analyzer.analyze_records(cleaned_records)
                │   │   ├─→ walk structure depth-first
                │   │   ├─→ track frequency, dominant_type, cardinality, nesting_depth
                │   │   ├─→ detect regex patterns in strings
                │   │   └─→ identify primary_key_candidates (freq=1.0, card=1.0)
                │   ├─→ analyzer.save_analysis() → analyzed_schema.json
                │   ├─→ flush received_data.json to []
                │   ├─→ update counter.txt
                │   ├─→ save to cleaned_data.json
                │   └─→ append to buffer.json
                │   └─→ set checkpoint: "profile"
                │
                ├─→ metadata_builder.merge_metadata(is_update=False):
                │   ├─→ flatten initial_schema.json to dot-notation
                │   ├─→ merge with analyzed_schema.json fields
                │   ├─→ flag is_discovered_buffer=(freq<0.1 AND name not in user_schema)
                │   └─→ save to metadata.json
                │   └─→ set checkpoint: "metadata"
                │
                ├─→ classifier.run_classification(verbose=True):
                │   ├─→ Pass 1: evaluate each field statistically
                │   │   ├─→ if is_discovered_buffer → BUFFER
                │   │   ├─→ if freq < 0.01 → UNKNOWN
                │   │   ├─→ if stability ≥ 1.0 AND freq ≥ 0.5 → SQL
                │   │   └─→ else → MONGO
                │   ├─→ Pass 2: apply deep nesting rule
                │   │   ├─→ if internal_nesting > 2 → force to MONGO
                │   │   └─→ children inherit parent's exile
                │   ├─→ embed decision, confidence, reason in metadata.json
                │   ├─→ print verbose routing table
                │   └─→ set checkpoint: "classify"
                │
                ├─→ data_router.route_data():
                │   ├─→ build field_routes from metadata decisions
                │   ├─→ for each record in cleaned_data.json:
                │   │   ├─→ split by decision: SQL fields → sql_rec
                │   │   │                       MONGO fields → mongo_rec
                │   │   │                       UNKNOWN fields → buffer
                │   │   │                       BOTH fields → both tables
                │   │   └─→ append sql_rec/mongo_rec/unknown_rec to respective lists
                │   ├─→ save: sql_data.json, mongo_data.json, buffer.json
                │   ├─→ flush: cleaned_data.json to []
                │   └─→ set checkpoint: "route"
                │
                ├─→ run_storage_with_safety(batch_record_ids, context):
                │   │
                │   ├─→ SQL Pipeline:
                │   │   ├─→ SQLEngine.__init__()
                │   │   ├─→ sql_engine.initialize():
                │   │   │   ├─→ SQLSchemaBuilder().analyze_and_build():
                │   │   │   │   ├─→ load metadata.json
                │   │   │   │   ├─→ analyze: root_fields, nested_objects, arrays
                │   │   │   │   ├─→ _create_main_table: record_id(PK) + root fields
                │   │   │   │   ├─→ _create_nested_table: for each nested object
                │   │   │   │   ├─→ _create_array_table: for each array
                │   │   │   │   └─→ create_engine(DATABASE_URL) → PostgreSQL connection
                │   │   │   │   └─→ Base.metadata.create_all() → CREATE TABLE statements
                │   │   │   ├─→ sql_engine.session = sessionmaker()
                │   │   │   └─→ return True/False
                │   │   ├─→ sql_engine.bulk_insert_from_file(sql_data.json):
                │   │   │   ├─→ for each record in sql_data.json:
                │   │   │   │   ├─→ normalizer.normalize_record(record):
                │   │   │   │   │   ├─→ root_data = {record_id, scalar fields}
                │   │   │   │   │   ├─→ nested_data = {table_name: [{...}, ...]}
                │   │   │   │   │   └─→ return (root_data, nested_data)
                │   │   │   │   ├─→ session.add(MainRecords(**root_data))
                │   │   │   │   ├─→ session.flush()
                │   │   │   │   ├─→ for each nested_table, nested_records:
                │   │   │   │   │   └─→ session.add(NestedModel(**nested_record))
                │   │   │   │   └─→ session.commit()
                │   │   │   └─→ return (success_count, fail_count)
                │   │   └─→ archive: data_till_now_sql.json (if success > 0)
                │   │ 
                │   │   If fail_count > 0:
                │   │   ├─→ compensate_sql_batch(record_ids):
                │   │   │   └─→ session.query(Model).filter(record_id IN list).delete()
                │   │   └─→ raise RuntimeError("SQL pipeline reported failed inserts")
                │   │
                │   ├─→ MongoDB Pipeline:
                │   │   ├─→ MongoClient(MONGO_URI, ...)
                │   │   ├─→ db = client[MONGO_DB_NAME]
                │   │   ├─→ load metadata.json → determineMongoStrategy():
                │   │   │   ├─→ for each field:
                │   │   │   │   ├─→ if isNested OR isArray → "reference"
                │   │   │   │   └─→ else → "embed"
                │   │   ├─→ for each record in mongo_data.json:
                │   │   │   ├─→ extract record_id
                │   │   │   ├─→ processNode(record, "", db, strategyMap):
                │   │   │   │   ├─→ for each field:
                │   │   │   │   │   ├─→ if strategy=="reference":
                │   │   │   │   │   │   ├─→ collectionName = field.replace(".", "_")
                │   │   │   │   │   │   ├─→ db[collectionName].insert_one({data: ...})
                │   │   │   │   │   │   └─→ return inserted._id (reference)
                │   │   │   │   │   └─→ else: keep embedded value
                │   │   │   ├─→ db["main_records"].update_one(
                │   │   │   │       {_id: record_id},
                │   │   │   │       {$set: processedRecord},
                │   │   │   │       upsert=True
                │   │   │   │   )
                │   │   │   └─→ success_count++
                │   │   └─→ return (success_count, fail_count)
                │   │
                │   │   If fail_count > 0:
                │   │   ├─→ compensate_mongo_batch(record_ids):
                │   │   │   └─→ db["main_records"].delete_many({$or: [{_id: ...}, {record_id: ...}]})
                │   │   └─→ raise RuntimeError("Mongo pipeline reported failed upserts")
                │   │
                │   └─→ set checkpoint: "sql"/"mongo"
                │
                └─→ api_process.terminate()
```

### 4.2 Fetch Flow (Batch Incremental)

```
User Input: python main.py fetch 300
                ↓
        fetch(300):
        ├─→ batch_records = [(0, 100), (1, 100), (2, 100)]
        │
        └─→ for each batch:
            ├─→ n_old = read counter.txt
            │
            ├─→ ingestion.fetch_data(100):
            │   └─→ GET /record/100 → raw_records
            │       √ (same as initialise)
            │
            ├─→ process_in_memory(raw_records, is_fetch=True):
            │   ├─→ assign record_id = (n_old - len(raw_records)) + index
            │   ├─→ save to cleaned_data.json with append=True
            │   ├─→ append to buffer.json with append=True
            │   ├─→ analyzer.analyze_records(cleaned_records)
            │       → creates analyzed_schema.json with new batch stats
            │
            ├─→ metadata_builder.merge_metadata(is_update=True, n_old, n_new):
            │   ├─→ load existing metadata.json
            │   ├─→ for each new_field in analyzed_schema:
            │   │   ├─→ if exists in metadata:
            │   │   │   ├─→ frequency = old_freq * (n_old / total) + new_freq * (n_new / total)
            │   │   │   ├─→ stability = old_stab * (n_old / total) + new_stab * (n_new / total)
            │   │   │   └─→ GRADUATE if freq >= 0.1: is_discovered_buffer = False
            │   │   └─→ else (brand new): add with is_discovered_buffer flag
            │   └─→ save updated metadata.json
            │
            ├─→ classifier.run_classification(verbose=False):
            │   └─→ re-route all fields based on updated metadata
            │       → evolution report printed
            │
            ├─→ data_router.route_data():
            │   ├─→ load existing shard files
            │   ├─→ append new batch shards
            │   └─→ flush cleaned_data.json
            │
            ├─→ run_storage_with_safety(...):
            │   ├─→ SQL: bulk_insert batch (same as initialise)
            │   └─→ MongoDB: upsert batch (same as initialise)
```

### 4.3 Query Flow (READ Example)

```
User Input: python main.py query
                ↓
        query():
        ├─→ CRUD_json_reader.main():
        │   ├─→ if query.json exists:
        │   │   ├─→ offer: validate (1) or paste (2)
        │   │   └─→ validate/save
        │   └─→ else:
        │       └─→ paste mode → query.json
        │           Example: {"operation": "READ", "entity": "main_records", "filters": {"age": 30}}
        │
        ├─→ CRUD_runner.query_runner():
        │
        ├─→ parsed_query = query_parser():
        │   └─→ read query.json, extract operation/entity/filters/payload
        │
        ├─→ db_analysis = analyze_query_databases(parsed_query):
        │   ├─→ for filter field "age":
        │   │   ├─→ load metadata.json
        │   │   ├─→ look up "age" decision → suppose "SQL"
        │   │   └─→ field_locations["age"] = "SQL"
        │   └─→ return {field_locations, databases_needed: ["SQL"]}
        │
        ├─→ result = read_operation(parsed_query, db_analysis):
        │   │
        │   ├─→ PHASE 1: Find matching record_ids
        │   │   ├─→ SQL module loaded; try:
        │   │   │   ├─→ Model = sql_engine.models["main_records"]
        │   │   │   ├─→ query = session.query(Model.record_id)
        │   │   │   ├─→ query = query.filter(Model.age == 30)
        │   │   │   └─→ record_ids = [1, 5, 10, ...]
        │   │   │   [MONGO skipped: not in databases_needed]
        │   │   │   [Unknown skipped: not in databases_needed]
        │   │
        │   ├─→ PHASE 2: Fetch full records by record_id
        │   │   ├─→ records = session.query(Model).filter(Model.record_id.in_([1,5,10])).all()
        │   │   ├─→ convert to list[dict]: {col.name: getattr(record, col.name) for each col}
        │   │
        │   ├─→ PHASE 3: Merge results
        │   │   └─→ merge_results_by_record_id({"SQL": [{record_id:1, ...}, ...]})
        │   │       └─→ {1: {record_id:1, ..., _source: ["SQL"]}, ...}
        │   │
        │   └─→ return {operation: "READ", entity: "main_records", data: {...}}
        │
        ├─→ save result to query_output.json
        └─→ print JSON to console
```

### 4.4 Record_id Contract

**How record_id is Assigned:**
- cleaner.py: `cleaned_node["record_id"] = offset + i` where offset is read from counter.txt

**How record_id Flows:**
1. Assigned at cleanup phase (globally sequential, starts at 0)
2. Stored in JSON files: cleaned_data.json → sql_data.json, mongo_data.json
3. **SQL:** record_id is the PRIMARY KEY of main_records table
4. **MongoDB:** record_id is stored in both:
   - MongoDB root document: as field "record_id" (NOT _id initially)
   - MongoDB root document: also as _id (set in create_operation)
   - Origin: processMongoData() does `processedRecord["_id"] = extractedRecordId`

**Cross-Database Join Semantics:**
- READ operation uses record_id as universal merge key
- merge_results_by_record_id() combines results: `merged[record_id] = {...SQL fields..., ...Mongo fields...}`
- If record exists in SQL but not MongoDB: only SQL fields appear
- If record exists in MongoDB but not SQL: only Mongo fields appear (rare, should not occur on successful CREATE)

---

## 5. Database Layer

### 5.1 SQL Engine (PostgreSQL)

**Initialization:**
```python
engine = SQLEngine(database_url=None)  # defaults to DATABASE_URL env var
success = engine.initialize()  # Calls:
  # 1. SQLSchemaBuilder().analyze_and_build()
  # 2. Loads metadata.json
  # 3. Creates SQLAlchemy models (main_records + nested/array tables)
  # 4. create_engine(database_url) → PostgreSQL connection
  # 5. Base.metadata.create_all() → CREATE TABLE statements
  # 6. engine.session = sessionmaker() → ORM session
```

**Session Management:**
- One session created per SQLEngine instance
- No external session passing (A3 must add)
- Manual commit: `session.commit()` after each operation
- Rollback on exception: `session.rollback()`
- NOT thread-safe; intended for single-threaded pipeline

**Table Structure:**

**Main Table (main_records):**
```sql
TABLE main_records (
  record_id INTEGER PRIMARY KEY NOT NULL,
  username VARCHAR(255),
  name VARCHAR(255),
  ...
  -- All root-level fields with SQL classification
  -- NOT NULL if frequency == 1.0
  -- UNIQUE if cardinality == 1.0
)
```

**Nested Object Table** (example: main_records_metadata_sensor_data):
```sql
TABLE main_records_metadata_sensor_data (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  main_records_id INTEGER NOT NULL,
  FOREIGN KEY (main_records_id) REFERENCES main_records(record_id),
  -- Sub-fields flattened as columns
  version VARCHAR(255),
  calibrated BOOLEAN,
  readings VARCHAR(255),  -- JSON array, NOT decomposed
)
```

**Array Table** (example: main_records_tags):
```sql
TABLE main_records_tags (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  main_records_id INTEGER NOT NULL,
  FOREIGN KEY (main_records_id) REFERENCES main_records(record_id),
  position INTEGER,
  value VARCHAR(255),         -- if array of scalars
  value_type VARCHAR(50),     -- data type name
  -- OR if array of objects:
  -- sub_field1 TYPE, sub_field2 TYPE, ...
)
```

**Data Normalization (Insert Flow):**
```python
# User calls:
engine.insert_record({"record_id": 42, "name": "Alice", "tags": ["a", "b"]})

# Internally:
1. normalizer.normalize_record({...}) →
   root_data = {"record_id": 42, "name": "Alice"}
   nested_data = {"main_records_tags": [
       {"position": 0, "value": "a", "value_type": "str"},
       {"position": 1, "value": "b", "value_type": "str"}
   ]}

2. session.add(MainRecords(**root_data))
3. session.flush()  # write main_records row, get auto ID
4. for each nested_record in nested_data:
     nested_record["main_records_id"] = record_id
     session.add(NestedModel(**nested_record))
5. session.commit()
```

**Query Examples:**
```python
# Read all records
engine.query_all("main_records", limit=100)
# → [{record_id: 1, username: "bob", ...}, ...]

# Count
engine.get_table_count("main_records")
# → 42

# Full stats
engine.get_database_stats()
# → {"main_records": 42, "main_records_metadata_sensor_data": 42, "main_records_tags": 84}
```

**Auto-Commit Behavior:**
- Disabled; manual commit required
- No explicit transaction control; SQLAlchemy defaults to AUTOCOMMIT OFF

### 5.2 MongoDB Engine

**Initialization:**
```python
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client[MONGO_DB_NAME]
# No ORM; direct document manipulation
```

**Strategy Map:**
```python
strategyMap = {
    "field_name": "reference" if isNested or isArray else "embed"
}
```

**processNode Behavior:**
- **Embed:** Value stays in parent document (suitable for small objects)
- **Reference:** Value inserted into separate collection, parent stores _id reference

**Example Document Structure:**

**Direct Embedding (Small Object):**
```json
{
  "_id": 42,
  "record_id": 42,
  "username": "alice",
  "metadata": {
    "is_bot": false
  }
}
```

**Reference Strategy (Large/Nested Object):**
```json
// Document in "main_records" collection:
{
  "_id": 42,
  "record_id": 42,
  "username": "alice",
  "sensor_data": ObjectId("507f1f77bcf86cd799439011")  // reference to separate doc
}

// Document in "main_records_sensor_data" collection:
{
  "_id": ObjectId("507f1f77bcf86cd799439011"),
  "data": {
    "version": "2.1",
    "readings": [1, 2, 3]
  }
}
```

**Upsert Semantics:**
```python
collection.update_one(
    {"_id": record_id},
    {"$set": record},
    upsert=True
)
```
- First run: inserts (no existing doc with _id)
- Second run: updates fields in place (merge semantics)
- Idempotent: safe to re-run

**Collection Naming:**
- Reference collections: `field_path.replace(".", "_")`
- Example: "metadata.sensor_data" → "metadata_sensor_data" collection

**Document Count Inflation:**
- main_records collection: N documents (one per record_id)
- Reference collections: variable, depends on nesting depth and cardinality
- Total documents >> record count when using references

**Known Limitation:**
- No explicit transaction support; multi-document transactions require MongoDB 4.0+ and replica set
- Current code does not use sessions; each operation is independent

### 5.3 Transaction Support (Current State)

**SQL:**
- No external session/transaction passing
- SQLAlchemy autocommit disabled; manual commit called
- Single operation transactions only (insert/update/delete)
- **A3 Must Add:** Support for externally managed sessions to enable cross-DB ACID

**MongoDB:**
- No session parameter accepted
- Each operation independent write (no multi-document transactions)
- Upserts are atomic per document, but not across collections or cross-database
- **A3 Must Add:** Session parameter, replica set transaction support

**TransactionCoordinator:**
- Implements saga pattern with compensation
- **Current Usage:** CREATE operation only; compensation rolls back if ANY database fails
- **Missing:** UPDATE and DELETE operations do not use saga (no rollback guarantee)
- **A3 Must Add:** Extend saga to all operations, add distributed transaction manager

---

## 6. Known Issues & Limitations

### 6.1 Blocking Input in Web Context

**CRITICAL:** Web workers will hang indefinitely:

| Module | Function | Blocking Call | Impact |
|--------|----------|---------------|--------|
| 00_schema_definition.py | get_pasted_json() | `sys.stdin.read()` | Paste mode blocks indefinitely on Ctrl+D/Ctrl+Z |
| 01_ingestion.py | fetch_data() | `httpx.AsyncClient(timeout=30)` | Ingestion can take 30+ seconds (not blocking, but slow) |
| CRUD_json_reader.py | get_pasted_json() | `sys.stdin.read()` | Query definition blocks on user input |

**Workaround for A3:**
- Use FastAPI endpoints to collect schema/query via HTTP POST instead of CLI stdin
- Pre-populate initial_schema.json, query.json via API, don't prompt user

### 6.2 Module-Level Side Effects

These run at import time, will cause errors if database unavailable:

| Module | Import Statement | Side Effect | Impact |
|--------|------------------|-------------|--------|
| sql_engine.py | `from .sql_schema_definer import SQLSchemaBuilder` | line 22: imports SQLAlchemy; if missing, raises ImportError | Pipeline crashes if SQLAlchemy not installed (handled by requirements.txt) |
| CRUD_operations.py | `sql_engine = SQLEngine()` (line 49) | Creates engine at import time | If PostgreSQL unavailable, engine.initialize() fails (caught, sets sql_available=False) |
| CRUD_operations.py | `mongo_client = MongoClient(MONGO_URI, ...)` (line 58) | Connects at import time | If MongoDB unavailable, caught, sets mongo_available=False |
| CRUD_operations.py | `tx_coordinator = TransactionCoordinator(TRANSACTION_LOG_FILE)` (line 64) | Creates log file at import time | Creates file system side effect; acceptable |

**A3 Workaround:**
- Lazy-initialize engines only when needed (on first CRUD operation, not at import)
- Retry connection logic with exponential backoff

### 6.3 CLI Context Assumptions

These assume interactive CLI; will fail in web/API context:

| Code | Assumption | Failure Mode |
|------|-----------|--------------|
| main.py: `subprocess.Popen(external/app.py)` | Assumes `sys.executable` available | Web framework may not have Python in PATH |
| starter.py: Windows Docker path hardcoded | Assumes Windows Docker Desktop path | Fails on Mac/Linux; must detect OS |
| CRUD_json_reader.py: `sys.stdin.read()` | Assumes terminal input available | Hangs in web context (no input) |
| cli argument parsing: `sys.argv` | Assumes command-line args | Must switch to HTTP POST body for web API |

### 6.4 Buffer Graduation Gap

**Architectural Issue:**

When a field graduates from buffer to a main sharding decision (freq >= 0.1):
1. Historical records in buffer.json (from before graduation) are NOT re-processed
2. Graduated field rows remain isolated in buffer collection
3. Cross-database joins (merge_results_by_record_id) will miss buffered rows

**Example:**
- Batch 1: field "rare_id" seen in 2/100 records (freq=0.02) → → marked BUFFER
- Batch 2: field "rare_id" seen in 50/100 new records → new freq = (2+50)/(100+100) = 0.26 → graduates to SQL
- Result: original 2 records with "rare_id" stay in buffer; 50 new records in SQL table; inconsistent data

**A3 Should Implement:**
- Backfill: when field graduates, re-process historical buffer rows and move to appropriate table

### 6.5 Compensation Gaps (UPDATE/DELETE)

| Operation | Saga Support | Issue |
|-----------|--------------|-------|
| CREATE | ✓ Yes | SQL/Mongo/Unknown all have compensation |
| READ | N/A | Read-only, no compensation needed |
| UPDATE | ✗ No | SQL and Mongo updated independently; if Mongo fails after SQL succeeds, inconsistency |
| DELETE | ✗ No | SQL and Mongo deleted independently; if Mongo fails after SQL succeeds, orphaned docs |

**A3 Must Implement:**
- Wrap UPDATE/DELETE in saga transactions with compensation

### 6.6 Session & Connection Management

| System | Issue |
|--------|-------|
| SQL | SQLEngine holds single session; not thread-safe; no external session passing |
| MongoDB | No session parameter; each operation independent |
| Unknown Buffer | Plain JSON files; no replay mechanism if crash mid-write |

**Current State:**
- All operations single-threaded (acceptable for CLI)
- Pipeline crashes mid-operation lose transactional guarantees

**A3 Workaround:**
- Use checksums/versioning to detect incomplete writes
- Add connection pooling for concurrent web requests

### 6.7 Error Handling Gaps

| Code | Issue |
|------|-------|
| main.py subprocess | If external/app.py crashes, not detected until API call |
| sql_engine.initialize() | Failures silently caught; printed but no exception propagated |
| cluster connection | If PostgreSQL/MongoDB take >20 sec to start, starter.py times out silently |
| compensate_sql_batch() | If compensation itself fails, exception caught but logged to stderr only |

### 6.8 Hardcoded Database URLs

| Variable | Value | Issue |
|----------|-------|-------|
| DATABASE_URL | `postgresql://admin:secret@localhost:5432/cs432_db` | Hardcoded credentials in code (security risk) |
| MONGO_URI | `mongodb://admin:secret@localhost:27017/` | Hardcoded credentials (security risk) |

**A3 Must Implement:**
- Credentials from secure vault (Kubernetes secrets, AWS Secrets Manager, etc.)
- Remove hardcoded defaults

---

## 7. What A3 Will Need to Modify

Based on code analysis, A3 must implement FastAPI dashboard, transaction coordinator, and ACID validation. Here are the specific files and changes required:

### 7.1 Files to Create (New)

| File | Purpose |
|------|---------|
| `src/dashboard_api.py` | FastAPI backend serving dashboard endpoints (schema, query, CRUD operations) |
| `src/dashboard_frontend/` | (Optional) React/Vue frontend for dashboard |
| `docs/ACID_VALIDATION_GUIDE.md` | Document ACID testing methodology |
| `src/advanced_transaction_manager.py` | Distributed transaction manager for cross-database ACID guarantees |

### 7.2 Files to Modify

#### **main.py**

**Changes Needed:**
1. **Remove subprocess API spawning** (lines 373–376)
   - Replace with: Import uvicorn and run FastAPI in same process (or separate thread)
   - Rationale: Dashboard backend will handle API spawning

2. **Add FastAPI initialization** (new)
   - Initialize FastAPI app with routes:
     - POST /api/schema → collect initial_schema.json
     - POST /api/ingest → trigger ingestion
     - POST /api/query → CRUD operations
     - GET /api/status → pipeline status

3. **Remove wait_for_api()** (lines 377–382)
   - No longer needed; FastAPI runs in-process

**Specific Lines:**
```python
# BEFORE:
api_process = start_api()  # Line 373
if not wait_for_api():     # Line 374
    ...

# AFTER:
# FastAPI is initialized elsewhere; no subprocess needed
```

#### **src/config.py**

**Changes Needed:**
1. Add dashboard-specific paths:
```python
DASHBOARD_PORT = os.environ.get("DASHBOARD_PORT", 8001)
DASHBOARD_HOST = os.environ.get("DASHBOARD_HOST", "0.0.0.0")
```

2. Add logging configuration:
```python
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
LOG_FILE = os.path.join(DATA_DIR, "app.log")
```

#### **src/phase_6/CRUD_operations.py**

**Changes Needed:**

1. **Add saga coordination to UPDATE operation** (~line 350)
   - Wrap update_operation in saga transaction
   - Define UpdateStep objects with compensation
   - Currently does direct updates without rollback

2. **Add saga coordination to DELETE operation** (~line 420)
   - Wrap delete_operation in saga transaction
   - Define DeleteStep objects with compensation
   - Currently does direct deletes without rollback

3. **Expose external transaction session parameter:**
```python
def create_operation(parsed_query, db_analysis, sql_session=None, mongo_session=None):
    # Allow A3 dashboard to pass external sessions for coordinated transactions
    if sql_session:
        sql_engine.session = sql_session  # Use provided session
    if mongo_session:
        # MongoDB session use (requires replica set)
        ...
```

4. **Make engines thread-safe:**
   - Add connection pooling
   - Create session per request context (web-friendly)

**Specific Issues:**

**Line ~350 (update_operation function):**
```python
# CURRENT: Direct SQL/Mongo updates without saga
sql_engine.session.query(Model).filter(...).update({...})

# NEEDED: Wrap in saga
steps = [
    TransactionStep(
        name="update_sql_record",
        participant="SQL",
        apply_fn=lambda: sql_engine.session.query(Model).filter(...).update(...),
        compensate_fn=lambda: restore_sql_record(...),
        verify_fn=lambda result: result > 0
    ),
    TransactionStep(
        name="update_mongo_document",
        participant="MONGO",
        apply_fn=lambda: mongo_db[entity].update_many(...),
        compensate_fn=lambda: restore_mongo_document(...),
        verify_fn=lambda result: result.modified_count > 0
    )
]
tx_result = tx_coordinator.run("UPDATE", entity, participants, steps)
```

#### **src/phase_6/CRUD_json_reader.py**

**Changes Needed:**

1. **Remove stdin blocking** (line 36)
   - Current: `sys.stdin.read()` for paste mode
   - Replace with: HTTP POST endpoint to receive JSON
   - Make silent read from file (no user prompt)

2. **Make it importable library** (currently CLI-only)
```python
def validate_and_store_query(request: dict) -> bool:
    """Library function called by FastAPI endpoint"""
    validate_structure(request)
    store_query_to_json(request)
    return True

# Current main() still works for CLI
if __name__ == "__main__":
    main()
```

#### **external/app.py**

**Changes Needed:**

1. **Remove hardcoded uvicorn.run()** (line 48)
   - Current: `uvicorn.run(app, host="0.0.0.0", port=8000)`
   - Replace with: Let FastAPI dashboard backend manage this
   - Or: Keep app.py untouched, just don't spawn subprocess from main.py

2. **Add CORS headers** (new):
```python
from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

#### **docker-compose.yml**

**Changes Needed:**

1. **Add dashboard backend service** (new):
```yaml
dashboard:
  build: .
  container_name: cs432_dashboard
  ports:
    - "8001:8001"
  environment:
    POSTGRES_URI: postgresql://admin:secret@postgres:5432/cs432_db
    MONGO_URI: mongodb://admin:secret@mongo:27017/
    DASHBOARD_PORT: 8001
  volumes:
    - ./data:/app/data
  command: ["python", "-m", "uvicorn", "src.dashboard_api:app", "--host", "0.0.0.0", "--port", "8001"]
```

2. **api service:** Keep as-is (mock data generator)

#### **Dockerfile**

**Changes Needed:**

No changes if using single Dockerfile for all services. Dashboard will be a separate service in docker-compose.

If making separate Dockerfile:
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN mkdir -p /app/data
CMD ["python", "-m", "uvicorn", "src.dashboard_api:app", "--host", "0.0.0.0", "--port", "8001"]
```

#### **starter.py**

**Changes Needed:**

1. **Add wait_for_port call for dashboard** (after mongo/postgres):
```python
print("[*] Waiting for Dashboard (port 8001)...")
if not wait_for_port(8001):
    print("[X] Timeout: Dashboard did not start in time.")
    sys.exit(1)
print("[+] Dashboard is ready.")
```

2. **Update instruction output** (line ~107):
```python
print("      python main.py initialise 500  # Via CLI")
print("      OR visit http://localhost:8001 # Via Dashboard")
```

### 7.3 New Files to Create

#### **src/dashboard_api.py** (New)

**Endpoints needed for A3:**

```python
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any

app = FastAPI(title="CS432 Dashboard API")

# === Schema Management ===
class SchemaInput(BaseModel):
    schema_: Dict[str, Any]

@app.post("/api/schema/validate")
def validate_schema(input: SchemaInput):
    """Validate and save initial_schema.json"""
    # Call: starter00_schema_definition.validate_structure(input.schema_)
    # Save to: initial_schema.json
    # Return: {valid: bool, errors: [...]}

@app.get("/api/schema")
def get_schema():
    """Retrieve current schema"""
    # Read initial_schema.json
    # Return: {schema: {...}}

# === Pipeline Control ===
@app.post("/api/pipeline/initialise")
def start_initialise(count: int = 1000):
    """Trigger initialise pipeline"""
    # Call: main.initialise(count)
    # Run async/in-background
    # Return: {job_id: str, status: "started"}

@app.post("/api/pipeline/fetch")
def start_fetch(count: int = 100):
    """Trigger fetch pipeline"""
    # Call: main.fetch(count)
    # Return: {job_id: str, status: "started"}

@app.get("/api/pipeline/status")
def get_pipeline_status():
    """Get current pipeline status"""
    # Read checkpoint.json, counter.txt
    # Return: {step: str, records: int, timestamp: float}

# === CRUD Operations ===
class CRUDRequest(BaseModel):
    operation: str  # CREATE, READ, UPDATE, DELETE
    entity: str
    filters: Dict[str, Any] = {}
    payload: Dict[str, Any] = {}

@app.post("/api/crud/query")
def execute_query(request: CRUDRequest):
    """Execute CRUD operation"""
    # Call: CRUD_runner.query_runner() after storing request to query.json
    # Return: result from CRUD_operations

@app.get("/api/crud/results")
def get_query_results():
    """Get last query result"""
    # Read query_output.json
    # Return: result

# === ACID Testing (A3) ===
@app.post("/api/acid/test")
def run_acid_test(test_name: str):
    """Run ACID validation test"""
    # Call: ACID.runner.run_acid_test(test_name)
    # Return: {test: str, passed: bool, ...}

@app.get("/api/acid/results")
def get_acid_results():
    """Get all ACID test results"""
    # Return: {atomicity: {...}, consistency: {...}, isolation: {...}, durability: {...}}
```

**A3 Implementation Plan:**
1. All endpoints should be async to not block UI
2. Use background tasks for long-running operations (initialise, fetch)
3. Add WebSocket for real-time pipeline progress updates
4. Serve a simple HTML dashboard (or React SPA)

#### **src/advanced_transaction_manager.py** (New)

**For extended ACID support:**

```python
class DistributedTransactionManager:
    """Coordinates transactions across SQL + MongoDB with ACID guarantees"""
    
    def __init__(self, sql_engine, mongo_db):
        self.sql_engine = sql_engine
        self.mongo_db = mongo_db
    
    def begin_transaction(self):
        """Start distributed transaction"""
        # Create SQL session
        # Create MongoDB session (requires replica set)
        # Return transaction context
    
    def commit(self):
        """Commit both SQL and MongoDB; compensation on failure"""
        # Implement saga-style orchestration
    
    def rollback(self):
        """Rollback both SQL and MongoDB"""
        # Compensation logic
```

### 7.4 Files to NOT Modify

These are stable and should remain untouched:

- `src/phase_1_to_4/` (all ingestion/cleaning/profiling logic)
- `src/phase_5/sql_schema_definer.py` (schema generation)
- `src/phase_5/mongo_engine.py` (document decomposition)
- `ACID/validators.py`, `ACID/advanced_validators.py` (tests work as-is)
- `requirements.txt` (add FastAPI, uvicorn if testing new endpoints locally)

### 7.5 Testing Strategy for A3

**ACID Validation:**
- Use existing validators.py and advanced_validators.py
- Add dashboard endpoint to trigger tests
- Capture results in database or JSON files

**Transaction Testing:**
- CREATE operation: already has saga + compensation; verify compensation executes
- UPDATE operation: add saga, test compensation
- DELETE operation: add saga, test compensation
- Multi-database atomicity: create record, inject failure mid-saga, verify rollback on both DBs

**Load Testing:**
- Run concurrent CRUD operations via dashboard
- Verify isolation: no dirty reads, phantom reads
- Verify durability: records persist after shutdown/restart

---

## Appendix A: Project Configuration Summary

### Environment Variables (Docker Compose Overrides)

```yaml
# Pipeline container sets these for Docker networking:
POSTGRES_URI: postgresql://admin:secret@postgres:5432/cs432_db  # Use container DNS
MONGO_URI: mongodb://admin:secret@mongo:27017/
MONGO_DB_NAME: cs432_db
API_HOST: http://api:8000  # Use container DNS
```

### Key Thresholds & Constants

| Constant | Value | File |
|----------|-------|------|
| INITIALISE_COUNT | 1000 | project_config.py |
| FETCH_COUNT | 100 | project_config.py |
| rare_field_threshold | 0.01 | 05_classifier.py |
| type_stability_threshold | 1.0 | 05_classifier.py |
| density_threshold | 0.50 | 05_classifier.py |
| max_internal_nesting | 2 | 05_classifier.py |
| Graduation threshold | 0.1 | 04_metadata_builder.py |
| Parse buffer limit | 10000 values | 03_analyzer.py |

---

## Appendix B: Ambiguities & Open Questions

| Issue | Current Behavior | A3 Action Required |
|-------|------------------|-------------------|
| UPDATE/DELETE have no saga | Docs inconsistent after partial failure | Implement saga coordination |
| Buffer graduation gap | Historical buffer records never re-processed | Implement backfill logic |
| No external session support | Cannot coordinate multi-ops from web dashboard | Add session parameter to CRUD ops |
| Credentials hardcoded | Security risk | Use secure vault |
| No multi-threaded safety | Crashes under concurrent requests | Add connection pooling, per-request sessions |
| CLI stdin() blocking | Web workers hang indefinitely | Replace with HTTP endpoints |
| subprocess.Popen() spawning | Tight coupling; hard to test | Refactor to library functions |

---

## Document Metadata

- **Created:** 2026-04-04
- **Last Updated:** 2026-04-04
- **Scope:** CS432 Assignment 2 Baseline (A2 → A3 transition)
- **Audience:** AI agents implementing A3 features
- **Accuracy Level:** Ground truth from code analysis; no inferences
- **Deprecated:** Any documentation in docs/ that conflicts with this file

