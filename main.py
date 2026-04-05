import argparse
import asyncio
import importlib
import json
import logging
import os
import shutil
import subprocess
import sys
import time
import httpx
import socket

import project_config
from src.config import *

# Suppress noisy logs from SQLAlchemy and pymongo when DBs are not reachable
logging.getLogger('sqlalchemy').setLevel(logging.CRITICAL)
logging.getLogger('pymongo').setLevel(logging.CRITICAL)
logging.getLogger('src.phase_5.sql_engine').setLevel(logging.CRITICAL)

CHECKPOINT_FILE = os.path.join(DATA_DIR, "pipeline_checkpoint.json")

schema_definition = importlib.import_module("src.phase_1_to_4.00_schema_definition")
ingestion = importlib.import_module("src.phase_1_to_4.01_ingestion")
cleaner_mod = importlib.import_module("src.phase_1_to_4.02_cleaner")
analyzer_mod = importlib.import_module("src.phase_1_to_4.03_analyzer")
metadata_builder = importlib.import_module("src.phase_1_to_4.04_metadata_builder")
classifier = importlib.import_module("src.phase_1_to_4.05_classifier")
data_router = importlib.import_module("src.phase_1_to_4.06_router")
sql_schema_definer = importlib.import_module("src.phase_5.sql_schema_definer")
sql_engine = importlib.import_module("src.phase_5.sql_engine")
sql_pipeline = importlib.import_module("src.phase_5.sql_pipeline")
crud_json_reader = importlib.import_module("src.phase_6.CRUD_json_reader")
crud_runner = importlib.import_module("src.phase_6.CRUD_runner")
mongo_engine = importlib.import_module("src.phase_5.mongo_engine")


def append_pipeline_failure(stage, context, error_message):
    """Persist pipeline failures for retry/recovery analysis."""
    failure_entry = {
        "time": time.time(),
        "stage": stage,
        "context": context,
        "error": str(error_message),
    }

    failures = []
    if os.path.exists(PIPELINE_FAILURE_LOG_FILE):
        try:
            with open(PIPELINE_FAILURE_LOG_FILE, 'r') as f:
                content = f.read().strip()
                failures = json.loads(content) if content else []
        except Exception:
            failures = []

    if not isinstance(failures, list):
        failures = []

    failures.append(failure_entry)
    with open(PIPELINE_FAILURE_LOG_FILE, 'w') as f:
        json.dump(failures, f, indent=2)


def compensate_sql_batch(record_ids):
    """Remove SQL rows for a failed batch by record_id."""
    if not record_ids:
        return {"attempted": False, "deleted": 0, "error": None}

    engine = sql_engine.SQLEngine()
    if not engine.initialize():
        return {"attempted": True, "deleted": 0, "error": "Failed to initialize SQL engine for compensation"}

    deleted = 0
    try:
        Model = engine.models.get("main_records")
        if not Model:
            return {"attempted": True, "deleted": 0, "error": "main_records model not found"}

        deleted = (
            engine.session.query(Model)
            .filter(Model.record_id.in_(record_ids))
            .delete(synchronize_session=False)
        )
        engine.session.commit()
        return {"attempted": True, "deleted": deleted, "error": None}
    except Exception as e:
        try:
            engine.session.rollback()
        except Exception:
            pass
        return {"attempted": True, "deleted": deleted, "error": str(e)}
    finally:
        engine.close()


def compensate_mongo_batch(record_ids):
    """Remove Mongo main collection documents for a failed batch by _id/record_id."""
    if not record_ids:
        return {"attempted": False, "deleted": 0, "error": None}

    try:
        from pymongo import MongoClient

        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[MONGO_DB_NAME]
        result = db["main_records"].delete_many({
            "$or": [
                {"_id": {"$in": record_ids}},
                {"record_id": {"$in": record_ids}},
            ]
        })
        deleted = result.deleted_count
        client.close()
        return {"attempted": True, "deleted": deleted, "error": None}
    except Exception as e:
        return {"attempted": True, "deleted": 0, "error": str(e)}


def run_storage_with_safety(batch_record_ids, context):
    """
    Run SQL then Mongo storage for one logical batch and compensate on failure.
    Returns ((sql_success, sql_fail), (mongo_success, mongo_fail)).
    """
    print("[*] Starting SQL Pipeline...", flush=True)
    start_time = time.time()
    
    engine_sql = sql_engine.SQLEngine()
    sql_success, sql_fail = sql_pipeline.run_sql_pipeline(engine_sql)
    
    sql_elapsed = time.time() - start_time
    print(f"[+] SQL Pipeline completed in {sql_elapsed:.2f}s (success={sql_success}, fail={sql_fail})", flush=True)

    if sql_fail > 0:
        sql_comp = compensate_sql_batch(batch_record_ids)
        append_pipeline_failure(
            "storage_sql",
            {
                **context,
                "sql_success": sql_success,
                "sql_fail": sql_fail,
                "sql_compensation": sql_comp,
            },
            "SQL pipeline reported failed inserts",
        )
        raise RuntimeError(
            f"SQL stage failed (success={sql_success}, fail={sql_fail}); "
            f"compensation_deleted={sql_comp.get('deleted')}"
        )

    print("[*] Starting MongoDB Pipeline...", flush=True)
    start_time = time.time()
    mongo_success, mongo_fail = mongo_engine.runMongoEngine()
    mongo_elapsed = time.time() - start_time
    print(f"[+] MongoDB Pipeline completed in {mongo_elapsed:.2f}s (success={mongo_success}, fail={mongo_fail})", flush=True)

    if mongo_fail > 0:
        mongo_comp = compensate_mongo_batch(batch_record_ids)
        sql_comp = compensate_sql_batch(batch_record_ids)
        append_pipeline_failure(
            "storage_mongo",
            {
                **context,
                "mongo_success": mongo_success,
                "mongo_fail": mongo_fail,
                "mongo_compensation": mongo_comp,
                "sql_compensation": sql_comp,
            },
            "Mongo pipeline reported failed upserts; compensated SQL and Mongo batch writes",
        )
        raise RuntimeError(
            f"Mongo stage failed (success={mongo_success}, fail={mongo_fail}); "
            f"sql_comp_deleted={sql_comp.get('deleted')}, mongo_comp_deleted={mongo_comp.get('deleted')}"
        )

    return (sql_success, sql_fail), (mongo_success, mongo_fail)


def start_api():
    """Starts the API server in a subprocess."""
    print("[*] Starting API server...")
    api_path = os.path.join("external", "app.py")
    return subprocess.Popen([sys.executable, api_path], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)


def wait_for_api(timeout=project_config.API_STARTUP_TIMEOUT):
    """Polls the API until it's ready."""
    url = f"http://{project_config.API_HOST}:{project_config.API_PORT}/"
    print(f"[*] Waiting for API server at {url}...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            httpx.get(url, timeout=1)
            return True
        except Exception:
            time.sleep(1)
    return False


def save_checkpoint(filepath, data, append=False):
    """Saves data to a JSON checkpoint, optionally appending to existing lists."""
    if append and os.path.exists(filepath):
        try:
            with open(filepath, 'r') as f:
                content = f.read().strip()
                existing = json.loads(content) if content else []
        except (json.JSONDecodeError, IOError):
            existing = []
        if isinstance(existing, list) and isinstance(data, list):
            data = existing + data
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)


def set_checkpoint(step):
    """Marks a step as completed."""
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump({"last_step": step, "timestamp": time.time()}, f)


def get_last_checkpoint():
    """Returns the last completed step."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f).get("last_step")
        except (json.JSONDecodeError, IOError):
            pass
    return None


def process_in_memory(raw_records, is_fetch=False):
    """Handles the sequential processing of data records in memory."""
    print("[*] Cleaning Data...")
    
    # helper to get offset
    offset = 0
    if os.path.exists(COUNTER_FILE):
        try:
            with open(COUNTER_FILE, 'r') as f:
                offset = int(f.read().strip() or 0) - len(raw_records)
        except: pass

    cleaner = cleaner_mod.DataCleaner()
    cleaned_records = []
    
    for i, record in enumerate(raw_records):
        ref_id = record.get("id", record.get("_id", f"idx_{time.time()}_{i}"))
        cleaned_node = cleaner.clean_recursive(record, cleaner.schema, ref_id)
        cleaned_node["record_id"] = offset + i 
        cleaned_records.append(cleaned_node)
    
    # Save the cleaned data
    save_checkpoint(CLEANED_DATA_FILE, cleaned_records, append=is_fetch)
    save_checkpoint(BUFFER_FILE, cleaner.buffer, append=is_fetch)

    # TASK: Flush raw ingestion file after successful cleaning
    if raw_records:
        with open(RECEIVED_DATA_FILE, 'w') as f:
            json.dump([], f)
        print("[*] Received_data flushed.")

    print("[*] Profiling Data...")
    analyzer = analyzer_mod.DataAnalyzer()
    analyzer.analyze_records(cleaned_records)
    analyzer.save_analysis(ANALYZED_SCHEMA_FILE)
    
    new_total = offset + len(raw_records)
    with open(COUNTER_FILE, 'w') as f:
        f.write(str(new_total))
        
    return cleaned_records


def clean_databases():
    """Drops all SQL tables and the MongoDB database for a fresh start."""
    from src.config import MONGO_URI, MONGO_DB_NAME, DATABASE_URL
    print("[*] Clearing databases for a clean slate...")
    
    # 1. Clear MongoDB
    try:
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        client.server_info()  # raises exception immediately if unreachable
        client.drop_database(MONGO_DB_NAME)
        print(f"[+] MongoDB '{MONGO_DB_NAME}' database dropped.")
    except Exception as e:
        print(f"[!] Warning: MongoDB not reachable, skipping: {e}")

    # 2. Clear SQL
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(DATABASE_URL, connect_args={"connect_timeout": 2})
        with engine.connect() as conn:
            # Force terminate other backends to ensure we can get an exclusive lock for the drop
            conn.execute(text("""
                SELECT pg_terminate_backend(pid) 
                FROM pg_stat_activity 
                WHERE datname = current_database() AND pid <> pg_backend_pid();
            """))
            conn.commit()
            
            conn.execute(text("DROP SCHEMA public CASCADE; CREATE SCHEMA public;"))
            conn.commit()
        print("[+] SQL database tables dropped.")
    except Exception as e:
        print(f"[!] Warning: PostgreSQL not reachable, skipping: {e}")


def initialise(count=1000):
    start_time = time.time()
    print("\n" + "="*80, flush=True)
    print("PIPELINE INITIALISE", flush=True)
    print("="*80, flush=True)
    
    files_to_clean = [
        COUNTER_FILE, RECEIVED_DATA_FILE, CLEANED_DATA_FILE, 
        BUFFER_FILE, ANALYZED_SCHEMA_FILE, METADATA_FILE, 
        SQL_DATA_FILE, MONGO_DATA_FILE, QUERY_FILE, CHECKPOINT_FILE,
        PIPELINE_FAILURE_LOG_FILE,
    ]
    for f in files_to_clean:
        if os.path.exists(f):
            if os.path.isfile(f):
                os.remove(f)
            else:
                shutil.rmtree(f)
    with open(COUNTER_FILE, 'w') as f:
        f.write("0")
    print("\n[!] Environment reset.", flush=True)

    # THIS CLEARS BOTH DATABASES
    print("[STAGE] Clearing Databases...", flush=True)
    stage_start = time.time()
    clean_databases()
    print(f"[+] Databases cleared in {time.time() - stage_start:.2f}s", flush=True)

    print("\n[STAGE] Schema validation...", flush=True)
    stage_start = time.time()
    if not os.path.exists(INITIAL_SCHEMA_FILE):
        raise RuntimeError(
            f"{INITIAL_SCHEMA_FILE} not found. Save a schema first via /api/pipeline/schema."
        )

    with open(INITIAL_SCHEMA_FILE, "r", encoding="utf-8") as schema_file:
        initial_schema = json.load(schema_file)

    schema_definition.validate_structure(initial_schema)
    print(f"[+] {INITIAL_SCHEMA_FILE} validated in {time.time() - stage_start:.2f}s", flush=True)
    
    print("\n[STAGE] Fetching data ({} records)...".format(count), flush=True)
    stage_start = time.time()
    raw_records = asyncio.run(ingestion.fetch_data(count))
    save_checkpoint(RECEIVED_DATA_FILE, raw_records, append=False)
    set_checkpoint("ingest")
    print(f"[+] Data fetched in {time.time() - stage_start:.2f}s", flush=True)

    print("\n[STAGE] Cleaning and profiling data...", flush=True)
    stage_start = time.time()
    cleaned_records = process_in_memory(raw_records, is_fetch=False)
    batch_record_ids = [r.get("record_id") for r in cleaned_records if r.get("record_id") is not None]
    set_checkpoint("profile")
    print(f"[+] Data cleaned and profiled in {time.time() - stage_start:.2f}s", flush=True)

    print("\n[STAGE] Building metadata...", flush=True)
    stage_start = time.time()
    metadata_builder.merge_metadata()
    set_checkpoint("metadata")
    print(f"[+] Metadata built in {time.time() - stage_start:.2f}s", flush=True)

    print("\n[STAGE] Classifying schema...", flush=True)
    stage_start = time.time()
    classifier.run_classification(verbose=True)
    set_checkpoint("classify")
    print(f"[+] Schema classified in {time.time() - stage_start:.2f}s", flush=True)
    
    print("\n[STAGE] Routing data to storage engines...", flush=True)
    stage_start = time.time()
    data_router.route_data()
    set_checkpoint("route")
    print(f"[+] Data routed in {time.time() - stage_start:.2f}s", flush=True)
    
    print("\n[STAGE] Running storage pipeline (SQL + MongoDB)...", flush=True)
    stage_start = time.time()
    run_storage_with_safety(
        batch_record_ids,
        {"pipeline": "initialise", "requested_count": count},
    )
    set_checkpoint("sql")
    set_checkpoint("mongo")
    print(f"[+] Storage pipeline completed in {time.time() - stage_start:.2f}s", flush=True)
    
    total_elapsed = time.time() - start_time
    print("\n" + "="*80, flush=True)
    print(f"[SUCCESS] Pipeline initialise completed in {total_elapsed:.2f}s", flush=True)
    print("="*80, flush=True)


def fetch(count=100):
    if not os.path.exists(METADATA_FILE):
        print("[X] ERROR: No metadata found. Run 'initialise' first.")
        return

    batch_size = 100
    remaining = count
    print(f"[*] Starting Batch-Fetch for {count} records...")

    while remaining > 0:
        current_batch = min(remaining, batch_size)
        
        # 1. Get current count BEFORE processing
        n_old = 0
        if os.path.exists(COUNTER_FILE):
            with open(COUNTER_FILE, 'r') as f:
                n_old = int(f.read().strip() or 0)

        try:
            # 2. Fetch and Clean
            print(f"[*] Fetching chunk of {current_batch}...")
            raw_records = asyncio.run(ingestion.fetch_data(current_batch))
            
            # This saves to cleaned_data.json and flushes received_data.json
            cleaned_batch = process_in_memory(raw_records, is_fetch=True)
            
            # 3. Update Intelligence (Evolution Suite)
            metadata_builder.merge_metadata(is_update=True, n_old=n_old, n_new=len(raw_records))
            classifier.run_classification(verbose=False)

            # 4. Route and Flush
            print("[*] Sharing Data...")
            batch_stats = data_router.route_data()
            
            if batch_stats:
                print(f"    >>> Batch Success: {batch_stats['sql']} records to SQL, {batch_stats['mongo']} to Mongo.")
            
            # 5. SQL Pipeline
            batch_record_ids = [r.get("record_id") for r in cleaned_batch if r.get("record_id") is not None]

            # 5 + 6. Coordinated SQL+Mongo storage with compensation
            run_storage_with_safety(
                batch_record_ids,
                {
                    "pipeline": "fetch",
                    "batch_size": current_batch,
                    "offset_before_batch": n_old,
                    "remaining_before_batch": remaining,
                },
            )
            set_checkpoint("sql")
            set_checkpoint("mongo")

            remaining -= current_batch
            print(f"[+] Chunk processed. Total global records: {n_old + current_batch}")
        except Exception as e:
            append_pipeline_failure(
                "fetch_batch",
                {
                    "batch_size": current_batch,
                    "offset_before_batch": n_old,
                    "remaining_before_batch": remaining,
                },
                str(e),
            )
            print(f"[X] Fetch batch failed: {e}")
            print(f"[X] Failure logged to {PIPELINE_FAILURE_LOG_FILE}. Resolve issue and retry fetch.")
            raise

    print(f"[SUCCESS] Fetch complete.")


def query():
    if not os.path.exists(METADATA_FILE):
        print("[X] ERROR: No metadata found. Run 'initialise' first.")
        return

    print("\n[*] Starting Query Operations...")
    
    # 1. Provide UI to get a valid query and save it to query.json
    crud_json_reader.main()
    
    # 2. Parse query.json and execute the query operation across databases
    crud_runner.query_runner()


def main():
    command = sys.argv[1] if len(sys.argv) > 1 else project_config.DEFAULT_COMMAND
    count = int(sys.argv[2]) if len(sys.argv) > 2 else (
        project_config.INITIALISE_COUNT if command == "initialise" 
        else project_config.FETCH_COUNT
    )

    # Docker is managed externally via starter.py
    # Run: python starter.py start   before running main.py
    # Run: python starter.py end     when done
    api_process = start_api()

    if not wait_for_api():
        print("[X] API server failed to start.")
        api_process.terminate()
        sys.exit(1)

    try:
        if command == "initialise":
            initialise(count)
        elif command == "fetch":
            fetch(count)
        elif command == "query":
            query()
        elif command == "resume":
            last_step = get_last_checkpoint()
            if not last_step:
                print("[!] No checkpoint found. Starting 'initialise' instead.")
                initialise(count)
            else:
                print(f"[*] Resuming from last step: {last_step}")
                if last_step == "ingest":
                    process_in_memory(json.load(open(RECEIVED_DATA_FILE)), is_fetch=False)
                    metadata_builder.merge_metadata()
                    classifier.run_classification()
                    data_router.route_data()
                elif last_step == "profile":
                    metadata_builder.merge_metadata()
                    classifier.run_classification()
                    data_router.route_data()
                elif last_step == "metadata":
                    classifier.run_classification()
                    data_router.route_data()
                elif last_step == "classify":
                    data_router.route_data()
                else:
                    print("[+] Pipeline already complete or at final stage.")
        else:
            print(f"Unknown command: {command}")
            sys.exit(1)
    finally:
        api_process.terminate()


if __name__ == "__main__":
    main()