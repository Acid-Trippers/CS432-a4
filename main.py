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
import subprocess
import time

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

def wait_for_port(port, host='localhost', timeout=30):
    """Checks if a port is open before proceeding."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except (OSError, ConnectionRefusedError):
            time.sleep(1)
    return False

def start_docker():
    print("[*] Launching Docker Infrastructure (Postgres/Mongo)...")
    try:
        # 1. Use DEVNULL to prevent the terminal buffer from hanging on logs
        # 2. Add 'postgres' and 'mongo' to ONLY start the DBs if they are in your Compose
        subprocess.run(
            ["docker-compose", "-f", project_config.DOCKER_COMPOSE_FILE, "up", "-d"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True
        )
        
        print("[*] Waiting for Database Engine readiness...")
        # Check for standard Postgres (5432) and Mongo (27017) ports
        if wait_for_port(5432) and wait_for_port(27017):
            print("[+] Databases are online and healthy.")
            time.sleep(2) # Final safety buffer for internal DB initialization
        else:
            print("[X] Timeout: Databases failed to start. Is Docker Desktop running?")
            sys.exit(1)
            
    except subprocess.CalledProcessError as e:
        print(f"[X] Docker Compose failed to start: {e}")
        sys.exit(1)


def stop_docker():
    print("[*] Stopping Docker containers...")
    subprocess.run(
            ["docker-compose", "-f", project_config.DOCKER_COMPOSE_FILE, "down", "--timeout", "5"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False  # Don't crash main.py if some containers are already gone
        )
    print("[+] Docker containers stopped.")


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
            # Drop all tables by querying information_schema instead of using Base.metadata
            conn.execute(text("DROP SCHEMA public CASCADE; CREATE SCHEMA public;"))
            conn.commit()
        print("[+] SQL database tables dropped.")
    except Exception as e:
        print(f"[!] Warning: PostgreSQL not reachable, skipping: {e}")

def initialise(count=1000):
    files_to_clean = [
        COUNTER_FILE, RECEIVED_DATA_FILE, CLEANED_DATA_FILE, 
        BUFFER_FILE, ANALYZED_SCHEMA_FILE, METADATA_FILE, 
        SQL_DATA_FILE, MONGO_DATA_FILE, QUERY_FILE, CHECKPOINT_FILE
    ]
    for f in files_to_clean:
        if os.path.exists(f):
            if os.path.isfile(f):
                os.remove(f)
            else:
                shutil.rmtree(f)
    with open(COUNTER_FILE, 'w') as f:
        f.write("0")
    print("\n[!] Environment reset.")

    #THIS CLEARS BOTH DATABASES
    clean_databases()

    print("[*] Running Schema Definition...")
    schema_definition.main()
    
    print("[*] Fetching Data...")
    raw_records = asyncio.run(ingestion.fetch_data(count))
    save_checkpoint(RECEIVED_DATA_FILE, raw_records, append=False)
    set_checkpoint("ingest")

    print("[*] Processed In-Memory. (Cleaning + Profiling)")
    process_in_memory(raw_records, is_fetch=False)
    set_checkpoint("profile")

    print("[*] Building Metadata...")
    metadata_builder.merge_metadata()
    set_checkpoint("metadata")

    print("[*] Classifying Schema...")
    classifier.run_classification(verbose = True)
    set_checkpoint("classify")
    
    print("[*] Routing Data...")
    data_router.route_data()
    set_checkpoint("route")
    
    print("[*] SQL Pipeline...")
    set_checkpoint("sql")
    engine_sql = sql_engine.SQLEngine()
    sql_pipeline.run_sql_pipeline(engine_sql)

    print("[*] MongoDB Pipeline...")
    set_checkpoint("mongo")
    mongo_engine.runMongoEngine()


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

        # 2. Fetch and Clean
        print(f"[*] Fetching chunk of {current_batch}...")
        raw_records = asyncio.run(ingestion.fetch_data(current_batch))
        
        # This saves to cleaned_data.json and flushes received_data.json
        cleaned_batch = process_in_memory(raw_records, is_fetch=True)
        
        # 3. Update Intelligence (Evolution Suite)
        # We pass n_old and the size of this new batch (n_new)
        metadata_builder.merge_metadata(is_update=True, n_old=n_old, n_new=len(raw_records))
        classifier.run_classification(verbose=False)

        # 4. Route and Flush
        # (Your updated router will clear cleaned_data.json)
        print("[*] Sharing Data...")
        batch_stats = data_router.route_data()
        
        if batch_stats:
            print(f"    >>> Batch Success: {batch_stats['sql']} records to SQL, {batch_stats['mongo']} to Mongo.")
        
        # 5. SQL Pipeline (Optional: uncomment if needed per batch)
        engine_sql = sql_engine.SQLEngine()
        sql_pipeline.run_sql_pipeline(engine_sql)

        print("[*] MongoDB Pipeline...")
        mongo_engine.runMongoEngine()

        remaining -= current_batch
        print(f"[+] Chunk processed. Total global records: {n_old + current_batch}")

    print(f"[SUCCESS] Fetch complete.")


# Removed redundant wait_for_server function


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

    start_docker()
    print("[*] Getting Docker environment ready...")
    time.sleep(project_config.DOCKER_STARTUP_TIMEOUT)
    api_process = start_api()

    if not wait_for_api():
        print("[X] API server failed to start.")
        api_process.terminate()
        stop_docker()
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
                # Logic to resume from specific steps could be more granular,
                # but for now we'll just re-run from metadata if profile was done, etc.
                if last_step == "ingest":
                    process_in_memory(json.load(open(RECEIVED_DATA_FILE)), is_fetch=False)
                    # continue the chain...
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
        stop_docker()


if __name__ == "__main__":
    main()