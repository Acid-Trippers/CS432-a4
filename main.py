import argparse
import asyncio
import importlib
import json
import os
import shutil
import subprocess
import sys
import time

from src.config import *
import src.router as data_router

schema_definition = importlib.import_module("src.00_schema_definition")
ingestion = importlib.import_module("src.01_ingestion")
cleaner_mod = importlib.import_module("src.02_cleaner")
analyzer_mod = importlib.import_module("src.03_analyzer")
metadata_builder = importlib.import_module("src.metadata_builder")
classifier = importlib.import_module("src.classifier")
sql_schema_definer = importlib.import_module("src.sql_schema_definer")
sql_engine = importlib.import_module("src.sql_engine")
sql_pipeline = importlib.import_module("src.sql_pipeline")


def start_api():
    """Starts the API server in a subprocess."""
    print("[*] Starting API server...")
    api_path = os.path.join("external", "app.py")
    return subprocess.Popen([sys.executable, api_path], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)


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


def process_in_memory(raw_records, is_fetch=False):
    """Handles the sequential processing of data records in memory."""
    print("[*] Cleaning Data...")
    cleaner = cleaner_mod.DataCleaner()
    cleaned_records = []
    
    for i, record in enumerate(raw_records):
        record_id = record.get("id", record.get("_id", f"idx_{time.time()}_{i}"))
        cleaned_records.append(cleaner.clean_recursive(record, cleaner.schema, record_id))
    
    save_checkpoint(CLEANED_DATA_FILE, cleaned_records, append=is_fetch)
    save_checkpoint(BUFFER_FILE, cleaner.buffer, append=is_fetch)

    print("[*] Profiling Data...")
    analyzer = analyzer_mod.DataAnalyzer()
    analyzer.analyze_records(cleaned_records)
    analyzer.save_analysis(ANALYZED_SCHEMA_FILE)


def initialise(count=1000):
    files_to_clean = [
        COUNTER_FILE, RECEIVED_DATA_FILE, CLEANED_DATA_FILE, 
        BUFFER_FILE, ANALYZED_SCHEMA_FILE, METADATA_FILE, 
        SQL_DATA_FILE, DATABASE_PATH, QUERY_FILE
    ]
    for f in files_to_clean:
        if os.path.exists(f):
            os.remove(f) if os.path.isfile(f) else shutil.rmtree(f)
    print("\n[!] Environment reset.")

    print("[*] Running Schema Definition...")
    schema_definition.main()
    
    print("[*] Fetching Data...")
    raw_records = asyncio.run(ingestion.fetch_data(count))
    save_checkpoint(RECEIVED_DATA_FILE, raw_records, append=False)

    process_in_memory(raw_records, is_fetch=False)

    print("[*] Building Metadata...")
    metadata_builder.merge_metadata()

    print("[*] Classifying Schema...")
    classifier.run_classification()
    
    print("[*] Routing Data...")
    data_router.route_data()
    
    print("[*] SQL Pipeline...")
    engine = sql_engine.SQLEngine()
    sql_pipeline.run_sql_pipeline(engine)


def fetch(count=100):
    if not os.path.exists(METADATA_FILE):
        print("[X] ERROR: No metadata found. Run 'initialise' first.")
        return

    print(f"\n[*] Fetching {count} additional records...")
    raw_records = asyncio.run(ingestion.fetch_data(count))
    save_checkpoint(RECEIVED_DATA_FILE, raw_records, append=True)

    process_in_memory(raw_records, is_fetch=True)

    print("[*] Routing Data...")
    data_router.route_data()

    print("[*] SQL Pipeline Insert...")
    engine = sql_engine.SQLEngine()
    sql_pipeline.run_sql_pipeline(engine)


def wait_for_server(url="http://127.0.0.1:8000/"):
    """Polls the API until it's ready, ensuring the machine doesn't try to fetch early."""
    import urllib.request
    from urllib.error import URLError
    print("[*] Waiting for API server to be ready...")
    for _ in range(30):
        try:
            urllib.request.urlopen(url)
            return True
        except URLError:
            time.sleep(0.5)
    print("[!] Warning: API server might not be ready.")
    return False


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python main.py initialise [count]")
        print("  python main.py fetch [count]")
        sys.exit(1)

    command = sys.argv[1]
    count = int(sys.argv[2]) if len(sys.argv) > 2 else (1000 if command == 'initialise' else 100)

    api_process = start_api()
    wait_for_server()

    try:
        if command == "initialise":
            initialise(count)
        elif command == "fetch":
            fetch(count)
        else:
            print(f"Unknown command: {command}")
            sys.exit(1)
    finally:
        print("\n[*] Shutting down API server...")
        api_process.terminate()

if __name__ == "__main__":
    main()