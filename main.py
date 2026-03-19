"""Main orchestrator for the database pipeline. Handles command-line arguments, manages the API lifecycle, and coordinates data ingestion."""

import argparse
import subprocess
import asyncio
import time
import sys
import os
import shutil
from src.config import *

def run_script(script_name, args = None):
    """Helper to run teammate's scripts or ingestion.py."""
    print(f"[*] Running {script_name}...")
    # sys.executable ensures we stay inside the same venv
    if args is None:
        args = []
    script_path = os.path.join("src", f"{script_name}.py")
    cmd = [sys.executable, script_path] + list(args)
    return subprocess.run(cmd, check = True)

def start_api():
    """Starts the API server in a subprocess."""
    print("[*] Starting API server...")
    api_path = os.path.join("external", "app.py")
    return subprocess.Popen([sys.executable, api_path], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

async def main():
    parser = argparse.ArgumentParser(description="Main orchestrator for the database pipeline.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser('initialise')
    init_parser.add_argument('records', type=int, default=1000, help='Number of records to start with')

    fetch_parser = subparsers.add_parser('fetch')
    fetch_parser.add_argument('records', type=int, default=1000, help='Additional number of records to fetch from API')

    if len(sys.argv) == 1:
        print("\n[!] No command provided.")
        print("Usage: python main.py [initialise | fetch] [records]")
        print("Example: python main.py initialise 500\n")
        parser.print_help()
        return
    
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return
    
    api_process = start_api()
    # Wait a moment for the API to be ready
    time.sleep(2)

    try:
        if args.command == 'initialise':
            # Wipe local storage
            files_to_clean = [RECEIVED_DATA_FILE, COUNTER_FILE, METADATA_MANAGER_FILE, ANALYZED_SCHEMA_FILE, FIELD_METADATA_FILE, NORMALIZED_DATA_FILE, CLEANED_DATA_FILE, BUFFER_FILE]
            for f in files_to_clean:
                if os.path.exists(f):
                    os.remove(f)
            if os.path.exists("counter.txt"):
                os.remove("counter.txt")
            
            print("[!] Environment reset.")
            
            # Step A: Get User Schema (Your friend's script)
            run_script("schema_definition")
            
            # Step B: Get first batch of data (Your worker)
            run_script("ingestion", [str(args.records)])

            print(f"[*] Records ingested. Running Analyzer on {args.records} records...")
            # run_script("analyzer")   <-- This creates analyzed_schema.json
            
            print("[+] System is now fully initialized and metadata.json is finalized.")

        elif args.command == 'fetch':
            if not os.path.exists(METADATA_MANAGER_FILE):
                print("[X] ERROR: No metadata found. You must run 'initialise' first to calibrate the system.")
                return

            print(f"[*] System Ready. Fetching {args.records} additional records...")
            run_script("ingestion", [str(args.records)])
            # Future: run_script("classifier") or "routing" could happen here
    finally:
        print("[*] Shutting down API server...")
        api_process.terminate()

if __name__ == "__main__":    asyncio.run(main())