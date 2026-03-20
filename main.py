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
            files_to_clean = [
                COUNTER_FILE, RECEIVED_DATA_FILE, 
                CLEANED_DATA_FILE, BUFFER_FILE, ANALYZED_SCHEMA_FILE, 
                METADATA_FILE
            ]
            for f in files_to_clean:
                if os.path.exists(f):
                    if os.path.isfile(f):
                        os.remove(f)
                    else:
                        shutil.rmtree(f)
            
            print("[!] Environment reset.")
            
            # Step A: Get User Schema
            run_script("schema_definition")
            
            # Step B: Get first batch of data
            run_script("ingestion", [str(args.records)])

            # Step C: Clean Data (Padding & Ripping)
            run_script("cleaner")

            # Step D: Analyze Data (Statistical Profiling)
            run_script("analyzer")

            # Step E: Validate & Consolidate (Merge Schema + Stats)
            run_script("validation")
            
            print("[+] System is now fully initialized and metadata.json is finalized.")

        elif args.command == 'fetch':
            if not os.path.exists(METADATA_FILE):
                print("[X] ERROR: No metadata found. You must run 'initialise' first to calibrate the system.")
                return

            print(f"[*] System Ready. Fetching {args.records} additional records...")
            run_script("ingestion", [str(args.records)])
            # Step C: Clean
            run_script("cleaner")
            # Step D: Analyze
            run_script("analyzer")
            # Step E: Validate
            run_script("validation")
    finally:
        print("[*] Shutting down API server...")
        api_process.terminate()

if __name__ == "__main__":    asyncio.run(main())