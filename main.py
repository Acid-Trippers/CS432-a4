"""Main orchestrator for the database pipeline. Handles command-line arguments, manages the API lifecycle, and coordinates data ingestion."""

import argparse
import subprocess
import asyncio
import time
import sys
import os
import shutil

def run_script(script_name, args):
    """Helper to run teammate's scripts or ingestion.py."""
    print(f"[*] Running {script_name}...")
    # sys.executable ensures we stay inside the same venv
    cmd = [sys.executable, f"{script_name}.py"] + list(args)
    return subprocess.run(cmd)

def start_api():
    """Starts the API server in a subprocess."""
    print("[*] Starting API server...")
    return subprocess.Popen([sys.executable, "app.py"], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

async def main():
    parser = argparse.ArgumentParser(description="Main orchestrator for the database pipeline.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser('initialise')
    init_parser.add_argument('records', type=int, default=1000, help='Number of records to start with')

    fetch_parser = subparsers.add_parser('fetch')
    fetch_parser.add_argument('records', type=int, default=1000, help='Additional number of records to fetch from API')

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
            files_to_clean = ["count.txt", "received_data.json"]
            for f in files_to_clean:
                if os.path.exists(f):
                    os.remove(f)
            if os.path.exists("counter.txt"):
                os.remove("counter.txt")
            
            print("[!] Environment reset.")
            
            # Step A: Get User Schema (Your friend's script)
            #run_script("schema_def")
            
            # Step B: Get first batch of data (Your worker)
            run_script("ingestion", [str(args.records)])
        elif args.command == 'fetch':
            # Just fetch more data
            run_script("ingestion", [str(args.records)])

        if os.path.exists("count.txt"):
            with open("count.txt", 'r') as f:
                total = f.read().strip()
                print(f"[*] Total records fetched across runs: {total}")
                if total >= 1000 and not os.path.exists("analyzed_schema.json"):
                    print("[*] 1000 records reached! Ready for Analysis & Validation...")
                # run_script("analyzer")
                # run_script("validation")
    finally:
        print("[*] Shutting down API server...")
        api_process.terminate()

if __name__ == "__main__":    asyncio.run(main())
