"""
- Task: Ingest data from the API endpoint and store it in a file.
- The script will request a specified number of records from the API and append them to a local file called "received_data.json".
- It also maintains a counter of total records fetched across runs in "counter.txt" to track progress.
- Usage: Run this script with the number of records you want to fetch as a command-line argument, e.g., `python ingestion.py 1000`
"""

import json
import os
import httpx
import asyncio
import sys

Data_file = "received_data.json"
Counter_file = "counter.txt"

def get_counter():
    if not os.path.exists(Counter_file):
        with open(Counter_file, 'w') as f:
            f.write('0')
    with open(Counter_file, 'r') as f:
        return int(f.read().strip())
    
def increment_counter(new_counter):
    with open(Counter_file, 'w') as f:
        f.write(str(new_counter))

async def fetch_data(num):
    curr_total = get_counter()
    fetched_now = 0
    url = f"http://127.0.0.1:8000/record/{num}"
    print(f"[*] Ingestion started: Requesting {num} records from API...")

    try:
        async with httpx.AsyncClient(timeout = None) as client:
            async with client.stream("GET", url) as response:
                async for line in response.aiter_lines():
                    if line.startswith("data: "):
                        record_json = line[6:]  # Remove "data: " prefix
                        with open(Data_file, 'a') as f:
                            f.write(record_json + '\n')
                        fetched_now += 1
                        if fetched_now % 100 == 0:
                            print(f"[*] Fetched {fetched_now} records so far...")
                update_total = curr_total + fetched_now
                increment_counter(update_total)
                print(f"[*] Ingestion completed: Total records fetched now {update_total}.")
    except httpx.ConnectError:
        print("[!] Error: Connection failed. Is app.py running?")
    except Exception as e:
        print(f"[!] An error occurred during ingestion: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        asyncio.run(fetch_data(int(sys.argv[1])))
    else:
        print("Usage: python ingestion.py <number_of_records_to_fetch>")
