"""
Ingests raw data from the local API endpoint and yields it in manageable chunks.

- Requests a specified number of records from the API in a streamed connection.
- Chunks incoming data into batches to stream directly into the memory pipeline.
- Preserves a persistent counter across runs using `counter.txt` to track total fetched records.
- Appends an ingestion timestamp (`sys_ingested_time`) to every fetched record.
"""

import json
import os
import httpx
import asyncio
import sys
from src.config import RECEIVED_DATA_FILE, COUNTER_FILE, API_HOST
import datetime

INGESTION_TIMEOUT = 30 # seconds


Data_file = RECEIVED_DATA_FILE
Counter_file = COUNTER_FILE

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
    url = f"{API_HOST}/record/{num}"  # FIX: was hardcoded to http://127.0.0.1:8000
    print(f"[*] Ingestion started: Requesting {num} records from {url}...")

    new_records = []
    try:
        async with httpx.AsyncClient(timeout = INGESTION_TIMEOUT) as client:
            async with client.stream("GET", url) as response:
                async for line in response.aiter_lines():
                    if line.startswith("data: "):
                        record_json = line[6:]  # Remove "data: " prefix
                        try:
                            record = json.loads(record_json)

                            # Record ingestion time
                            record['sys_ingested_time'] = datetime.datetime.now().isoformat()
                            new_records.append(record)
                            fetched_now += 1
                            if fetched_now % 100 == 0:
                                print(f"[*] Fetched {fetched_now} records so far...")
                        except json.JSONDecodeError:
                            print(f"[!] Warning: Failed to parse record: {record_json}")

                update_total = curr_total + fetched_now
                increment_counter(update_total)
                print(f"[*] Ingestion completed: {fetched_now} records fetched. Total now {update_total}.")
                return new_records
    except httpx.ConnectError:
        print(f"[!] Error: Connection failed. Is the API running at {API_HOST}?")
        return []
    except Exception as e:
        print(f"[!] An error occurred during ingestion: {e}")
        return []

if __name__ == "__main__":
    if len(sys.argv) > 1:
        asyncio.run(fetch_data(int(sys.argv[1])))
    else:
        print("Usage: python ingestion.py <number_of_records_to_fetch>")