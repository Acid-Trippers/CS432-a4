"""
Verify that ACID tests don't leave behind extra records
Run this after executing tests to check for data leaks
"""

import sys
from src.phase_5.sql_engine import SQLEngine
from pymongo import MongoClient
from src.config import MONGO_URI, MONGO_DB_NAME, COUNTER_FILE
from pathlib import Path

print("=" * 70)
print("ACID TEST ISOLATION VERIFICATION")
print("=" * 70)

# Get current state
sql_engine = SQLEngine()
sql_engine.initialize()
sql_error = getattr(sql_engine, "initialization_error", None)
if sql_error:
    print("\nERROR: SQL Engine failed to initialize")
    print(f"  Details: {sql_error}")
    print("  Isolation verification is not reliable in this state.")
    sys.exit(1)

session = sql_engine.schema_builder.get_session()
try:
    model = sql_engine.models.get("main_records")
    if not model:
        print("\nERROR: SQL model 'main_records' is not available")
        print("  Isolation verification is not reliable in this state.")
        sys.exit(1)
    sql_count = session.query(model).count()
finally:
    session.close()

mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
mongo_db = mongo_client[MONGO_DB_NAME]
mongo_count = mongo_db["main_records"].count_documents({})
mongo_client.close()

counter = 0
if Path(COUNTER_FILE).exists():
    with open(COUNTER_FILE, 'r') as f:
        counter = int(f.read().strip() or 0)

print(f"\nCurrent System State:")
print(f"  SQL records:     {sql_count}")
print(f"  MongoDB records: {mongo_count}")
print(f"  Counter:         {counter}")

if sql_count == 0 and mongo_count == 0 and counter == 0:
    print("\nERROR: Environment appears uninitialized (all counts are zero)")
    print("  Run the initial data pipeline before using this verifier.")
    sys.exit(2)

expected_records = counter

print("\nExpected Baseline From Counter:")
print(f"  SQL records:     {expected_records}")
print(f"  MongoDB records: {expected_records} (approx)")
print(f"  Counter:         {expected_records}")

# Check for leaks
sql_leak = sql_count - counter
mongo_leak = mongo_count - counter

print(f"\n{'='*70}")
if sql_leak > 10 or mongo_leak > 10:
    print("WARNING: DATA LEAK DETECTED")
    print(f"  SQL leak:   {sql_leak} extra records")
    print(f"  Mongo leak: {mongo_leak} extra records")
    print(f"\nTests may not be cleaning up properly.")
    print(f"Run: python cleanup_empty_records.py --auto")
elif sql_leak == 0 and mongo_leak == 0:
    print("PASS: No data leaks detected")
    print(f"  ACID tests are properly isolated")
else:
    print(f"WARNING: MINOR LEAK: {sql_leak + mongo_leak} total extra records")
    print(f"  Within acceptable range (< 10 total)")

print("=" * 70)
