"""
Complete database reset and cleanup.
Removes all corrupted records and resets counter to 0.
"""

import json
from pathlib import Path

from pymongo import MongoClient

from src.config import COUNTER_FILE, METADATA_FILE, MONGO_DB_NAME, MONGO_URI
from src.phase_5.sql_engine import SQLEngine

print("=" * 70)
print("COMPLETE DATABASE RESET")
print("=" * 70)

# 1. Delete all records from SQL
print("\n[1] Clearing SQL database...")
try:
    sql_engine = SQLEngine()
    sql_engine.initialize()
    session = sql_engine.schema_builder.get_session()

    model = sql_engine.models.get("main_records")
    if model:
        count = session.query(model).delete()
        session.commit()
        print(f"    ✓ Deleted {count} records from SQL")

    session.close()
except Exception as e:
    print(f"    ✗ Error: {e}")

# 2. Delete all records from MongoDB
print("\n[2] Clearing MongoDB database...")
try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo_db = mongo_client[MONGO_DB_NAME]

    # Clear all collections
    for collection_name in mongo_db.list_collection_names():
        result = mongo_db[collection_name].delete_many({})
        print(f"    ✓ Cleared collection '{collection_name}': {result.deleted_count} docs")

    mongo_client.close()
except Exception as e:
    print(f"    ✗ Error: {e}")

# 3. Reset counter to 0
print("\n[3] Resetting counter file...")
try:
    with open(COUNTER_FILE, 'w') as f:
        f.write('0')
    print("    ✓ Counter reset to 0")
except Exception as e:
    print(f"    ✗ Error: {e}")

# 4. Reset metadata total_records to 0
print("\n[4] Resetting metadata...")
try:
    if Path(METADATA_FILE).exists():
        with open(METADATA_FILE, 'r') as f:
            metadata = json.load(f)

        metadata['total_records'] = 0

        with open(METADATA_FILE, 'w') as f:
            json.dump(metadata, f, indent=2)

        print("    ✓ Metadata total_records reset to 0")
except Exception as e:
    print(f"    ✗ Error: {e}")

print("\n" + "=" * 70)
print("✓ COMPLETE RESET SUCCESSFUL")
print("=" * 70)
print("\nDatabase is now clean. You can initialize new data:")
print("  python legacy/main.py initialise <count>")
print("\n")
