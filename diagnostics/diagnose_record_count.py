import json
from pathlib import Path
from src.config import COUNTER_FILE, METADATA_FILE
from src.phase_5.sql_engine import SQLEngine

# Check counter file
print("=" * 60)
print("COUNTER FILE:")
if Path(COUNTER_FILE).exists():
    with open(COUNTER_FILE, 'r') as f:
        counter = f.read().strip()
        print(f"  Counter value: {counter}")
else:
    print("  Counter file not found")

# Check metadata total_records
print("\nMETADATA FILE:")
if Path(METADATA_FILE).exists():
    with open(METADATA_FILE, 'r') as f:
        metadata = json.load(f)
        total = metadata.get("total_records", "NOT SET")
        print(f"  Metadata total_records: {total}")
else:
    print("  Metadata file not found")

# Check actual SQL table count
print("\nSQL TABLE COUNT:")
try:
    sql_engine = SQLEngine()
    sql_engine.initialize()
    session = sql_engine.schema_builder.get_session()
    
    model = sql_engine.models.get("main_records")
    if model:
        count = session.query(model).count()
        print(f"  Actual SQL records: {count}")
        
        # Get record_id range
        records = session.query(model).all()
        if records:
            ids = [r.record_id for r in records]
            print(f"  Record ID range: {min(ids)} to {max(ids)}")
            print(f"  Expected sequential: {list(range(min(ids), max(ids)+1)) == sorted(ids)}")
    
    session.close()
except Exception as e:
    print(f"  Error: {e}")

print("\n" + "=" * 60)
