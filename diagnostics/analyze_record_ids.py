import json
from src.phase_5.sql_engine import SQLEngine
from collections import Counter

sql_engine = SQLEngine()
sql_engine.initialize()
session = sql_engine.schema_builder.get_session()

model = sql_engine.models.get("main_records")
if model:
    records = session.query(model).all()
    ids = sorted([r.record_id for r in records])
    
    print(f"Total records: {len(ids)}")
    print(f"Record IDs: {ids[:50]}")  # First 50
    print(f"...and {len(ids) - 50} more" if len(ids) > 50 else "")
    
    # Find gaps
    expected = set(range(0, 1000))
    actual = set(ids)
    
    print(f"\n=== ANALYSIS ===")
    print(f"Expected 0-999: Missing {len(expected - actual)} records")
    print(f"Extra records (not 0-999): {len(actual - expected)}")
    
    if len(actual - expected) <= 20:
        print(f"Extra IDs: {sorted(actual - expected)}")
    
    # Get samples of records with negative or huge IDs
    weird_records = [(r.record_id, r.device_id) for r in records if r.record_id < 0 or r.record_id > 1000]
    print(f"\nWeird records (< 0 or > 1000): {len(weird_records)}")
    for rid, dev in weird_records[:5]:
        print(f"  record_id={rid}, device_id={dev}")

session.close()
