import json
from src.config import QUERY_FILE
from src.phase_6.CRUD_runner import query_parser, analyze_query_databases
from src.phase_6.CRUD_operations import read_operation, refresh_connections

# Refresh connections first
refresh_connections()

# Create query for selected columns
query = {
    "operation": "READ",
    "entity": "main_records",
    "filters": {},
    "columns": ["username", "city", "subscription"],
}

# Write to query.json
with open(QUERY_FILE, 'w') as f:
    json.dump(query, f, indent=2)

# Parse, analyze, and execute
parsed = query_parser()
db_analysis = analyze_query_databases(parsed)
results = read_operation(parsed, db_analysis)

print("\n=== COLUMN READ CHECK ===\n")

if isinstance(results, dict) and "data" in results:
    data = results["data"]

    if isinstance(data, dict) and data:
        print(f"Total records: {len(data)}\n")

        first_record_id = sorted(list(data.keys()))[0]
        first_record = data[first_record_id]

        print(f"First record fields: {list(first_record.keys())}")
        print(f"Field count: {len(first_record)}")
        print(f"Requested columns: {query.get('columns')}\n")
        print(f"Full first record: {first_record}")
    else:
        print("No records returned. Initialize/fetch data first.")
else:
    print("Unexpected results structure")
