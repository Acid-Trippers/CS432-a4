import json
from src.config import QUERY_FILE
from src.phase_6.CRUD_runner import query_parser, analyze_query_databases
from src.phase_6.CRUD_operations import read_operation, refresh_connections

# Refresh connections first
refresh_connections()

# Create query for single column
query = {
    "operation": "READ",
    "entity": "main_records",
    "filters": {},
    "columns": ["username", "city", "subscription"]  # Multiple columns test
}

# Write to query.json
with open(QUERY_FILE, 'w') as f:
    json.dump(query, f, indent=2)

# Parse the query
parsed = query_parser()

# Analyze which databases to use
db_analysis = analyze_query_databases(parsed)

# Execute READ with both parameters
results = read_operation(parsed, db_analysis)

# Extract and print single column (username)
print("\n=== SINGLE COLUMN: username ===\n")

if isinstance(results, dict) and "data" in results:
    data = results["data"]
    
    # data is already a dict with record_id -> record_data
    if isinstance(data, dict):
        print(f"Total records: {len(data)}\n")
        
        # Get first record
        first_record_id = sorted(list(data.keys()))[0]
        first_record = data[first_record_id]
        
        print(f"First record fields: {list(first_record.keys())}")
        print(f"Field count: {len(first_record)}")
        print(f"Requested columns: {query.get('columns')}\n")
        print(f"Full first record: {first_record}")
else:
    print(f"Unexpected results structure")