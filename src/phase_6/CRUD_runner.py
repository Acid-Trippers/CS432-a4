import json
from src.config import QUERY_FILE, METADATA_FILE, QUERY_OUTPUT_FILE
from .CRUD_operations import create_operation, read_operation, update_operation, delete_operation


def _json_safe(value):
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)

def query_parser():
    """Parse the query.json and interpret what it means."""
    try:
        with open(QUERY_FILE, 'r') as f:
            query = json.load(f)
        
        if not query:
            print("There is no query to execute")
            return None
        # Extract basic fields from query
        operation = query.get("operation")  # READ, UPDATE, DELETE
        entity = query.get("entity")        # employees, departments, etc.
        filters = query.get("filters")      # WHERE clause conditions
        payload = query.get("payload")      # Data to update (if UPDATE operation)
        columns = query.get("columns")      # Specific columns to fetch
        
        # Print what we extracted
        print(f"\n--- PARSED QUERY ---")
        print(f"Operation: {operation}")
        print(f"Entity: {entity}")
        print(f"Filters: {filters}")
        if payload:
            print(f"Payload: {payload}")
        if columns:
            print(f"Columns: {columns}")
        
        return {
            "operation": operation,
            "entity": entity,
            "filters": filters,
            "payload": payload,
            "columns": columns
        }
        
    except FileNotFoundError:
        print(f"Error: {QUERY_FILE} not found")
        return None
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in {QUERY_FILE}")
        return None

def get_field_locations():
    """Read metadata.json and create a map of field -> database location."""
    try:
        with open(METADATA_FILE, 'r') as f:
            metadata = json.load(f)
        
        # Build a dictionary: field_name -> decision
        field_map = {}
        for field in metadata.get("fields", []):
            field_name = field.get("field_name")
            decision = field.get("decision")  # SQL, MongoDB, Unknown, etc.
            field_map[field_name] = decision
        
        return field_map
    
    except FileNotFoundError:
        print(f"Error: {METADATA_FILE} not found")
        return {}
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in {METADATA_FILE}")
        return {}

def analyze_query_databases(parsed_query):
    """
    Analyze which databases we need based on operation type and fields.
    
    For CREATE/UPDATE: Analyzes payload fields to know where to store data
    For READ/DELETE: Analyzes filter fields to know where to query data
    For UPDATE: Analyzes both filters (to find records) and payload (to store updates)
    """
    operation = parsed_query.get("operation")
    field_map = get_field_locations()
    
    # Determine which fields to analyze based on operation
    fields_to_analyze = {}
    field_source_type = {}  # Track what type of field each is (filter/payload)
    
    if operation == "CREATE":
        # For CREATE, analyze payload fields only
        payload = parsed_query.get("payload", {})
        fields_to_analyze = payload
        field_source_type = {k: "payload" for k in payload.keys()}
        print(f"\n[ANALYZE] Operation: {operation}")
        print(f"[ANALYZE] Analyzing PAYLOAD fields ({len(payload)} fields)")
        
    elif operation == "UPDATE":
        # For UPDATE, analyze BOTH filters AND payload
        filters = parsed_query.get("filters", {})
        payload = parsed_query.get("payload", {})
        fields_to_analyze = {**filters, **payload}
        field_source_type = {k: "filter" for k in filters.keys()}
        field_source_type.update({k: "payload" for k in payload.keys()})
        print(f"\n[ANALYZE] Operation: {operation}")
        print(f"[ANALYZE] Analyzing FILTER fields ({len(filters)}) + PAYLOAD fields ({len(payload)})")
        
    else:  # READ or DELETE
        # For READ/DELETE, analyze filter fields only
        filters = parsed_query.get("filters", {})
        fields_to_analyze = filters
        field_source_type = {k: "filter" for k in filters.keys()}
        print(f"\n[ANALYZE] Operation: {operation}")
        print(f"[ANALYZE] Analyzing FILTER fields ({len(filters)} fields)")
    
    # If no fields to analyze, query all databases for safety
    if not fields_to_analyze:
        print(f"\n--- FIELD LOCATIONS ---")
        print(f"No fields specified - will query all databases for {operation} operation")
        
        databases_needed = ["SQL", "MONGO", "Unknown"]
        
        print(f"\n--- DATABASES NEEDED ---")
        print(f"Databases: {', '.join(databases_needed)}")
        
        return {
            "field_locations": {},
            "databases_needed": databases_needed
        }
    
    # Categorize each field by its database
    field_locations = {}
    for field_name in fields_to_analyze.keys():
        location = field_map.get(field_name, "Unknown")
        field_locations[field_name] = location
    
    # Determine which databases to query
    databases_needed = set()
    for location in field_locations.values():
        databases_needed.add(location)
    
    print(f"\n--- FIELD LOCATIONS ---")
    for field, location in field_locations.items():
        source = field_source_type.get(field, "unknown")
        print(f"{field} ({source}): {location}")
    
    print(f"\n--- DATABASES NEEDED ---")
    print(f"Databases: {', '.join(databases_needed)}")
    
    return {
        "field_locations": field_locations,
        "databases_needed": list(databases_needed)
    }
    
def query_runner(query_dict=None):
    """
    Execute a CRUD query.

    If query_dict is provided, run directly on that payload.
    Otherwise, preserve legacy CLI flow by reading QUERY_FILE via query_parser().
    """
    parsed_query = query_dict if query_dict is not None else query_parser()

    if not parsed_query:
        return None

    db_analysis = analyze_query_databases(parsed_query)
    print(f"\nAnalysis Result: {db_analysis}")

    operation = parsed_query.get("operation")
    result = None

    if operation == "CREATE":
        result = create_operation(parsed_query, db_analysis)
    elif operation == "READ":
        result = read_operation(parsed_query, db_analysis)
    elif operation == "UPDATE":
        result = update_operation(parsed_query, db_analysis)
    elif operation == "DELETE":
        result = delete_operation(parsed_query, db_analysis)
    else:
        result = {
            "operation": operation,
            "status": "failed",
            "error": f"Unsupported operation: {operation}"
        }

    # Save result to query_output.json
    if result:
        result = _json_safe(result)
        try:
            with open(QUERY_OUTPUT_FILE, 'w') as f:
                json.dump(result, f, indent=2, default=str)
            print(f"[SAVED] Query output saved to {QUERY_OUTPUT_FILE}")
        except Exception as e:
            print(f"[ERROR] Failed to save query output: {e}")

    # Print the result in formatted JSON
    if result:
        print(f"\n{'='*60}")
        print("[FINAL RESULT]")
        print(f"{'='*60}")
        print(json.dumps(result, indent=2, default=str))
        print(f"{'='*60}\n")

    return result

if __name__ == "__main__":
    query_runner()