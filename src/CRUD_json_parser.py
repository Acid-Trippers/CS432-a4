import json
from config import QUERY_FILE

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
        
        # Print what we extracted
        print(f"\n--- PARSED QUERY ---")
        print(f"Operation: {operation}")
        print(f"Entity: {entity}")
        print(f"Filters: {filters}")
        if payload:
            print(f"Payload: {payload}")
        
        return {
            "operation": operation,
            "entity": entity,
            "filters": filters,
            "payload": payload
        }
        
    except FileNotFoundError:
        print(f"Error: {QUERY_FILE} not found")
        return None
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in {QUERY_FILE}")
        return None

if __name__ == "__main__":
    parsed_query = query_parser()
    print(f"\nParsed Query Object: {parsed_query}")