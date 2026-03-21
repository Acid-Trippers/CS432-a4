import json
import sys
import os
from .config import QUERY_FILE

def validate_structure(data, path="root"):
    """Validates CRUD request structure."""
    if not isinstance(data, dict):
        raise ValueError(f"CRUD request must be a JSON object at '{path}'.")
    
    if not data:
        raise ValueError(f"CRUD request cannot be empty at '{path}'.")
    
    # Validate required fields
    if "operation" not in data:
        raise ValueError(f"Missing required field 'operation'.")
    
    if "entity" not in data:
        raise ValueError(f"Missing required field 'entity'.")
    
    # Validate operation
    valid_operations = ["CREATE", "READ", "UPDATE", "DELETE"]
    if data["operation"] not in valid_operations:
        raise ValueError(f"Invalid operation '{data['operation']}'. Must be one of: {valid_operations}")
    
    # Validate entity
    if not isinstance(data["entity"], str) or not data["entity"]:
        raise ValueError(f"Field 'entity' must be a non-empty string.")
    
    # Validate by operation type
    operation = data["operation"]
    
    if operation == "CREATE":
        if "payload" not in data:
            raise ValueError(f"CREATE operation requires 'payload' field.")
        if not isinstance(data["payload"], dict) or not data["payload"]:
            raise ValueError(f"Field 'payload' must be a non-empty object.")
    
    elif operation == "READ":
        # Filters are OPTIONAL for READ (can be empty dict or missing)
        if "filters" not in data:
            data["filters"] = {}  # Add empty filters if not provided
        elif not isinstance(data["filters"], dict):
            raise ValueError(f"Field 'filters' must be a dict.")
        # Empty filters are OK for READ
    
    elif operation == "UPDATE":
        if "filters" not in data:
            raise ValueError(f"UPDATE operation requires 'filters' field.")
        if not isinstance(data["filters"], dict) or not data["filters"]:
            raise ValueError(f"Field 'filters' must be a non-empty object.")
        
        if "payload" not in data:
            raise ValueError(f"UPDATE operation requires 'payload' field.")
        if not isinstance(data["payload"], dict) or not data["payload"]:
            raise ValueError(f"Field 'payload' must be a non-empty object.")
    
    elif operation == "DELETE":
        if "filters" not in data:
            raise ValueError(f"DELETE operation requires 'filters' field.")
        if not isinstance(data["filters"], dict) or not data["filters"]:
            raise ValueError(f"Field 'filters' must be a non-empty object.")

def store_query_to_json(request):
    """Stores the validated CRUD request to query.json in the data directory."""
    try:
        with open(QUERY_FILE, 'w') as f:
            json.dump(request, f, indent=2)
        
        print(f"Query stored successfully to {QUERY_FILE}")
        return True
    except Exception as e:
        print(f"Error storing query: {e}")
        return False

def get_pasted_json():
    print("\n--- [PASTE MODE] ---")
    print("Paste your CRUD request below. Press Enter, then Ctrl+D (Mac/Linux) or Ctrl+Z (Win) and Enter.")
    print("-" * 50)
    try:
        raw_data = sys.stdin.read().strip()
        if not raw_data: return None
        request = json.loads(raw_data)
        validate_structure(request)
        store_query_to_json(request)
        return request
    except (json.JSONDecodeError, ValueError) as e:
        print(f"\n[X] Validation Error: {e}")
        print("Please give a valid json input")
    return None

def main():
    print("=== Schema Definition Gatekeeper ===")
    
    # Logic: If file exists, offer to validate it. Otherwise, force paste.
    if os.path.exists(QUERY_FILE):
        print(f"[!] Existing '{QUERY_FILE}' found.")
            
        choice = input("Use/Validate existing file (1) or Overwrite with new Paste (2)? [1/2]: ").strip()
    else:
        print(f"[*] '{QUERY_FILE}' not found. Moving to Paste Mode...")
        choice = '2'

    schema = None

    if choice == '1':
        try:
            with open(QUERY_FILE, 'r') as f:
                schema = json.load(f)
            validate_structure(schema)
            print(f"[+] Existing file is valid.")
        except Exception as e:
            print(f"[X] Existing file is invalid: {e}")
            if input("Retry with Paste Mode? (y/n): ").lower() == 'y':
                schema = get_pasted_json()
    else:
        schema = get_pasted_json()

    if schema:
        with open(QUERY_FILE, "w") as f:
            json.dump(schema, f, indent=4)
        print(f"\n[SUCCESS] {QUERY_FILE} finalized.")
    else:
        print("\n[!] No valid schema provided.")
        sys.exit(1) # Exit with error so main.py knows to stop

if __name__ == "__main__":
    main()