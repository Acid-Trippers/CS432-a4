import json
import os
import sys
from config import INITIAL_SCHEMA_FILE

PRIMITIVES = ["string", "int", "float", "bool"]

def validate_structure(data, path="root"):
    """
    Recursively validates that the JSON mirrors our allowed structure:
    - Dicts: Represent JSON objects
    - Lists: Must have exactly one item (the template for array elements)
    - Strings: Must be one of our PRIMITIVES
    """
    if isinstance(data, dict):
        if not data:
            raise ValueError(f"Empty object at '{path}'. Must define at least one field.")
        for key, value in data.items():
            validate_structure(value, f"{path}.{key}")
    
    elif isinstance(data, list):
        if len(data) != 1:
            raise ValueError(f"Array at '{path}' must contain exactly one template element (e.g., ['int'] or [{{...}}]).")
        validate_structure(data[0], f"{path}[]")
    
    elif isinstance(data, str):
        if data.lower() not in PRIMITIVES:
            raise ValueError(f"Invalid type '{data}' at '{path}'. Must be one of: {PRIMITIVES}, json(as {{}}), or array(as []).")
    
    else:
        raise ValueError(f"Unsupported data type {type(data).__name__} at '{path}'. Use only dicts, lists, or primitive strings.")

def get_pasted_json():
    print("\n--- [MODE 1] JSON Paste Mode ---")
    print("Paste your JSON schema structure below.")
    print("Press Enter, then Ctrl+D (Linux/Mac) or Ctrl+Z (Windows) and Enter.")
    print("--------------------------------------------------")
    try:
        raw_data = sys.stdin.read().strip()
        if not raw_data:
            return None
        schema = json.loads(raw_data)
        validate_structure(schema)
        return schema
    except json.JSONDecodeError as e:
        print(f"\n[X] JSON Syntax Error: {e}")
    except ValueError as e:
        print(f"\n[X] Schema Logic Error: {e}")
    return None

def check_existing_file():
    print("\n--- [MODE 2] File Check Mode ---")
    if not os.path.exists(INITIAL_SCHEMA_FILE):
        print(f"[!] {INITIAL_SCHEMA_FILE} not found. Please create it first or use Paste Mode.")
        return None
    
    try:
        with open(INITIAL_SCHEMA_FILE, 'r') as f:
            schema = json.load(f)
        validate_structure(schema)
        print(f"[+] Existing {INITIAL_SCHEMA_FILE} is valid.")
        return schema
    except Exception as e:
        print(f"[X] Error in existing file: {e}")
    return None

def main():
    print("=== Schema Definition Gatekeeper ===")
    print("1. Paste JSON into Terminal")
    print("2. Validate existing initial_schema.json file")
    
    choice = input("\nSelect an option [1/2]: ").strip()

    schema = None
    if choice == '1':
        schema = get_pasted_json()
    elif choice == '2':
        schema = check_existing_file()
        if schema:
            # Re-saving just to ensure pretty-printing/formatting
            pass 
    else:
        print("[!] Invalid selection.")
        return

    if schema:
        with open(INITIAL_SCHEMA_FILE, "w") as f:
            json.dump(schema, f, indent=4)
        print(f"\n[SUCCESS] {INITIAL_SCHEMA_FILE} is finalized and valid.")
    else:
        print("\n[!] Failed to finalize schema. Please try again.")

if __name__ == "__main__":
    main()