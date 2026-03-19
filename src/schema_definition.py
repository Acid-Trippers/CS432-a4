import json
import os
import sys
from config import INITIAL_SCHEMA_FILE

PRIMITIVES = ["string", "int", "float", "bool"]

def validate_structure(data, path="root"):
    """Recursively validates the Mirror Structure."""
    if isinstance(data, dict):
        if not data:
            raise ValueError(f"Empty object at '{path}'.")
        for key, value in data.items():
            validate_structure(value, f"{path}.{key}")
    elif isinstance(data, list):
        if len(data) != 1:
            raise ValueError(f"Array at '{path}' must have exactly one template element.")
        validate_structure(data[0], f"{path}[]")
    elif isinstance(data, str):
        if data.lower() not in PRIMITIVES:
            raise ValueError(f"Invalid type '{data}' at '{path}'. Use: {PRIMITIVES}")
    else:
        raise ValueError(f"Unsupported type {type(data).__name__} at '{path}'.")

def get_pasted_json():
    print("\n--- [PASTE MODE] ---")
    print("Paste your JSON structure below. Press Enter, then Ctrl+D (Mac/Linux) or Ctrl+Z (Win) and Enter.")
    print("-" * 50)
    try:
        raw_data = sys.stdin.read().strip()
        if not raw_data: return None
        schema = json.loads(raw_data)
        validate_structure(schema)
        return schema
    except (json.JSONDecodeError, ValueError) as e:
        print(f"\n[X] Validation Error: {e}")
    return None

def main():
    print("=== Schema Definition Gatekeeper ===")
    
    # Logic: If file exists, offer to validate it. Otherwise, force paste.
    if os.path.exists(INITIAL_SCHEMA_FILE):
        print(f"[!] Existing '{INITIAL_SCHEMA_FILE}' found.")
        choice = input("Use/Validate existing file (1) or Overwrite with new Paste (2)? [1/2]: ").strip()
    else:
        print(f"[*] '{INITIAL_SCHEMA_FILE}' not found. Moving to Paste Mode...")
        choice = '2'

    schema = None

    if choice == '1':
        try:
            with open(INITIAL_SCHEMA_FILE, 'r') as f:
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
        with open(INITIAL_SCHEMA_FILE, "w") as f:
            json.dump(schema, f, indent=4)
        print(f"\n[SUCCESS] {INITIAL_SCHEMA_FILE} finalized.")
    else:
        print("\n[!] No valid schema provided.")
        sys.exit(1) # Exit with error so main.py knows to stop

if __name__ == "__main__":
    main()