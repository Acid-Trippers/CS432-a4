import json
import os
import sys
from config import INITIAL_SCHEMA_FILE

# Base types
PRIMITIVES = ["string", "int", "float", "bool"]

def get_field_details(field_name, depth=0):
    """Recursively defines the structure to mirror the real JSON layout."""
    indent = "  " * depth
    print(f"\n{indent}--- Configuring: {field_name} ---")
    print(f"{indent}Options: {', '.join(PRIMITIVES)}, json, array")
    
    while True:
        dtype = input(f"{indent}Select type: ").strip().lower()
        
        # CASE 1: JSON (Object) -> Stores as a dictionary
        if dtype == "json":
            nested_obj = {}
            print(f"{indent}Defining internal fields for '{field_name}' (type 'done' to exit level):")
            while True:
                child_name = input(f"{indent}  Child field name: ").strip()
                if child_name.lower() == 'done': break
                nested_obj[child_name] = get_field_details(child_name, depth + 1)
            return nested_obj

        # CASE 2: Array -> Stores as a single-item list [type_or_structure]
        elif dtype == "array":
            print(f"{indent}What is inside this array?")
            inner_content = get_field_details(f"{field_name}[]", depth + 1)
            return [inner_content]

        # CASE 3: Primitives -> Stores as the type name string
        elif dtype in PRIMITIVES:
            # We skip unique/not_null here to keep the file as a "pure" structure template.
            # If you need those, we'd store them as "string|unique|not_null" 
            return dtype
        
        else:
            print(f"{indent}[!] Invalid selection. Choose from {PRIMITIVES}, json, or array.")

def get_guided_input():
    schema = {}
    print("\n--- Recursive Schema Builder (Structure Mode) ---")
    print("Define your fields. This will generate a representative template.")

    while True:
        field_name = input("\nTop-level Field Name (or 'done'): ").strip()
        if field_name.lower() == 'done':
            if not schema: continue
            break
        
        schema[field_name] = get_field_details(field_name)

    return schema

def get_pasted_json():
    print("\n--- JSON Paste Mode ---")
    print("Paste your JSON schema. Press Enter, then Ctrl+D (Linux/Mac) or Ctrl+Z (Windows) and Enter.")
    print("--------------------------------------------------")
    try:
        raw_data = sys.stdin.read() 
        return json.loads(raw_data)
    except json.JSONDecodeError as e:
        print(f"\n[!] Invalid JSON: {e}")
        return None

def main():
    print("=== Database Pipeline Setup ===")
    print("1. Recursive Guided Entry (Handles Nesting)")
    print("2. Paste Raw JSON")
    
    choice = input("\nSelect an option [1/2]: ").strip()

    if choice == '2':
        schema = get_pasted_json()
        if not schema:
            print("[!] Falling back to Guided Entry...")
            schema = get_guided_input()
    else:
        schema = get_guided_input()

    with open(INITIAL_SCHEMA_FILE, "w") as f:
        json.dump(schema, f, indent=4)
    
    print(f"\n[+] {INITIAL_SCHEMA_FILE} has been saved.")

if __name__ == "__main__":
    main()
