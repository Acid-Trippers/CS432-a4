"""
Consolidates user-defined schema specifications with statistically analyzed fields 
to build a unified metadata playbook for schema classification.

- Schema Flattening: Transforms nested initial schemas into flat, dot-notation path mappings (e.g., parent.child).
- Constraint Extraction: Derives intended data types (user_type) from the user's base template.
- Metadata Merging: Injects user-defined types directly into the statistically generated fields mapped by the analyzer.
- Buffer Identification: Flags purely discovered/unmapped variables by assigning them null constraints, instructing the classifier to rely purely on heuristics.
- Playbook Export: Outputs the fully merged data (metadata.json) to drive downstream SQL vs. Mongo routing decisions.
"""

import json
import os
from src.config import INITIAL_SCHEMA_FILE, ANALYZED_SCHEMA_FILE, METADATA_FILE

def merge_metadata():
    if not os.path.exists(INITIAL_SCHEMA_FILE):
        print(f"[!] {INITIAL_SCHEMA_FILE} not found.")
        return
    if not os.path.exists(ANALYZED_SCHEMA_FILE):
        print(f"[!] {ANALYZED_SCHEMA_FILE} not found.")
        return

    with open(INITIAL_SCHEMA_FILE, 'r') as f:
        initial_schema = json.load(f)
    
    with open(ANALYZED_SCHEMA_FILE, 'r') as f:
        analyzed_schema = json.load(f)

    # Flatten initial_schema to match analyzed_schema's field-based list
    # Analyzed schema uses dot notation for paths (e.g. "socials.twitter")
    user_constraints = {}
    
    def flatten_schema(schema_node, path=""):
        if isinstance(schema_node, dict):
            if path:
                user_constraints[path] = {"user_type": "object"}
            for k, v in schema_node.items():
                new_path = f"{path}.{k}" if path else k
                flatten_schema(v, new_path)
        elif isinstance(schema_node, list) and schema_node:
            # Array itself
            if path:
                user_constraints[path] = {"user_type": "array"}
            # Array template
            new_path = f"{path}[]"
            flatten_schema(schema_node[0], new_path)
        else:
            # Leaf node (primitive type)
            if path:
                user_constraints[path] = {
                    "user_type": schema_node
                }

    flatten_schema(initial_schema)

    # Merge into analyzed_schema fields
    for field in analyzed_schema.get('fields', []):
        f_name = field['field_name']
        if f_name in user_constraints:
            field['user_constraints'] = user_constraints[f_name]
        else:
            field['user_constraints'] = None
            # If it's in analyzed but not in initial, it was likely an unmapped field from the buffer.
            # Buffer fields pass through into cleaned_data.json via fuzzy match or retaining extra fields,
            # meaning they end up in analyzed_schema.json with user_constraints: null.
            # This is exactly the signal the classifier expects to treat it as "no schema declaration, fall back to heuristic scoring".

    with open(METADATA_FILE, 'w') as f:
        json.dump(analyzed_schema, f, indent=4)
        
    return analyzed_schema
    
    print(f"\n[SUCCESS] Pipeline Phase 3c Complete.")
    print(f"[+] Consolidated Metadata: {METADATA_FILE}")

if __name__ == "__main__":
    merge_metadata()
