"""
Field Decomposition Module
Purpose: Maps nested JSON structures to Relational SQL patterns.

Logic:
  - Applies a Depth <= 2 constraint for normalization.
  - Pattern 1 (1-to-1): Nested 'OBJECT' becomes a separate table.
  - Pattern 2 (1-to-N): 'ARRAY_OF_OBJECTS' becomes a separate table for rows.
  - Pattern 3 (Junction): 'ARRAY_OF_PRIMITIVES' becomes a bridge table.
  - Depth > 2: Data is collapsed into 'native_storage' (JSON/String).

Example:
  {person: {address: {house: ""}}} 
  -> rel_person (Table) 
  -> rel_person_address (Table) 
  -> house (Column in rel_person_address)
"""

import json
import os
from config import METADATA_FILE

class SQLDecomposer:
    def __init__(self):
        self.metadata_data = {}

    def _load_data(self):
        if not os.path.exists(METADATA_FILE):
            print(f"[!] Error: {METADATA_FILE} is missing. Run classifier first.")
            return False
            
        with open(METADATA_FILE, 'r', encoding='utf-8') as f:
            self.metadata_data = json.load(f)
        return True

    def run_decomposition(self):
        if not self._load_data():
            return

        for field in self.metadata_data.get('fields', []):
            fname = field['field_name']
            depth = field['nesting_depth']
            decision = field.get('decision')
            
            if not decision:
                continue

            # Identify structural type from analyzer metrics
            if field.get('is_nested') and not field.get('is_array'):
                stype = "OBJECT"
            elif field.get('is_array') and field.get('array_content_type') == 'object':
                stype = "ARRAY_OF_OBJECTS"
            elif field.get('is_array') and field.get('array_content_type') == 'primitive':
                stype = "ARRAY_OF_PRIMITIVES"
            else:
                stype = "SCALAR"

            is_sql_candidate = decision == 'SQL'
            
            # Identify immediate parent for relational linking
            parent_path = field.get('parent_path')
            if parent_path:
                parent_table = f"rel_{parent_path.replace('.', '_')}"
            else:
                parent_table = "main_records"

            # ENFORCE DEPTH & SQL STRATEGY
            if is_sql_candidate and depth <= 2:
                
                # PATTERN 1: Nested Dictionary (1-to-1)
                if stype == "OBJECT":
                    field["decomposition_strategy"] = "separate_table_1_to_1"
                    field["table_config"] = {
                        "table_name": f"rel_{fname.replace('.', '_')}",
                        "relationship": "ONE_TO_ONE",
                        "parent_table": parent_table
                    }

                # PATTERN 2: Array of Dictionaries (1-to-N)
                elif stype == "ARRAY_OF_OBJECTS":
                    field["decomposition_strategy"] = "separate_table_1_to_N"
                    field["table_config"] = {
                        "table_name": f"rel_{fname.replace('.', '_')}",
                        "relationship": "ONE_TO_MANY",
                        "parent_table": parent_table
                    }

                # PATTERN 3: Array of Primitives (Junction Table)
                elif stype == "ARRAY_OF_PRIMITIVES":
                    field["decomposition_strategy"] = "junction_table"
                    field["table_config"] = {
                        "table_name": f"jt_{fname.replace('.', '_')}",
                        "relationship": "MANY_TO_MANY_EMULATED",
                        "parent_table": parent_table
                    }

                # DEFAULT: Scalar Leaf Node (Direct Column)
                else:
                    field["decomposition_strategy"] = "direct_column"
                    field["table_config"] = {
                        "target_table": parent_table
                    }

            # OVERRIDE: Depth > 2 or Mongo Decision
            else:
                if is_sql_candidate and depth > 2:
                    # Logic should ideally stay consistent, but we can flag it
                    field["decomposition_reason"] = f"OVERRIDE: SQL candidate demoted. Depth {depth} > 2 limit."
                
                field["decomposition_strategy"] = "native_storage"
                field["table_config"] = None

        with open(METADATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(self.metadata_data, f, indent=4)

        print(f"\n" + "="*50)
        print(f"SUCCESS: Decomposition completed (Stage 3).")
        print(f"Relational patterns applied for Depth <= 2.")
        print(f"Updated {METADATA_FILE}")
        print("="*50)

if __name__ == "__main__":
    SQLDecomposer().run_decomposition()

if __name__ == "__main__":
    SQLDecomposer().run_decomposition()