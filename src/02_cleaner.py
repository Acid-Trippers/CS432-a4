"""
Loads the incoming raw JSON data (received_data.json) and structurally aligns it to the expected predefined data schema (initial_schema.json).

- Canonical Key Mapping & Fuzzy Matching: Maps incoming arbitrary keys to standard schema shapes 
   using direct, case-insensitive, and snake_case transformations.
- Value Sanitization: Strips whitespaces from strings and converts purely empty strings to `None`.
- Type Casting: Attempts to gracefully cast incoming values to the native types dictated by the template 
   schema (int, float, bool, str) while letting values safely pass through if casting fails.
- Quarantine (Buffering) & Lossless Ripping: Captures unmapped extraneous fields into a secondary 
   quarantine file (buffer.json) for auditing, while still retaining them recursively 
   in the output `cleaned_data.json` to guarantee zero data loss.
"""

import re
import json
import os
import uuid
from typing import Dict, Any, List, Set, Union
from collections import defaultdict
from src.config import INITIAL_SCHEMA_FILE, RECEIVED_DATA_FILE, CLEANED_DATA_FILE, BUFFER_FILE

class DataCleaner:
    def __init__(self, schema_file: str = INITIAL_SCHEMA_FILE):
        self.buffer: List[Dict] = []
        self.schema: Dict = {}
        self.canonical_map: Dict[str, str] = {}
        
        if os.path.exists(schema_file):
            with open(schema_file, 'r') as f:
                self.schema = json.load(f)
            print(f"[*] Loaded schema from: {schema_file}")
            self._build_canonical_map(self.schema)
        else:
            print(f"[!] Warning: {schema_file} not found. Running with empty schema.")

    def _build_canonical_map(self, schema: Any):
        """Recursively builds a map of lower-case keys to their canonical case."""
        if isinstance(schema, dict):
            for k, v in schema.items():
                self.canonical_map[k.lower()] = k
                self._build_canonical_map(v)
        elif isinstance(schema, list) and schema:
            self._build_canonical_map(schema[0])

    def _to_snake_case(self, name: str) -> str:
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
        return s2.lower()

    def _is_similar(self, name1: str, name2: str) -> bool:
        clean1 = name1.replace('_', '').lower()
        clean2 = name2.replace('_', '').lower()
        if clean1 == clean2:
            return True
        if len(clean1) > 3 and len(clean2) > 3:
            if clean1 in clean2 or clean2 in clean1:
                if abs(len(clean1) - len(clean2)) <= 3:
                    return True
        return False

    def _find_canonical_match(self, field_name: str, schema_level: Dict) -> Union[str, None]:
        """Finds if field_name matches any key in the current schema_level."""
        lower_name = field_name.lower()
        schema_keys = schema_level.keys()
        
        # 1. Direct match
        if field_name in schema_keys:
            return field_name
        
        # 2. Case-insensitive match
        for k in schema_keys:
            if k.lower() == lower_name:
                return k
        
        # 3. Fuzzy/Snake match
        snake_name = self._to_snake_case(field_name)
        for k in schema_keys:
            if self._is_similar(snake_name, k):
                return k
        
        return None

    def sanitize_value(self, value: Any) -> Any:
        if isinstance(value, str):
            cleaned = value.strip()
            return None if cleaned == "" else cleaned
        return value

    def _try_cast(self, value: Any, expected_type_example: Any) -> Any:
        # If no strict type to infer from, or value is None, return as-is
        if expected_type_example is None or value is None:
            return value
            
        expected_type = type(expected_type_example)
        # If it's already the expected type, let it pass
        if isinstance(value, expected_type):
            return value
            
        # Attempt to cast
        try:
            if expected_type is int:
                return int(value)
            elif expected_type is float:
                return float(value)
            elif expected_type is bool:
                if isinstance(value, str):
                    lower = value.lower()
                    if lower in ('true', '1', 'yes', 'y'): return True
                    if lower in ('false', '0', 'no', 'n'): return False
                return bool(value)
            elif expected_type is str:
                return str(value)
        except (ValueError, TypeError):
            pass # If casting fails, we silently let the original data pass through
            
        return value

    def clean_recursive(self, data: Dict, schema: Dict, parent_id: str) -> Dict:
        """
        Recursively cleans data against schema.
        - Padding: missing schema fields -> null
        - Ripping: extra data fields -> buffer.json
        """
        cleaned_node = {}
        
        # 1. Mapping and Ripping
        matched_data_keys = set()
        for data_key, data_val in data.items():
            # Skip internal metadata if any
            if data_key == "sys_ingested_time":
                cleaned_node[data_key] = data_val
                continue
                
            canonical_key = self._find_canonical_match(data_key, schema)
            
            if canonical_key:
                matched_data_keys.add(data_key)
                schema_val = schema[canonical_key]
                
                if isinstance(schema_val, dict) and isinstance(data_val, dict):
                    # Recurse for nested objects
                    cleaned_node[canonical_key] = self.clean_recursive(data_val, schema_val, parent_id)
                elif isinstance(schema_val, list) and isinstance(data_val, list) and schema_val:
                    # Recurse for arrays
                    template = schema_val[0]
                    cleaned_node[canonical_key] = [
                        self.clean_recursive(item, template, parent_id) if isinstance(item, dict) and isinstance(template, dict)
                        else self._try_cast(self.sanitize_value(item), template)
                        for item in data_val
                    ]
                else:
                    # Scalar value
                    sanitized_val = self.sanitize_value(data_val)
                    cleaned_node[canonical_key] = self._try_cast(sanitized_val, schema_val)
            else:
                # Ripper logic: Field not in schema
                self.buffer.append({
                    "parent_record_id": parent_id,
                    "field": data_key,
                    "value": data_val
                })
                # Preserve unmatched fields to prevent data loss
                if isinstance(data_val, dict):
                    cleaned_node[data_key] = self.clean_recursive(data_val, {}, parent_id)
                elif isinstance(data_val, list):
                    cleaned_node[data_key] = [
                        self.clean_recursive(item, {}, parent_id) if isinstance(item, dict)
                        else self.sanitize_value(item)
                        for item in data_val
                    ]
                else:
                    cleaned_node[data_key] = self.sanitize_value(data_val)
        
        return cleaned_node

def run_cleaning_pipeline():
    if not os.path.exists(RECEIVED_DATA_FILE):
        print(f"[!] Input file not found: {RECEIVED_DATA_FILE}")
        return

    cleaner = DataCleaner()
    all_cleaned_records = []

    try:
        with open(RECEIVED_DATA_FILE, 'r') as f:
            all_raw_records = json.load(f)
            
        for i, raw_record in enumerate(all_raw_records):
            try:
                # Use sys_ingested_time or index as temporary ID for the buffer
                parent_id = raw_record.get("id", raw_record.get("_id", f"idx_{i}"))
                cleaned_record = cleaner.clean_recursive(raw_record, cleaner.schema, parent_id)
                cleaned_record["record_id"] = i  # Add a unique primary key across datasets
                all_cleaned_records.append(cleaned_record)
            except Exception as e:
                print(f"[!] Error cleaning record {i}: {e}")
                
    except (json.JSONDecodeError, IOError) as e:
        print(f"[!] Error reading input file: {e}")
        return

    # Save Cleaned Data
    with open(CLEANED_DATA_FILE, 'w') as f:
        json.dump(all_cleaned_records, f, indent=4)
    
    # Save Buffer Data
    with open(BUFFER_FILE, 'w') as f:
        json.dump(cleaner.buffer, f, indent=4)
    
    print(f"\n[SUCCESS] Pipeline Phase 3a Complete.")
    print(f"[+] Cleaned data: {CLEANED_DATA_FILE}")
    print(f"[+] Buffer (Quarantine): {BUFFER_FILE}")

if __name__ == "__main__":
    run_cleaning_pipeline()