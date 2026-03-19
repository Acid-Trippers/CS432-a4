import re
import json
import os
from typing import Dict, Any, Set
from collections import defaultdict
from config import INITIAL_SCHEMA_FILE, RECEIVED_DATA_FILE, CLEANED_DATA_FILE

class DataCleaner:
    def __init__(self, schema_file: str = INITIAL_SCHEMA_FILE):
        self.canonical_map: Dict[str, str] = {}
        self.variations: Dict[str, Set[str]] = defaultdict(set)
        
        if os.path.exists(schema_file):
            with open(schema_file, 'r') as f:
                self.user_schema = json.load(f)
            print(f"[*] Loaded schema from: {schema_file}")
        else:
            print(f"[!] Warning: {schema_file} not found. Running in discovery mode.")
            self.user_schema = {}

        for canonical in self.user_schema.keys():
            self.canonical_map[canonical.lower()] = canonical
            self.variations[canonical].add(canonical)

    def _to_snake_case(self, name: str) -> str:
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
        return s2.lower()

    def _is_similar(self, name1: str, name2: str) -> bool:
        clean1 = name1.replace('_', '')
        clean2 = name2.replace('_', '')
        if clean1 == clean2:
            return True
        if len(clean1) > 3 and len(clean2) > 3:
            if clean1 in clean2 or clean2 in clean1:
                if abs(len(clean1) - len(clean2)) <= 3:
                    return True
        return False

    def _find_canonical_match(self, field_name: str) -> str:
        lower_name = field_name.lower()
        if lower_name in self.canonical_map:
            return self.canonical_map[lower_name]
        
        snake_name = self._to_snake_case(field_name)
        if snake_name in self.canonical_map.values():
            self.canonical_map[lower_name] = snake_name
            self.variations[snake_name].add(field_name)
            return snake_name
        
        for canonical in self.canonical_map.values():
            if self._is_similar(snake_name, canonical):
                self.canonical_map[lower_name] = canonical
                self.variations[canonical].add(field_name)
                return canonical
        
        canonical = snake_name
        self.canonical_map[lower_name] = canonical
        self.variations[canonical].add(field_name)
        return canonical

    def sanitize_value(self, value: Any) -> Any:
        if isinstance(value, str):
            cleaned = value.strip()
            return None if cleaned == "" else cleaned
        return value

    def clean_record(self, record: Dict) -> Dict:
        cleaned_dict = {}
        for k, v in record.items():
            canonical_key = self._find_canonical_match(k)
            
            if isinstance(v, dict):
                cleaned_dict[canonical_key] = self.clean_record(v)
            elif isinstance(v, list):
                cleaned_dict[canonical_key] = [
                    self.clean_record(i) if isinstance(i, dict) else self.sanitize_value(i) 
                    for i in v
                ]
            else:
                cleaned_dict[canonical_key] = self.sanitize_value(v)
        return cleaned_dict

def run_cleaning_pipeline():
    if not os.path.exists(RECEIVED_DATA_FILE):
        print(f"[!] Input file not found: {RECEIVED_DATA_FILE}")
        return

    cleaner = DataCleaner()
    all_cleaned_records = []

    with open(RECEIVED_DATA_FILE, 'r') as f:
        for line in f:
            if not line.strip(): 
                continue
            try:
                raw_record = json.loads(line)
                cleaned_record = cleaner.clean_record(raw_record)
                all_cleaned_records.append(cleaned_record)
            except Exception as e:
                print(f"[!] Error cleaning record: {e}")

    with open(CLEANED_DATA_FILE, 'w') as f:
        json.dump(all_cleaned_records, f, indent=4)
    
    print(f"\n[SUCCESS] Content cleaned and keys normalized (Structure Preserved).")
    print(f"[+] Output saved to {CLEANED_DATA_FILE}")

if __name__ == "__main__":
    run_cleaning_pipeline()