import re
import os
import json
from typing import Dict, Any, List
from collections import defaultdict
from config import DATA_DIR, CLEANED_DATA_FILE, ANALYZED_DATA_FILE


class DataAnalyzer:
    def __init__(self):
        self.total_records = 0
        self.field_counts = defaultdict(int)
        self.field_types = defaultdict(lambda: defaultdict(int))
        self.field_values = defaultdict(set)
        self.value_count_limit = 10000
        self.is_cardinality_capped = defaultdict(bool) # new addition
        self.nesting_depths = defaultdict(int) # new addition
        self.is_nested = defaultdict(bool)
        self.is_array = defaultdict(bool)
        self.array_content_types = defaultdict(str) # new addition
        self.parent_paths = defaultdict(lambda: None) # new addition

    def _get_type_name(self, value: Any) -> str:
        if value is None: return 'null'
        if isinstance(value, bool): return 'boolean'
        if isinstance(value, int): return 'integer'
        if isinstance(value, float): return 'float'
        if isinstance(value, str): return 'string'
        if isinstance(value, list): return 'array'
        if isinstance(value, dict): return 'object'
        return type(value).__name__

    def _detect_pattern(self, value: Any) -> str:
        """
        Detects patterns in string values using regex masks.
        Example: 123-456-7890 -> ddd-ddd-dddd
        """
        if not isinstance(value, str): return 'none'
        if re.search(r'\d', value) and re.search(r'[.\-_]', value):
            mask = re.sub(r'\d+', 'd', value)
            return mask
        return 'none'

    def _analyze_recursive(self, field_path: str, value: Any, depth: int, parent: str = None):
        """
        Recursively analyzes the data structure.
        """
        self.field_counts[field_path] += 1
        self.nesting_depths[field_path] = max(self.nesting_depths[field_path], depth) 
        self.parent_paths[field_path] = parent

        type_name = self._get_type_name(value)
        self.field_types[field_path][type_name] += 1

        if isinstance(value, dict):
            self.is_nested[field_path] = True
            for k, v in value.items():
                if field_path:
                    child_path = f"{field_path}.{k}"
                else: # If field_path is empty, it's a top-level key
                    child_path = k
                self._analyze_recursive(child_path, v, depth + 1, field_path)
        
        elif isinstance(value, list):
            self.is_array[field_path] = True
            if value:
                sample = value[0]

                # Differentiate array of dicts from array of scalars
                content_type = 'object' if isinstance(sample, dict) else 'primitive'
                self.array_content_types[field_path] = content_type

                for i, item in enumerate(value):
                    self._analyze_recursive(f"{field_path}[]", item, depth + 1, field_path)
            else:
                self.array_content_types[field_path] = 'empty'

        else:
            # Track unique values up to a limit
            if not self.is_cardinality_capped[field_path]:
                if len(self.field_values[field_path]) < self.value_count_limit:
                    self.field_values[field_path].add(str(value))
                else:
                    self.is_cardinality_capped[field_path] = True

    def analyze_records(self, records: List[Dict]):
        for record in records:
            self.total_records += 1
            for k, v in record.items():
                self._analyze_recursive(k, v, 0) 

    def save_analysis(self, output_file: str = ANALYZED_DATA_FILE):
        # Create /data folder if doesn't already exist
        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR)

        fields_summary = []
        for f in sorted(self.field_counts.keys()):
            count = self.field_counts[f]
            freq = count / self.total_records
            type_counts = self.field_types[f]
            dom_type, type_val = max(type_counts.items(), key=lambda x: x[1])
            
            # Refined pattern detection for strings
            if dom_type == "string" and self.field_values[f]:
                sample_value = next(iter(self.field_values[f]))
                pattern = self._detect_pattern(sample_value)
                if pattern != 'none':
                    dom_type = pattern

            stability = type_val / sum(type_counts.values())
            cardinality = len(self.field_values[f]) / count if count > 0 else 0
            
            fields_summary.append({
                'field_name': f,
                'parent_path': self.parent_paths[f],
                'nesting_depth': self.nesting_depths[f],
                'frequency': freq,
                'dominant_type': dom_type,
                'type_stability': stability,
                'cardinality': cardinality,
                'is_cardinality_capped': self.is_cardinality_capped[f],
                'is_nested': self.is_nested[f],
                'is_array': self.is_array[f],
                'array_content_type': self.array_content_types[f] if self.is_array[f] else None
            })

        summary = {
            'total_records': self.total_records,
            'fields': fields_summary
        }
        with open(output_file, 'w') as f:
            json.dump(summary, f, indent=4)
        print(f"Analysis saved to {output_file}")
        return summary


    
def run_data_analysis():
    INPUT_FILE = CLEANED_DATA_FILE
    ANALYSIS_FILE = ANALYZED_DATA_FILE
    
    if os.path.exists(INPUT_FILE):
        records = []
        with open(INPUT_FILE, 'r') as f:
            for line in f:
                if line.strip():
                    records.append(json.loads(line))
        
        if not records:
            print(f"No records found in {INPUT_FILE}.")
            return

        analyzer = DataAnalyzer()
        analyzer.analyze_records(records)
        analyzer.save_analysis(ANALYSIS_FILE)

        print(f"Analysis complete: {len(records)} records from {INPUT_FILE} analyzed.")
    else:
        print(f"No data found at {INPUT_FILE}. Run ingestion first.")



if __name__ == "__main__":
    run_data_analysis()
