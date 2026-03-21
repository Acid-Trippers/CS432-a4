"""
Evaluates merged schema metadata to algorithmically route each field mechanism into the optimal 
database pipeline (SQL, MongoDB, or Quarantine/Unknown).

- Statistical Triage (Pass 1): Grades fields based purely on historical density. Stable, dense variables (>50% frequency, 100% type stability) are tentatively marked for SQL. Unstable or highly sparse data is slated for Mongo.
- Anomaly Quarantine: Catches extreme outlier fields (e.g., <1% frequency) and forces them into an UNKNOWN buffer state for manual review.
- Structural Pruning (Pass 2): Limits relational complexity by automatically exiling any deeply nested components (Depth > 2) and all of their children straight into the Mongo document pipeline, overriding initial statistical scores.
- Final Designation: Embeds the ultimate routing choice and confidence justification back into the metadata.json file to dictate actual Database table generation.
"""

import json
import os
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from src.config import DATA_DIR, METADATA_FILE

@dataclass
class FieldStats:
    fieldName: str
    frequency: float
    dominantType: str
    typeStability: float
    cardinality: float
    isNested: bool
    isArray: bool
    nestingDepth: int = 0
    parentPath: Optional[str] = None

class SchemaClassifier:
    def __init__(self, user_schema: Dict = None):
        self.user_schema = user_schema or {}

    def classify_statistically(self, field: FieldStats) -> Dict[str, Any]:
        """
        Phase 1: Determine decision based ONLY on statistics and frequency.
        """
        # 1. The Unknown Gate (Rare Data)
        if field.frequency < 0.01:
            return {
                "decision": "UNKNOWN",
                "confidence": round(1.0 - field.frequency, 3),
                "reason": f"Rare field (seen in only {field.frequency:.1%})"
            }

        # 2. Statistical Merit (Stable & Dense)
        # We define SQL-worthy as: Type stays the same AND it's usually present.
        is_stable = field.typeStability >= 1.0
        is_dense = field.frequency >= 0.50

        if is_stable and is_dense:
            return {
                "decision": "SQL",
                "confidence": round((field.typeStability + field.frequency) / 2, 3),
                "reason": "Stable and Dense (Statistical Merit)"
            }
        else:
            return {
                "decision": "MONGO",
                "confidence": 0.8,
                "reason": f"Unstable or Sparse (Freq: {field.frequency:.1%}, Stability: {field.typeStability:.1%})"
            }

def runPipeline():
    if not os.path.exists(METADATA_FILE):
        print(f"[X] ERROR: {METADATA_FILE} not found.")
        return

    with open(METADATA_FILE, 'r', encoding='utf-8') as f:
        analyzed_data = json.load(f)

    classifier = SchemaClassifier()
    field_dict = {}

    # --- PASS 1: INDIVIDUAL STATISTICAL EVALUATION ---
    for field in analyzed_data['fields']:
        stats = FieldStats(
            fieldName=field['field_name'],
            frequency=field['frequency'],
            dominantType=field['dominant_type'],
            typeStability=field['type_stability'],
            cardinality=field['cardinality'],
            isNested=field['is_nested'],
            isArray=field['is_array'],
            nestingDepth=field.get('nesting_depth', 0),
            parentPath=field.get('parent_path', None)
        )
        
        # Get decision based purely on numbers (ignoring structure for now)
        result = classifier.classify_statistically(stats)
        
        field['decision'] = result['decision']
        field['confidence'] = result['confidence']
        field['reason'] = result['reason']
        
        # Store in dict for Phase 2 lookups
        field_dict[field['field_name']] = field

    # --- PASS 2: STRUCTURAL DELINEATION (DEEP NESTING) ---
    # Logic: If a field contains more than 2 levels of nesting *inside it*, 
    # the entire field and ALL of its children are routed to MONGO to prevent fragmenting 
    # deep documents across SQL tables.
    
    # First, calculate the maximum depth of any descendant for each field
    max_descendant_depth = {}
    for field in analyzed_data['fields']:
        path = field['field_name']
        depth = field.get('nesting_depth', 0)
        
        # Every field is its own descendant of depth `depth`
        max_descendant_depth[path] = depth
        
        # Update your parents' max depths
        parts = path.split('.')
        # Handle array brackets in paths (if any) while resolving parents
        # This simple loop registers the max depth backwards up the chain
        for i in range(1, len(parts)):
            parent_path = ".".join(parts[:i])
            parent_path_no_array = parent_path.replace('[]', '')
            
            # Simple parent substring matching
            for p in field_dict:
                if path.startswith(p):
                    max_descendant_depth[p] = max(max_descendant_depth.get(p, 0), depth)

    # Now apply the Mongo exile rule top-down
    for field in analyzed_data['fields']:
        path = field['field_name']
        depth = field.get('nesting_depth', 0)
        max_depth = max_descendant_depth.get(path, depth)
        
        internal_nesting_levels = max_depth - depth
        
        # 1. Does this field have > 2 levels of nesting INSIDE it?
        if internal_nesting_levels > 2:
            field['decision'] = "MONGO"
            field['reason'] = f"Exiled: Contains {internal_nesting_levels} levels of deep nesting"
            field['confidence'] = 1.0
            continue
            
        # 2. Is this field a child of something that was already exiled to Mongo?
        # Check parents to see if they were forced into Mongo
        inherited_exile = False
        for p_path, p_meta in field_dict.items():
            if path != p_path and path.startswith(p_path):
                # If the parent was exiled to Mongo due to deep nesting
                p_internal_levels = max_descendant_depth.get(p_path, p_meta.get('nesting_depth', 0)) - p_meta.get('nesting_depth', 0)
                if p_internal_levels > 2:
                    field['decision'] = "MONGO"
                    field['reason'] = f"Inherited Exile from deep parent object ({p_path})"
                    field['confidence'] = 1.0
                    inherited_exile = True
                    break
        
        if inherited_exile:
            continue

    # --- FINAL SUMMARY & OUTPUT ---
    sql_count = 0
    mongo_count = 0
    unknown_count = 0

    print("\n" + "="*80)
    print("HYBRID CLASSIFICATION RESULTS")
    print("="*80)
    print(f"{'Field':<40} {'Decision':<10} {'Reason'}")
    print("-"*80)
    
    for field in analyzed_data['fields']:
        dec = field['decision']
        if dec == 'SQL': sql_count += 1
        elif dec == 'MONGO': mongo_count += 1
        else: unknown_count += 1
        
        print(f"{field['field_name']:<40} {dec:<10} {field.get('reason', '')}")
    
    print("-"*80)
    print(f"SQL: {sql_count} | Mongo: {mongo_count} | Unknown: {unknown_count}")
    print("="*80)

    with open(METADATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(analyzed_data, f, indent=4)
    print(f"\n[+] Metadata finalized at {METADATA_FILE}")

def run_classification():
    runPipeline()

if __name__ == "__main__":
    run_classification()