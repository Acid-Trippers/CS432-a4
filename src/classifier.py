"""
Field Classification Module
Purpose: Classify fields as SQL, Mongo, or Unknown based on:
  1. Data analysis metrics (from metadata_manager.json)
  2. User-defined schema constraints (from initial_schema.json)
  3. Type stability, structure complexity, and sparsity

Decision Logic:
  - SQL: Clean types, stable, low sparsity, enforceable constraints
  - Mongo: Nested/array structures, type-unstable, sparse, complex
  - Unknown: When confidence is low for both SQL and Mongo
"""
    
import json
import os
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from config import DATA_DIR, METADATA_MANAGER_FILE

# Thresholds for SQL suitability
SQL_CRITERIA = {
    "minTypeStability": 0.95,      # How stable is the type?
    "maxSparsity": 0.4,             # Max acceptable sparsity (1 - frequency)
    "minCardinality": 0.0,          # Min cardinality (can be low for categories)
    "complexityPenalty": -1.0       # Mono if nested/array without strong SQL signals
}

# Thresholds for Mongo suitability
MONGO_CRITERIA = {
    "maxTypeStability": 0.85,       # Type instability prefers Mongo
    "minSparsity": 0.3,             # Higher sparsity suits Mongo
    "complexAllowed": True,         # Mongo can handle nested/arrays
    "maxCardinality": 2.0           # High cardinality OK for Mongo
}

# Decision confidence thresholds
CONFIDENCE_THRESHOLDS = {
    "highConfidence": 0.75,         # Score > 0.75 = confident decision
    "lowConfidence": 0.35          # Score < 0.35 = not confident
}

@dataclass
class FieldStats:
    """Represents analyzed statistics for a single field"""
    fieldName: str
    frequency: float
    dominantType: str
    typeStability: float
    cardinality: float
    isNested: bool
    isArray: bool
    nestingDepth: int = 0
    parentPath: Optional[str] = None
    
    @property
    def sparsity(self) -> float:
        """Sparsity = 1 - frequency (how often field is missing)"""
        return 1.0 - self.frequency
    
    @property
    def isComplex(self) -> bool:
        """Is field structurally complex (nested or array)?"""
        return self.isNested or self.isArray


@dataclass
class UserSchema:
    """Represents user-defined schema constraints"""
    fieldName: str
    fieldType: str
    isUnique: bool = False
    isNotNull: bool = False

class SchemaClassifier:
    """
    Classifies fields as SQL, Mongo, or Unknown based on analyzed metrics
    and optional user schema constraints.
    """
    
    def __init__(self, user_schema: Dict[str, UserSchema] = None):
        self.user_schema = user_schema or {}
    
    def _evaluate_sql_suitability(self, field: FieldStats) -> tuple[float, List[str]]:
        """
        Evaluates how suitable a field is for SQL.
        Returns: (score 0-1, list of supporting/opposing reasons)
        
        SQL is good for:
        - Stable types (high type stability)
        - Non-sparse data (frequent presence)
        - Simple scalar types (not nested/array)
        - Enforceable constraints
        """
        reasons = []
        score = 0.0
        max_score = 3.0
        
        # Factor 1: Type Stability (0-1.0 contribution)
        if field.typeStability >= SQL_CRITERIA["minTypeStability"]:
            score += field.typeStability
            reasons.append(f"Type stable ({field.typeStability:.1%})")
        else:
            reasons.append(f"Type unstable ({field.typeStability:.1%} < {SQL_CRITERIA['minTypeStability']:.0%})")
        
        # Factor 2: Sparsity (0-1.0 contribution)
        if field.sparsity <= SQL_CRITERIA["maxSparsity"]:
            score += 1.0 - field.sparsity
            reasons.append(f"Dense field ({field.frequency:.1%} frequency)")
        else:
            reasons.append(f"Sparse field ({field.frequency:.1%} frequency)")
        
        # Factor 3: Structure Complexity (0-1.0 contribution)
        if not field.isComplex:
            score += 1.0
            reasons.append(f"Simple scalar type")
        else:
            reasons.append(f"Complex structure (nested={field.isNested}, array={field.isArray})")
        
        # Check user schema if available
        if field.fieldName in self.user_schema:
            user_type = self.user_schema[field.fieldName].fieldType
            if user_type in ["int", "float", "string", "bool"]:
                reasons.append(f"  User defined as '{user_type}' (SQL-compatible)")
            else:
                reasons.append(f"  User defined as '{user_type}' (Mongo-favorable)")
        
        normalized_score = score / max_score
        return normalized_score, reasons
    
    def _evaluate_mongo_suitability(self, field: FieldStats) -> tuple[float, List[str]]:
        """
        Evaluates how suitable a field is for Mongo.
        Returns: (score 0-1, list of supporting/opposing reasons)
        
        Mongo is good for:
        - Type-unstable fields (graceful schema handling)
        - Sparse data (optional fields)
        - Nested/array structures
        - High cardinality (flexible documents)
        """
        reasons = []
        score = 0.0
        max_score = 3.0
        
        # Factor 1: Type Instability (0-1.0 contribution)
        if field.typeStability <= MONGO_CRITERIA["maxTypeStability"]:
            type_instability = 1.0 - field.typeStability
            score += type_instability
            reasons.append(f"Type instability ({1-field.typeStability:.1%})")
        else:
            reasons.append(f"Type too stable ({field.typeStability:.1%})")
        
        # Factor 2: Sparsity (0-1.0 contribution)
        if field.sparsity >= MONGO_CRITERIA["minSparsity"]:
            score += field.sparsity
            reasons.append(f"Sparse field OK ({field.frequency:.1%} frequency)")
        else:
            reasons.append(f"Dense field ({field.frequency:.1%} frequency)")
        
        # Factor 3: Structure Complexity (0-1.0 contribution)
        if field.isComplex:
            score += 1.0
            reasons.append(f"Handles complex structure (nested={field.isNested}, array={field.isArray})")
        else:
            reasons.append(f"Simple structure (less need for Mongo)")
        
        # Check user schema if available
        if field.fieldName in self.user_schema:
            user_type = self.user_schema[field.fieldName].fieldType
            if user_type in ["json", "array_int", "array_string", "array_float", "array_bool"]:
                reasons.append(f"  User defined as '{user_type}' (Mongo-favorable)")
        
        normalized_score = score / max_score
        return normalized_score, reasons
    
    def classifyField(self, field: FieldStats) -> Dict[str, Any]:
        """
        Classifies a field as SQL, Mongo, or Unknown with detailed reasoning.
        
        Decision Process:
        1. Evaluate SQL suitability (0-1 score)
        2. Evaluate Mongo suitability (0-1 score)
        3. If SQL strong (>0.75) → SQL
        4. Else if Mongo strong (>0.75) → Mongo
        5. Else → Unknown (not confident in either)
        """
        
        sql_score, sql_reasons = self._evaluate_sql_suitability(field)
        mongo_score, mongo_reasons = self._evaluate_mongo_suitability(field)
        
        # Determine decision
        decision = "UNKNOWN"
        confidence = 0.0
        reason = "Low confidence for both SQL and Mongo"
        all_reasons = sql_reasons + mongo_reasons
        
        # SQL strong candidate?
        if sql_score >= CONFIDENCE_THRESHOLDS["highConfidence"]:
            decision = "SQL"
            confidence = sql_score
            reason = f"SQL suitable ({sql_score:.2f}/1.0): Clean types, stable, non-sparse"
        
        # Mongo strong candidate?
        elif mongo_score >= CONFIDENCE_THRESHOLDS["highConfidence"]:
            decision = "MONGO"
            confidence = mongo_score
            reason = f"Mongo suitable ({mongo_score:.2f}/1.0): Complex structure, sparse, or type-unstable"
        
        # Borderline - check which is higher
        ##**Maybe this is not that needed.
        elif sql_score > mongo_score + 0.3:
            decision = "SQL"
            confidence = sql_score
            reason = f"Prefer SQL (SQL: {sql_score:.2f} > Mongo: {mongo_score:.2f}), but not high confidence"
        
        elif mongo_score > sql_score + 0.3:
            decision = "MONGO"
            confidence = mongo_score
            reason = f"Prefer Mongo (Mongo: {mongo_score:.2f} > SQL: {sql_score:.2f}), but not high confidence"
        
        else:
            decision = "UNKNOWN"
            confidence = max(sql_score, mongo_score)
            reason = f"Unable to confidently classify (SQL: {sql_score:.2f}, Mongo: {mongo_score:.2f}) - review manually"
        
        return {
            "fieldName": field.fieldName,
            "decision": decision,
            "confidence": round(confidence, 3),
            "sql_score": round(sql_score, 3),
            "mongo_score": round(mongo_score, 3),
            "sql_analysis": sql_reasons,
            "mongo_analysis": mongo_reasons,
            "reason": reason,
            "metrics": {
                "frequency": round(field.frequency, 3),
                "typeStability": round(field.typeStability, 3),
                "cardinality": round(field.cardinality, 3),
                "sparsity": round(field.sparsity, 3),
                "isComplex": field.isComplex,
                "dominantType": field.dominantType
            }
        }

def load_user_schema(schema_file: str = None) -> Dict[str, UserSchema]:
    """
    Loads user-defined schema from initial_schema.json if it exists.
    
    Args:
        schema_file: Path to initial_schema.json (defaults to data/initial_schema.json)
    
    Returns:
        Dictionary mapping field_name → UserSchema, or empty dict if file doesn't exist
    """
    if schema_file is None:
        schema_file = os.path.join(DATA_DIR, "initial_schema.json")
    
    if not os.path.exists(schema_file):
        return {}
    
    try:
        with open(schema_file, 'r', encoding='utf-8') as f:
            schema_data = json.load(f)
        
        user_schema = {}
        for field_name, field_def in schema_data.items():
            user_schema[field_name] = UserSchema(
                fieldName=field_name,
                fieldType=field_def.get('type', 'unknown'),
                isUnique=field_def.get('unique', False),
                isNotNull=field_def.get('not_null', False)
            )
        
        print(f"[+] User schema loaded from {schema_file} ({len(user_schema)} fields)")
        return user_schema
    
    except Exception as e:
        print(f"[!] Could not load user schema: {e}")
        return {}


def runPipeline():
    """
    Main classification pipeline:
    1. Load metadata_manager.json (system analysis)
    2. Load initial_schema.json (user-defined constraints)
    3. Classify each field as SQL, Mongo, or Unknown
    4. Save results to field_metadata.json
    """
    
    # Load analyzed data
    with open(METADATA_MANAGER_FILE, 'r', encoding='utf-8') as f:
        analyzed_data = json.load(f)
    
    # Load user-defined schema (optional)
    user_schema = load_user_schema()
    
    # Initialize classifier
    classifier = SchemaClassifier(user_schema=user_schema)
    output_records = []
    
    # Classification results summary
    sql_count = 0
    mongo_count = 0
    unknown_count = 0
    
    print("\n" + "="*100)
    print("FIELD CLASSIFICATION RESULTS")
    print("="*100)
    print(f"{'Field':<25} {'Decision':<10} {'Confidence':<12} {'Reason':<35}")
    print("-"*100)
    
    # Classify each field
    for record in analyzed_data['fields']:
        field_name = record['field_name']
        
        stats = FieldStats(
            fieldName=field_name,
            frequency=record['frequency'],
            dominantType=record['dominant_type'],
            typeStability=record['type_stability'],
            cardinality=record['cardinality'],
            isNested=record['is_nested'],
            isArray=record['is_array'],
            nestingDepth=record.get('nesting_depth', 0),
            parentPath=record.get('parent_path', None)
        )
        
        # Classify field
        result = classifier.classifyField(stats)
        output_records.append(result)
        
        # Update counts
        if result['decision'] == 'SQL':
            sql_count += 1
        elif result['decision'] == 'MONGO':
            mongo_count += 1
        else:
            unknown_count += 1
        
        # Print summary line
        decision_color = result['decision']
        confidence_display = f"{result['confidence']:.2f}"
        reason_short = result['reason'].split(':')[0]  # First part of reason
        
        print(f"{field_name:<25} {decision_color:<10} {confidence_display:<12} {reason_short:<35}")
    
    # Summary statistics
    total_fields = len(output_records)
    print("-"*100)
    print(f"\nSummary: SQL={sql_count}/{total_fields} | Mongo={mongo_count}/{total_fields} | Unknown={unknown_count}/{total_fields}")
    print("="*100)
    
    # Save results
    output_file = os.path.join(DATA_DIR, 'field_metadata.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_records, f, indent=2)
    
    print(f"\n[+] Classification results saved to {output_file}")
    
    # Print detailed analysis for UNKNOWN fields
    unknown_fields = [r for r in output_records if r['decision'] == 'UNKNOWN']
    if unknown_fields:
        print(f"\n[!] {len(unknown_fields)} field(s) classified as UNKNOWN - review required:\n")
        for field_result in unknown_fields:
            print(f"  {field_result['fieldName']}")
            print(f"   Reason: {field_result['reason']}")
            print(f"   SQL Score: {field_result['sql_score']:.3f}")
            print(f"   Mongo Score: {field_result['mongo_score']:.3f}")
            print()


def run_classification():
    """Entry point for running classification pipeline"""
    runPipeline()


if __name__ == "__main__":
    run_classification() 