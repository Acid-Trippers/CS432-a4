"""
Field Classification Module
Purpose: Classify fields as SQL, Mongo, or Unknown based on:
  1. Data analysis metrics (from metadata.json)
  2. User-defined schema constraints (embedded in metadata.json)
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
from config import DATA_DIR, METADATA_FILE

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
        """
        reasons = []
        score = 0.0
        max_score = 3.0
        
        # Factor 1: Type Stability
        if field.typeStability >= SQL_CRITERIA["minTypeStability"]:
            score += field.typeStability
            reasons.append(f"Type stable ({field.typeStability:.1%})")
        else:
            reasons.append(f"Type unstable ({field.typeStability:.1%} < {SQL_CRITERIA['minTypeStability']:.0%})")
        
        # Factor 2: Sparsity
        if field.sparsity <= SQL_CRITERIA["maxSparsity"]:
            score += 1.0 - field.sparsity
            reasons.append(f"Dense field ({field.frequency:.1%} frequency)")
        else:
            reasons.append(f"Sparse field ({field.frequency:.1%} frequency)")
        
        # Factor 3: Structure Complexity
        if not field.isComplex:
            score += 1.0
            reasons.append(f"Simple scalar type")
        else:
            reasons.append(f"Complex structure (nested={field.isNested}, array={field.isArray})")
        
        # Check user schema if available
        if field.fieldName in self.user_schema:
            user_type = self.user_schema[field.fieldName].fieldType
            if user_type in ["int", "float", "string", "bool"]:
                score += 0.5
                reasons.append(f"  User defined as '{user_type}' (SQL-compatible)")
            else:
                reasons.append(f"  User defined as '{user_type}' (Mongo-favorable)")
        
        normalized_score = min(1.0, score / max_score)
        return normalized_score, reasons
    
    def _evaluate_mongo_suitability(self, field: FieldStats) -> tuple[float, List[str]]:
        """
        Evaluates how suitable a field is for Mongo.
        Returns: (score 0-1, list of supporting/opposing reasons)
        """
        reasons = []
        score = 0.0
        max_score = 3.0
        
        # Factor 1: Type Instability
        if field.typeStability <= MONGO_CRITERIA["maxTypeStability"]:
            type_instability = 1.0 - field.typeStability
            score += type_instability
            reasons.append(f"Type instability ({1-field.typeStability:.1%})")
        else:
            reasons.append(f"Type too stable ({field.typeStability:.1%})")
        
        # Factor 2: Sparsity
        if field.sparsity >= MONGO_CRITERIA["minSparsity"]:
            score += field.sparsity
            reasons.append(f"Sparse field OK ({field.frequency:.1%} frequency)")
        else:
            reasons.append(f"Dense field ({field.frequency:.1%} frequency)")
        
        # Factor 3: Structure Complexity
        if field.isComplex:
            score += 1.0
            reasons.append(f"Handles complex structure via Embedding")
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
        """
        sql_score, sql_reasons = self._evaluate_sql_suitability(field)
        mongo_score, mongo_reasons = self._evaluate_mongo_suitability(field)
        
        decision = "UNKNOWN"
        confidence = 0.0
        
        if sql_score >= CONFIDENCE_THRESHOLDS["highConfidence"]:
            decision = "SQL"
            confidence = sql_score
        elif mongo_score >= CONFIDENCE_THRESHOLDS["highConfidence"]:
            decision = "MONGO"
            confidence = mongo_score
        elif sql_score > mongo_score + 0.3:
            decision = "SQL"
            confidence = sql_score
        elif mongo_score > sql_score + 0.3:
            decision = "MONGO"
            confidence = mongo_score
        else:
            decision = "UNKNOWN"
            confidence = max(sql_score, mongo_score)
        
        return {
            "fieldName": field.fieldName,
            "decision": decision,
            "nesting_depth": field.nestingDepth,
            "parent_path": field.parentPath,
            "is_nested": field.isNested,
            "is_array": field.isArray,
            "confidence": round(confidence, 3),
            "sql_score": round(sql_score, 3),
            "mongo_score": round(mongo_score, 3),
            "sql_analysis": sql_reasons,
            "mongo_analysis": mongo_reasons,
            "reason": f"Decision: {decision} (SQL: {sql_score:.2f}, Mongo: {mongo_score:.2f})",
            "metrics": {
                "frequency": round(field.frequency, 3),
                "typeStability": round(field.typeStability, 3),
                "cardinality": round(field.cardinality, 3),
                "sparsity": round(field.sparsity, 3),
                "isComplex": field.isComplex,
                "dominantType": field.dominantType
            }
        }

def load_user_schema_from_metadata(analyzed_data: Dict) -> Dict[str, UserSchema]:
    """Extracts user-defined schema constraints from the merged metadata."""
    user_schema = {}
    for field in analyzed_data.get('fields', []):
        constraints = field.get('user_constraints')
        if constraints:
            user_schema[field['field_name']] = UserSchema(
                fieldName=field['field_name'],
                fieldType=constraints.get('user_type', 'unknown'),
                isUnique=False,
                isNotNull=constraints.get('is_required', False)
            )
    return user_schema


def runPipeline():
    if not os.path.exists(METADATA_FILE):
        print(f"[X] ERROR: {METADATA_FILE} not found. Run validation first.")
        return

    with open(METADATA_FILE, 'r', encoding='utf-8') as f:
        analyzed_data = json.load(f)
    
    user_schema = load_user_schema_from_metadata(analyzed_data)
    classifier = SchemaClassifier(user_schema=user_schema)
    
    sql_count = 0
    mongo_count = 0
    unknown_count = 0
    
    print("\n" + "="*100)
    print("FIELD CLASSIFICATION RESULTS (Stage 2)")
    print("="*100)
    print(f"{'Field':<25} {'Decision':<10} {'Confidence':<12} {'Reason':<35}")
    print("-"*100)
    
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
        
        result = classifier.classifyField(stats)
        
        # Update the field entry in-place with the decision
        field['decision'] = result['decision']
        # We can also keep some of the classification metadata if helpful, 
        # but the user specifically asked for "decision". 
        # I'll stick to what was requested to avoid cluttering metadata.json too much.
        
        if field['decision'] == 'SQL':
            sql_count += 1
        elif field['decision'] == 'MONGO':
            mongo_count += 1
        else:
            unknown_count += 1
        
        print(f"{field['field_name']:<25} {field['decision']:<10} {result['confidence']:<12.2f} {result['reason'].split(':')[0]:<35}")
    
    total_fields = len(analyzed_data['fields'])
    print("-"*100)
    print(f"\nSummary: SQL={sql_count}/{total_fields} | Mongo={mongo_count}/{total_fields} | Unknown={unknown_count}/{total_fields}")
    print("="*100)
    
    with open(METADATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(analyzed_data, f, indent=4)
    
    print(f"\n[+] Classification decisions (Stage 2) added to {METADATA_FILE}")


def run_classification():
    runPipeline()


if __name__ == "__main__":
    run_classification()