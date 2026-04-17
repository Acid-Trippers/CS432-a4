import json
import os
import re
import time
from src.config import (
    DATA_DIR,
    METADATA_FILE,
    COUNTER_FILE,
    MONGO_URI,
    MONGO_DB_NAME,
    TRANSACTION_LOG_FILE,
)
from src.phase_5.sql_engine import SQLEngine
from src.phase_5.mongo_engine import determineMongoStrategy, processNode
from src.phase_6.transaction_coordinator import TransactionCoordinator, TransactionStep
from src.phase_6.conflict_detector import get_conflict_detector, ConflictException
from pymongo import MongoClient, errors

sql_engine = None
sql_available = False
mongo_client = None
mongo_db = None
mongo_available = False


def refresh_connections():
    """Rebuild the module-level SQL and Mongo connections."""
    global sql_engine, sql_available, mongo_client, mongo_db, mongo_available

    sql_engine = SQLEngine()
    sql_available = False
    try:
        sql_engine.initialize()
        sql_available = True
        print("[SQL] Engine initialized successfully")
    except Exception as e:
        print(f"[WARNING] SQL Engine initialization failed: {str(e)[:100]}...")
        print("[WARNING] Continuing with MongoDB and Unknown data sources only")

    if mongo_client is not None:
        try:
            mongo_client.close()
        except Exception:
            pass

    mongo_client = None
    mongo_db = None
    mongo_available = False
    try:
        print(f"[MONGO] Connecting to {MONGO_URI.replace(MONGO_URI.split('@')[0].split('//')[1], '***')}...")
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        mongo_client.admin.command('ping')
        mongo_db = mongo_client[MONGO_DB_NAME]
        mongo_available = True
        print(f"[MONGO] Connected to database '{MONGO_DB_NAME}' successfully")
    except errors.ServerSelectionTimeoutError:
        print(f"[WARNING] MongoDB connection timeout - server not reachable at {MONGO_URI}")
        print("[WARNING] MongoDB features will be unavailable")
    except errors.OperationFailure as e:
        print(f"[WARNING] MongoDB authentication failed: {e}")
        print("[WARNING] MongoDB features will be unavailable")
    except Exception as e:
        print(f"[WARNING] MongoDB initialization failed: {str(e)[:150]}")
        print("[WARNING] MongoDB features will be unavailable")


refresh_connections()

tx_coordinator = TransactionCoordinator(TRANSACTION_LOG_FILE)

_FILTER_LOGIC_KEYS = {"$and", "$or", "$not"}
_FILTER_COMPARISON_OPS = {
    "$eq",
    "$ne",
    "$gt",
    "$gte",
    "$lt",
    "$lte",
    "$in",
    "$nin",
    "$contains",
    "$starts_with",
    "$ends_with",
    "$regex",
    "$exists",
}


def _normalize_type_name(raw_type):
    if raw_type is None:
        return "unknown"

    name = str(raw_type).strip().lower()
    if name in {"int", "integer", "long", "short"}:
        return "integer"
    if name in {"float", "double", "decimal", "number"}:
        return "float"
    if name in {"bool", "boolean"}:
        return "boolean"
    if name in {"str", "string", "text", "varchar", "char"}:
        return "string"

    return "string"


def _load_field_type_map():
    field_types = {"record_id": "integer"}
    try:
        with open(METADATA_FILE, "r", encoding="utf-8") as f:
            metadata = json.load(f)
    except Exception:
        return field_types

    for field in metadata.get("fields", []):
        field_name = field.get("field_name")
        if not field_name:
            continue
        user_type = (field.get("user_constraints") or {}).get("user_type")
        dominant_type = field.get("dominant_type")
        field_types[field_name] = _normalize_type_name(user_type or dominant_type)

    return field_types


def _parse_scalar_for_type(value, field_type):
    if value is None:
        return None

    if field_type == "integer":
        if isinstance(value, bool):
            raise ValueError("boolean is not valid for integer comparison")
        return int(value)

    if field_type == "float":
        if isinstance(value, bool):
            raise ValueError("boolean is not valid for numeric comparison")
        return float(value)

    if field_type == "boolean":
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "1", "yes"}:
                return True
            if lowered in {"false", "0", "no"}:
                return False
        raise ValueError(f"invalid boolean value: {value}")

    return value


def _parse_operator_shorthand(raw_value):
    if not isinstance(raw_value, str):
        return None

    match = re.match(r"^\s*(<=|>=|!=|=|<|>)\s*(.+?)\s*$", raw_value)
    if not match:
        return None

    symbol, rhs = match.groups()
    op_map = {
        "=": "$eq",
        "!=": "$ne",
        ">": "$gt",
        ">=": "$gte",
        "<": "$lt",
        "<=": "$lte",
    }
    return op_map[symbol], rhs


def _coerce_predicate_value(op, value, field_type, field_name):
    if op in {"$in", "$nin"}:
        if not isinstance(value, list):
            raise ValueError(f"Operator '{op}' for field '{field_name}' requires a list value")
        return [_parse_scalar_for_type(v, field_type) for v in value]

    if op == "$exists":
        if not isinstance(value, bool):
            raise ValueError(f"Operator '$exists' for field '{field_name}' requires true/false")
        return value

    if op in {"$contains", "$starts_with", "$ends_with", "$regex"}:
        if not isinstance(value, str):
            raise ValueError(f"Operator '{op}' for field '{field_name}' requires a string value")
        return value

    if op in {"$gt", "$gte", "$lt", "$lte"}:
        return _parse_scalar_for_type(value, field_type)

    if op in {"$eq", "$ne"}:
        return _parse_scalar_for_type(value, field_type)

    return value


def _normalize_field_condition(field_name, raw_condition, field_types):
    field_type = field_types.get(field_name, "string")

    if isinstance(raw_condition, dict):
        if not raw_condition:
            raise ValueError(f"Field '{field_name}' has an empty operator object")

        predicates = []
        for op, value in raw_condition.items():
            if op not in _FILTER_COMPARISON_OPS:
                raise ValueError(f"Unsupported operator '{op}' for field '{field_name}'")

            if op in {"$contains", "$starts_with", "$ends_with", "$regex"} and field_type != "string":
                raise ValueError(f"Operator '{op}' is only valid for string fields (field: '{field_name}')")

            if op in {"$gt", "$gte", "$lt", "$lte"} and field_type not in {"integer", "float"}:
                raise ValueError(f"Operator '{op}' is only valid for numeric fields (field: '{field_name}')")

            coerced = _coerce_predicate_value(op, value, field_type, field_name)
            predicates.append({"kind": "predicate", "field": field_name, "op": op, "value": coerced})

        if len(predicates) == 1:
            return predicates[0]
        return {"kind": "logic", "op": "$and", "children": predicates}

    shorthand = _parse_operator_shorthand(raw_condition)
    if shorthand:
        op, rhs = shorthand
        if field_type not in {"integer", "float"}:
            raise ValueError(
                f"Shorthand operator '{raw_condition}' is only valid for numeric fields (field: '{field_name}')"
            )
        coerced = _coerce_predicate_value(op, rhs, field_type, field_name)
        return {"kind": "predicate", "field": field_name, "op": op, "value": coerced}

    coerced = _coerce_predicate_value("$eq", raw_condition, field_type, field_name)
    return {"kind": "predicate", "field": field_name, "op": "$eq", "value": coerced}


def _normalize_filter_tree(filters, field_types):
    if not filters:
        return {"kind": "logic", "op": "$and", "children": []}

    if not isinstance(filters, dict):
        raise ValueError("Field 'filters' must be an object")

    logic_keys = [k for k in filters if k in _FILTER_LOGIC_KEYS]
    field_keys = [k for k in filters if k not in _FILTER_LOGIC_KEYS]

    if logic_keys and field_keys:
        raise ValueError("Do not mix logical operators ($and/$or/$not) with direct field filters at the same level")

    if logic_keys:
        if len(logic_keys) != 1:
            raise ValueError("Only one logical operator is allowed per object level")

        logic_key = logic_keys[0]
        value = filters[logic_key]

        if logic_key in {"$and", "$or"}:
            if not isinstance(value, list) or not value:
                raise ValueError(f"'{logic_key}' must be a non-empty array of filter objects")
            children = [_normalize_filter_tree(item, field_types) for item in value]
            return {"kind": "logic", "op": logic_key, "children": children}

        if not isinstance(value, dict):
            raise ValueError("'$not' must contain one filter object")
        child = _normalize_filter_tree(value, field_types)
        return {"kind": "logic", "op": "$not", "children": [child]}

    children = []
    for field_name, raw_condition in filters.items():
        if str(field_name).startswith("$"):
            raise ValueError(f"Unsupported logical operator '{field_name}'")
        children.append(_normalize_field_condition(field_name, raw_condition, field_types))

    if len(children) == 1:
        return children[0]
    return {"kind": "logic", "op": "$and", "children": children}


def _is_simple_equality_filters(filters):
    if not filters:
        return True
    if not isinstance(filters, dict):
        return False

    for key, value in filters.items():
        if str(key).startswith("$"):
            return False
        if isinstance(value, dict):
            return False
        if _parse_operator_shorthand(value):
            return False

    return True


def _to_float(value):
    if isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _evaluate_predicate(record, predicate):
    field_name = predicate["field"]
    op = predicate["op"]
    expected = predicate["value"]

    field_exists = field_name in record
    actual = record.get(field_name)

    if op == "$exists":
        return field_exists and actual is not None if expected else (not field_exists or actual is None)
    if op == "$eq":
        return actual == expected
    if op == "$ne":
        return actual != expected

    if op in {"$gt", "$gte", "$lt", "$lte"}:
        left = _to_float(actual)
        right = _to_float(expected)
        if left is None or right is None:
            return False
        if op == "$gt":
            return left > right
        if op == "$gte":
            return left >= right
        if op == "$lt":
            return left < right
        return left <= right

    if op == "$in":
        return actual in expected
    if op == "$nin":
        return actual not in expected

    if op in {"$contains", "$starts_with", "$ends_with", "$regex"}:
        if actual is None:
            return False
        actual_text = str(actual)
        if op == "$contains":
            return expected in actual_text
        if op == "$starts_with":
            return actual_text.startswith(expected)
        if op == "$ends_with":
            return actual_text.endswith(expected)
        try:
            return re.search(expected, actual_text) is not None
        except re.error:
            return False

    return False


def _record_matches_filter_ast(record, filter_ast):
    kind = filter_ast.get("kind")
    if kind == "predicate":
        return _evaluate_predicate(record, filter_ast)

    op = filter_ast.get("op")
    children = filter_ast.get("children", [])

    if op == "$and":
        return all(_record_matches_filter_ast(record, child) for child in children)
    if op == "$or":
        return any(_record_matches_filter_ast(record, child) for child in children)
    if op == "$not":
        return not _record_matches_filter_ast(record, children[0]) if children else True

    return False

def merge_results_by_record_id(results_by_db):
    """
    Merge results from multiple databases into a single output keyed by record_id.
    Each record_id maps to a merged document combining fields from all sources.
    
    Input:
        results_by_db: {"SQL": [...], "MONGO": [...], "Unknown": [...]}
    
    Output:
        {"record_id_1": {merged fields}, "record_id_2": {merged fields}, ...}
    """
    merged = {}
    
    for record in results_by_db.get("SQL", []):
        record_id = record.get("record_id")
        if record_id is not None:
            if record_id not in merged:
                merged[record_id] = {}
            merged[record_id].update(record)
            merged[record_id]["_source"] = merged[record_id].get("_source", [])
            if "SQL" not in merged[record_id]["_source"]:
                merged[record_id]["_source"].append("SQL")
    
    for record in results_by_db.get("MONGO", []):
        record_id = record.get("record_id") or record.get("_id")
        if record_id is not None:
            if record_id not in merged:
                merged[record_id] = {}
            mongo_record = {k: str(v) if k == "_id" else v for k, v in record.items()}
            merged[record_id].update(mongo_record)
            merged[record_id]["_source"] = merged[record_id].get("_source", [])
            if "MONGO" not in merged[record_id]["_source"]:
                merged[record_id]["_source"].append("MONGO")
    
    for record in results_by_db.get("Unknown", []):
        record_id = record.get("record_id")
        if record_id is not None:
            if record_id not in merged:
                merged[record_id] = {}
            merged[record_id].update(record)
            merged[record_id]["_source"] = merged[record_id].get("_source", [])
            if "Unknown" not in merged[record_id]["_source"]:
                merged[record_id]["_source"].append("Unknown")
    
    return merged


def _atomic_write_json(file_path, data):
    temp_path = f"{file_path}.tmp"
    with open(temp_path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, default=str)
    os.replace(temp_path, file_path)


def _unknown_data_file_path():
    return os.path.join(DATA_DIR, "unknown_data.json")


def _load_unknown_records():
    unknown_file = _unknown_data_file_path()
    if not os.path.exists(unknown_file):
        return []

    with open(unknown_file, "r", encoding="utf-8") as f:
        content = f.read().strip()
        if not content:
            return []
        data = json.loads(content)

    if isinstance(data, list):
        return data
    return [data]


def _write_unknown_records(records):
    _atomic_write_json(_unknown_data_file_path(), records)


def _load_top_level_schema_fields():
    """Load top-level scalar fields from metadata for stable READ column shape."""
    try:
        with open(METADATA_FILE, "r", encoding="utf-8") as f:
            metadata = json.load(f)
    except Exception:
        return []

    fields = []
    for item in metadata.get("fields", []):
        name = item.get("field_name")
        if not name:
            continue

        # Only include root-level scalar/object keys used by dashboard tables.
        if item.get("nesting_depth", 0) != 0:
            continue
        if item.get("is_array"):
            continue
        if "." in name or "[]" in name:
            continue

        fields.append(name)

    return fields


def _hydrate_missing_fields(records_by_id):
    """Backfill missing fields with None so sparse records expose full schema columns."""
    if not records_by_id:
        return records_by_id

    schema_fields = _load_top_level_schema_fields()
    if not schema_fields:
        return records_by_id

    for record in records_by_id.values():
        for field_name in schema_fields:
            if field_name not in record:
                record[field_name] = None

    return records_by_id


def _sql_records_by_ids(entity, record_ids):
    if not sql_available:
        return []
    if not record_ids:
        return []

    Model = sql_engine.models.get(entity)
    if not Model:
        return []

    from sqlalchemy import inspect as sql_inspect

    rows = (
        sql_engine.session.query(Model)
        .filter(Model.record_id.in_(record_ids))
        .all()
    )

    return [
        {col.name: getattr(row, col.name) for col in sql_inspect(Model).columns}
        for row in rows
    ]


def _sql_delete_by_record_id(entity, record_id):
    if not sql_available:
        return
    Model = sql_engine.models.get(entity)
    if not Model:
        return
    sql_engine.session.query(Model).filter(Model.record_id == record_id).delete()
    sql_engine.session.commit()


def _sql_restore_records(entity, records):
    if not sql_available:
        return
    Model = sql_engine.models.get(entity)
    if not Model:
        return

    for record in records:
        rid = record.get("record_id")
        if rid is None:
            continue
        sql_engine.session.query(Model).filter(Model.record_id == rid).delete()
        sql_engine.session.add(Model(**record))

    sql_engine.session.commit()

def read_operation(parsed_query, db_analysis):
    """
    Read records using two-phase approach:
    Phase 1: Find all matching record_ids based on filters
    Phase 2: Fetch full records using those record_ids
    """
    print(f"\n{'='*60}")
    print("[READ OPERATION - TWO PHASE APPROACH]")
    print(f"{'='*60}")
    
    entity = parsed_query.get("entity")
    filters = parsed_query.get("filters", {})
    columns = parsed_query.get("columns")  # Column selection
    databases_needed = db_analysis.get("databases_needed", [])
    field_locations = db_analysis.get("field_locations", {})

    field_types = _load_field_type_map()
    try:
        filter_ast = _normalize_filter_tree(filters, field_types)
    except ValueError as ve:
        return {
            "operation": "READ",
            "entity": entity,
            "status": "failed",
            "error": f"Invalid filters: {ve}",
        }
    
    print(f"\n[DEBUG] Entity: {entity}, Filters: {filters}, Databases: {databases_needed}")
    
    # ============================================================================
    # PHASE 1: FIND record_ids matching filters
    # ============================================================================
    print(f"\n{'─'*60}")
    print("[PHASE 1] Finding matching record_ids...")
    print(f"{'─'*60}")
    
    matching_record_ids = {}
    has_filters = bool(filters)
    simple_eq_filters = _is_simple_equality_filters(filters)
    force_full_scan = has_filters and not simple_eq_filters
    no_filters = not has_filters or force_full_scan

    if no_filters and not has_filters:
        print("[PHASE 1] No filters provided — skipping ID lookup, will fetch all records directly.")
    elif no_filters and force_full_scan:
        print("[PHASE 1] Advanced filters detected — using safe full-scan fetch before in-memory filter evaluation.")
    else:
        if "SQL" in databases_needed and sql_available:
            try:
                Model = sql_engine.models.get(entity)
                if Model:
                    query = sql_engine.session.query(Model.record_id)
                    sql_filters = {k: v for k, v in filters.items() if field_locations.get(k) == "SQL"}
                    if sql_filters:
                        for field_name, field_value in sql_filters.items():
                            query = query.filter(getattr(Model, field_name) == field_value)
                            print(f"[SQL Filter] {field_name} = {field_value}")
                    record_ids = [rid[0] for rid in query.all()]
                    matching_record_ids["SQL"] = record_ids
                    print(f"[SQL] Found {len(record_ids)} matching record_ids: {record_ids[:5]}{'...' if len(record_ids) > 5 else ''}")
            except Exception as e:
                print(f"[SQL] Error in Phase 1: {e}")
                matching_record_ids["SQL"] = []
        elif "SQL" in databases_needed:
            print(f"[SQL] Skipped (SQL Engine not available)")
            matching_record_ids["SQL"] = []

        if "MONGO" in databases_needed and mongo_available:
            try:
                collection = mongo_db[entity]
                mongo_filters = {k: v for k, v in filters.items() if field_locations.get(k) == "MONGO"}
                docs = list(collection.find(mongo_filters, {"_id": 1}))
                record_ids = [doc.get("_id") for doc in docs if "_id" in doc]
                matching_record_ids["MONGO"] = record_ids
                print(f"[MONGO] Found {len(record_ids)} matching record_ids: {record_ids[:5]}{'...' if len(record_ids) > 5 else ''}")
            except Exception as e:
                print(f"[MONGO] Error in Phase 1: {e}")
                matching_record_ids["MONGO"] = []
        elif "MONGO" in databases_needed:
            print(f"[MONGO] Skipped (MongoDB not available)")
            matching_record_ids["MONGO"] = []

        if "Unknown" in databases_needed:
            try:
                unknown_file = os.path.join(DATA_DIR, "unknown_data.json")
                if os.path.exists(unknown_file):
                    with open(unknown_file, 'r') as f:
                        data = json.load(f)
                    unknown_filters = {k: v for k, v in filters.items() if field_locations.get(k) == "Unknown"}
                    all_records = data if isinstance(data, list) else [data]
                    matching = [r for r in all_records if all(r.get(k) == v for k, v in unknown_filters.items())] if unknown_filters else all_records
                    record_ids = [r.get("record_id") for r in matching if "record_id" in r]
                    matching_record_ids["Unknown"] = record_ids
                    print(f"[Unknown] Found {len(record_ids)} matching record_ids: {record_ids[:5]}{'...' if len(record_ids) > 5 else ''}")
            except Exception as e:
                print(f"[Unknown] Error in Phase 1: {e}")
                matching_record_ids["Unknown"] = []

    # ============================================================================
    # PHASE 2: FETCH full records (all records if no filters, else by matched IDs)
    # ============================================================================
    print(f"\n{'─'*60}")
    print("[PHASE 2] Fetching full records...")
    print(f"{'─'*60}")

    results = {}

    # SQL
    sql_needed = "SQL" in databases_needed and sql_available
    sql_ids = matching_record_ids.get("SQL")
    if sql_needed and (no_filters or sql_ids):
        try:
            Model = sql_engine.models.get(entity)
            if Model:
                from sqlalchemy import inspect as sql_inspect
                query = sql_engine.session.query(Model)
                if not no_filters and sql_ids:
                    query = query.filter(Model.record_id.in_(sql_ids))
                records = query.all()
                # Filter columns if specified (always include record_id)
                cols_to_fetch = columns if columns else [col.name for col in sql_inspect(Model).columns]
                if columns and "record_id" not in cols_to_fetch:
                    cols_to_fetch = ["record_id"] + columns
                results["SQL"] = [
                    {col.name: getattr(record, col.name) for col in sql_inspect(Model).columns if col.name in cols_to_fetch}
                    for record in records
                ]
                print(f"[SQL] Fetched {len(results['SQL'])} records with columns: {cols_to_fetch}")
        except Exception as e:
            print(f"[SQL] Error in Phase 2: {e}")
            results["SQL"] = []
    else:
        results["SQL"] = []

    # MONGO
    mongo_needed = "MONGO" in databases_needed and mongo_available
    mongo_ids = matching_record_ids.get("MONGO")
    if mongo_needed and (no_filters or mongo_ids):
        try:
            collection = mongo_db[entity]
            query_filter = {} if no_filters else {"_id": {"$in": mongo_ids}}
            records = list(collection.find(query_filter))
            # Filter columns if specified (always include _id/record_id)
            cols_to_fetch = columns if columns else None
            results["MONGO"] = []
            for r in records:
                if cols_to_fetch:
                    filtered = {"record_id": r["_id"], **{k: v for k, v in r.items() if k != "_id" and k in cols_to_fetch}}
                else:
                    filtered = {"record_id": r["_id"], **{k: v for k, v in r.items() if k != "_id"}}
                results["MONGO"].append(filtered)
            cols_msg = columns if columns else "all"
            print(f"[MONGO] Fetched {len(results['MONGO'])} documents with columns: {cols_msg}")
        except Exception as e:
            print(f"[MONGO] Error in Phase 2: {e}")
            results["MONGO"] = []
    else:
        results["MONGO"] = []

    # Unknown
    unknown_ids = matching_record_ids.get("Unknown")
    if "Unknown" in databases_needed and (no_filters or unknown_ids):
        try:
            unknown_file = os.path.join(DATA_DIR, "unknown_data.json")
            if os.path.exists(unknown_file):
                with open(unknown_file, 'r') as f:
                    data = json.load(f)
                all_records = data if isinstance(data, list) else [data]
                results["Unknown"] = all_records if no_filters else [r for r in all_records if r.get("record_id") in unknown_ids]
                print(f"[Unknown] Fetched {len(results['Unknown'])} full records")
        except Exception as e:
            print(f"[Unknown] Error in Phase 2: {e}")
            results["Unknown"] = []
    else:
        results["Unknown"] = []
    
    print(f"\n{'='*60}")
    print("[SUMMARY] Total records fetched:")
    print(f"  SQL: {len(results.get('SQL', []))}")
    print(f"  MONGO: {len(results.get('MONGO', []))}")

    # ============================================================================
    # PHASE 3: MERGE results by record_id
    # ============================================================================
    print(f"\n{'─'*60}")
    print("[PHASE 3] Merging results by record_id...")
    print(f"{'─'*60}")
    
    merged_results = merge_results_by_record_id(results)
    merged_results = _hydrate_missing_fields(merged_results)

    if has_filters:
        before_filter_count = len(merged_results)
        merged_results = {
            rid: record
            for rid, record in merged_results.items()
            if _record_matches_filter_ast(record, filter_ast)
        }
        print(f"[MERGE] Filtered records: {before_filter_count} -> {len(merged_results)}")
    
    # Filter columns if specified
    if columns:
        filtered_merged = {}
        for record_id, record in merged_results.items():
            filtered_merged[record_id] = {k: v for k, v in record.items() if k in columns or k == "record_id"}
        merged_results = filtered_merged
        print(f"[MERGE] Filtered to columns: {columns}")
    
    print(f"[MERGE] Merged {len(merged_results)} unique records")
    print(f"[MERGE] Sample keys: {list(merged_results.keys())[:5]}")
    
    return {"operation": "READ", "entity": entity, "data": merged_results}
    
def create_operation(parsed_query, db_analysis):
    """
    Create a new record using metadata routing:
    Phase 1: Route fields based on metadata decisions
    Phase 2: Insert into appropriate databases

    Both SQL and MongoDB always receive a record to keep record_id in sync.
    SQL gets at minimum {"record_id": N}, MongoDB gets at minimum {"_id": N}.
    This ensures READ merge always finds the record on both sides.
    """
    print(f"\n{'='*60}")
    print("[CREATE OPERATION - MULTI-DATABASE ROUTING]")
    print(f"{'='*60}")
    
    entity = parsed_query.get("entity")
    payload = parsed_query.get("payload", {})
    
    print(f"\n[DEBUG] Entity: {entity}, Payload Size: {len(payload)} fields")
    
    record_id = 0
    if os.path.exists(COUNTER_FILE):
        try:
            with open(COUNTER_FILE, 'r') as f:
                record_id = int(f.read().strip() or 0)
        except Exception as e:
            print(f"[WARNING] Could not read counter file: {e}")

    payload["record_id"] = record_id
    print(f"[INFO] Assigned sequential record_id: {record_id}")

    try:
        with open(COUNTER_FILE, 'w') as f:
            f.write(str(record_id + 1))
    except Exception as e:
        print(f"[WARNING] Could not update counter file: {e}")

    # ============================================================================
    # PHASE 1: ROUTE PAYLOAD FIELDS
    # ============================================================================
    print(f"\n{'─'*60}")
    print("[PHASE 1] Routing payload fields based on metadata...")
    print(f"{'─'*60}")

    sql_payload = {"record_id": record_id}
    mongo_payload = {"record_id": record_id}
    unknown_payload = {"record_id": record_id}
    
    field_locations = db_analysis.get("field_locations", {})
    strategy_map = {}
    try:
        with open(METADATA_FILE, 'r') as f:
            metadata = json.load(f)
        fields = metadata.get("fields", [])
        strategy_map = determineMongoStrategy(fields)
    except Exception as e:
        print(f"[WARNING] Could not load MongoDB strategy: {e}")

    for key, value in payload.items():
        if key == "record_id":
            continue
        decision = field_locations.get(key, "Unknown")
        if decision == "SQL":
            sql_payload[key] = value
        elif decision == "MONGO":
            mongo_payload[key] = value
        else:
            unknown_payload[key] = value
            
    print(f"[Routing] SQL: {len(sql_payload)-1} fields, MongoDB: {len(mongo_payload)-1} fields, Unknown: {len(unknown_payload)-1} fields")

    # ============================================================================
    # PHASE 2: INSERT INTO DATABASES
    # ============================================================================
    print(f"\n{'─'*60}")
    print("[PHASE 2] Executing database insertions...")
    print(f"{'─'*60}")

    participants = []
    if sql_available:
        participants.append("SQL")
    else:
        return {
            "operation": "CREATE",
            "entity": entity,
            "status": "failed",
            "error": "SQL participant unavailable",
        }

    if mongo_available:
        participants.append("MONGO")
    else:
        return {
            "operation": "CREATE",
            "entity": entity,
            "status": "failed",
            "error": "MongoDB participant unavailable",
        }

    has_unknown_fields = len(unknown_payload) > 1
    if has_unknown_fields:
        participants.append("Unknown")

    unknown_before = _load_unknown_records() if has_unknown_fields else []

    def apply_sql_create():
        inserted_id = sql_engine.insert_record(sql_payload)
        if inserted_id is None:
            raise RuntimeError("SQL insert returned None")
        return inserted_id

    def compensate_sql_create():
        _sql_delete_by_record_id(entity, record_id)

    def verify_sql_create(result):
        return result is not None

    def apply_mongo_create():
        processed_mongo_record = processNode(mongo_payload, "", mongo_db, strategy_map)
        # Keep SQL and Mongo identity aligned for cross-database atomicity checks.
        processed_mongo_record["_id"] = record_id
        return mongo_db[entity].insert_one(processed_mongo_record)

    def compensate_mongo_create():
        # Support rollback for both legacy docs keyed by record_id field and
        # new docs keyed by _id == record_id.
        mongo_db[entity].delete_many({"$or": [{"_id": record_id}, {"record_id": record_id}]})

    def verify_mongo_create(result):
        return getattr(result, "acknowledged", False)

    steps = [
        TransactionStep(
            name="create_sql_record",
            participant="SQL",
            apply_fn=apply_sql_create,
            compensate_fn=compensate_sql_create,
            verify_fn=verify_sql_create,
        ),
        TransactionStep(
            name="create_mongo_document",
            participant="MONGO",
            apply_fn=apply_mongo_create,
            compensate_fn=compensate_mongo_create,
            verify_fn=verify_mongo_create,
        ),
    ]

    if has_unknown_fields:
        def apply_unknown_create():
            current = _load_unknown_records()
            current.append(unknown_payload)
            _write_unknown_records(current)
            return True

        def compensate_unknown_create():
            _write_unknown_records(unknown_before)

        steps.append(
            TransactionStep(
                name="create_unknown_record",
                participant="Unknown",
                apply_fn=apply_unknown_create,
                compensate_fn=compensate_unknown_create,
                verify_fn=lambda ok: bool(ok),
            )
        )

    tx_result = tx_coordinator.run(
        operation="CREATE",
        entity=entity,
        participants=participants,
        steps=steps,
        metadata={"record_id": record_id},
    )

    if tx_result.get("success"):
        results = {
            "record_id": record_id,
            "inserted_into": participants,
        }
    else:
        try:
            with open(COUNTER_FILE, 'w') as f:
                f.write(str(record_id))
        except Exception:
            pass

        results = {
            "record_id": record_id,
            "inserted_into": [],
            "error": tx_result.get("error"),
            "compensation_errors": tx_result.get("compensation_errors", []),
        }

    print(f"\n{'='*60}")
    print("[SUMMARY] Create Operation Completed")
    print(f"  Inserted into: {', '.join(results['inserted_into']) if results['inserted_into'] else 'None'}")
    print(f"{'='*60}\n")

    return {
        "operation": "CREATE",
        "entity": entity,
        "status": "success" if tx_result.get("success") else "failed",
        "transaction": {
            "transaction_id": tx_result.get("transaction_id"),
            "state": tx_result.get("state"),
        },
        "details": results,
    }
    
def update_operation(parsed_query, db_analysis):
    """
    Update records using two-phase approach:
    Phase 1: Find all matching record_ids based on filters
    Phase 2: Update records using those record_ids and new payload values
    
    If filters = {}, updates ALL records from all databases.
    Only specified columns in payload are updated.
    """
    print(f"\n{'='*60}")
    print("[UPDATE OPERATION - TWO PHASE APPROACH]")
    print(f"{'='*60}")
    
    entity = parsed_query.get("entity")
    filters = parsed_query.get("filters", {})
    payload = parsed_query.get("payload", {})
    databases_needed = db_analysis.get("databases_needed", [])
    field_locations = db_analysis.get("field_locations", {})
    
    print(f"\n[DEBUG] Entity: {entity}, Filters: {filters}, Payload: {payload}, Databases: {databases_needed}")
    
    if not filters:
        print(f"\n[WARNING] No filters specified - will UPDATE ALL records in all databases")
    
    # ============================================================================
    # CONFLICT DETECTION: Check for field-level conflicts before proceeding
    # ============================================================================
    print(f"\n{'─'*60}")
    print("[CONFLICT DETECTION] Checking for field-level conflicts...")
    print(f"{'─'*60}")
    
    # Extract fields being read (from WHERE clause) and written (from SET clause)
    read_fields = set(filters.keys()) if filters else set()
    write_fields = set(payload.keys()) if payload else set()
    
    print(f"[Fields] Read (WHERE): {read_fields}, Write (SET): {write_fields}")
    
    detector = get_conflict_detector()
    conflict_info = detector.check_conflict(
        read_fields=read_fields,
        write_fields=write_fields,
        entity=entity
    )
    
    if conflict_info:
        print(f"\n[CONFLICT DETECTED]")
        print(f"  Conflicting TX: {conflict_info['conflicting_tx_id'][:8]}")
        print(f"  Overlapping fields: {conflict_info['field_overlap']}")
        print(f"  Message: {conflict_info['message']}")
        
        return {
            "operation": "UPDATE",
            "status": "conflict",
            "entity": entity,
            "error": conflict_info['message'],
            "conflict_info": {
                "conflicting_transaction": conflict_info['conflicting_tx_id'][:8],
                "overlapping_fields": list(conflict_info['field_overlap']),
                "recommendation": "Please retry your update query - data may have changed"
            },
            "transaction": None,
        }
    
    # Register this transaction for conflict tracking
    tx_id = detector.register_transaction(
        read_fields=read_fields,
        write_fields=write_fields,
        entity=entity
    )
    print(f"\n[TRANSACTION REGISTERED] TX ID: {tx_id[:8]}")
    
    # ============================================================================
    # PHASE 1: FIND record_ids matching filters
    # ============================================================================
    print(f"\n{'─'*60}")
    print("[PHASE 1] Finding matching record_ids to update...")
    print(f"{'─'*60}")
    
    matching_record_ids = {}
    
    if "SQL" in databases_needed and sql_available:
        try:
            Model = sql_engine.models.get(entity)
            if Model:
                query = sql_engine.session.query(Model.record_id)
                sql_filters = {k: v for k, v in filters.items() if field_locations.get(k) == "SQL"}
                if sql_filters:
                    for field_name, field_value in sql_filters.items():
                        query = query.filter(getattr(Model, field_name) == field_value)
                        print(f"[SQL Filter] {field_name} = {field_value}")
                record_ids = [rid[0] for rid in query.all()]
                matching_record_ids["SQL"] = record_ids
                print(f"[SQL] Found {len(record_ids)} matching record_ids: {record_ids[:5]}{'...' if len(record_ids) > 5 else ''}")
        except Exception as e:
            print(f"[SQL] Error in Phase 1: {e}")
            matching_record_ids["SQL"] = []
    elif "SQL" in databases_needed:
        print(f"[SQL] Skipped (SQL Engine not available)")
        matching_record_ids["SQL"] = []
    
    if "MONGO" in databases_needed:
        try:
            collection = mongo_db[entity]
            mongo_filters = {k: v for k, v in filters.items() if field_locations.get(k) == "MONGO"}
            docs = list(collection.find(mongo_filters, {"_id": 1}))
            record_ids = [doc.get("_id") for doc in docs if "_id" in doc]
            matching_record_ids["MONGO"] = record_ids
            print(f"[MONGO] Found {len(record_ids)} matching record_ids: {record_ids[:5]}{'...' if len(record_ids) > 5 else ''}")
        except Exception as e:
            print(f"[MONGO] Error in Phase 1: {e}")
            matching_record_ids["MONGO"] = []
    
    if "Unknown" in databases_needed:
        try:
            unknown_file = os.path.join(DATA_DIR, "unknown_data.json")
            if os.path.exists(unknown_file):
                with open(unknown_file, 'r') as f:
                    data = json.load(f)
                unknown_filters = {k: v for k, v in filters.items() if field_locations.get(k) == "Unknown"}
                all_records = data if isinstance(data, list) else [data]
                matching = [r for r in all_records if all(r.get(k) == v for k, v in unknown_filters.items())] if unknown_filters else all_records
                record_ids = [r.get("record_id") for r in matching if "record_id" in r]
                matching_record_ids["Unknown"] = record_ids
                print(f"[Unknown] Found {len(record_ids)} matching record_ids: {record_ids[:5]}{'...' if len(record_ids) > 5 else ''}")
        except Exception as e:
            print(f"[Unknown] Error in Phase 1: {e}")
            matching_record_ids["Unknown"] = []
    
    # ============================================================================
    # PHASE 2: UPDATE records using record_ids
    # ============================================================================
    print(f"\n{'─'*60}")
    print("[PHASE 2] Updating records by record_id...")
    print(f"{'─'*60}")
    
    strategy_map = {}
    try:
        with open(METADATA_FILE, 'r') as f:
            metadata = json.load(f)
        fields = metadata.get("fields", [])
        strategy_map = determineMongoStrategy(fields)
    except Exception as e:
        print(f"[WARNING] Could not load MongoDB strategy: {e}")
    
    sql_updates = {}
    mongo_updates = {}
    unknown_updates = {}
    
    for key, value in payload.items():
        if key == "record_id":
            continue
        decision = field_locations.get(key, "Unknown")
        if decision == "SQL":
            sql_updates[key] = value
        elif decision == "MONGO":
            mongo_updates[key] = value
        else:
            unknown_updates[key] = value
    
    print(f"[Routing] SQL: {len(sql_updates)} fields, MongoDB: {len(mongo_updates)} fields, Unknown: {len(unknown_updates)} fields")
    
    updated_summary = {"SQL": 0, "MONGO": 0, "Unknown": 0}

    participants = []
    steps = []
    tx_metadata = {}

    sql_ids = matching_record_ids.get("SQL", [])
    if "SQL" in databases_needed and sql_ids and sql_updates:
        if not sql_available:
            detector.abort(tx_id)
            return {
                "operation": "UPDATE",
                "status": "failed",
                "entity": entity,
                "error": "SQL participant unavailable",
            }

        participants.append("SQL")
        sql_before = _sql_records_by_ids(entity, sql_ids)
        tx_metadata["sql_record_count"] = len(sql_before)

        def apply_sql_update():
            Model = sql_engine.models.get(entity)
            update_query = sql_engine.session.query(Model).filter(Model.record_id.in_(sql_ids))
            count = update_query.count()
            update_query.update(dict(sql_updates), synchronize_session=False)
            sql_engine.session.commit()
            updated_summary["SQL"] = count
            return count

        def compensate_sql_update():
            _sql_restore_records(entity, sql_before)

        steps.append(
            TransactionStep(
                name="update_sql_records",
                participant="SQL",
                apply_fn=apply_sql_update,
                compensate_fn=compensate_sql_update,
                verify_fn=lambda c: c >= 0,
            )
        )

    mongo_ids = matching_record_ids.get("MONGO", [])
    if "MONGO" in databases_needed and mongo_ids and mongo_updates:
        if not mongo_available:
            detector.abort(tx_id)
            return {
                "operation": "UPDATE",
                "status": "failed",
                "entity": entity,
                "error": "MongoDB participant unavailable",
            }

        participants.append("MONGO")
        mongo_collection = mongo_db[entity]
        mongo_before = list(mongo_collection.find({"_id": {"$in": mongo_ids}}))
        tx_metadata["mongo_record_count"] = len(mongo_before)

        def apply_mongo_update():
            result = mongo_collection.update_many(
                {"_id": {"$in": mongo_ids}},
                {"$set": mongo_updates},
            )
            updated_summary["MONGO"] = result.modified_count
            return result

        def compensate_mongo_update():
            for doc in mongo_before:
                mongo_collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)

        steps.append(
            TransactionStep(
                name="update_mongo_documents",
                participant="MONGO",
                apply_fn=apply_mongo_update,
                compensate_fn=compensate_mongo_update,
                verify_fn=lambda r: r.acknowledged,
            )
        )

    unknown_ids = matching_record_ids.get("Unknown", [])
    if "Unknown" in databases_needed and unknown_ids and unknown_updates:
        participants.append("Unknown")
        unknown_before = _load_unknown_records()
        tx_metadata["unknown_record_count"] = len(unknown_before)

        def apply_unknown_update():
            current = _load_unknown_records()
            changed = 0
            for record in current:
                if record.get("record_id") in unknown_ids:
                    for key, value in unknown_updates.items():
                        record[key] = value
                    changed += 1
            _write_unknown_records(current)
            updated_summary["Unknown"] = changed
            return changed

        def compensate_unknown_update():
            _write_unknown_records(unknown_before)

        steps.append(
            TransactionStep(
                name="update_unknown_records",
                participant="Unknown",
                apply_fn=apply_unknown_update,
                compensate_fn=compensate_unknown_update,
                verify_fn=lambda c: c >= 0,
            )
        )

    if not steps:
        detector.commit(tx_id)
        print(f"[TRANSACTION COMMITTED] TX ID: {tx_id[:8]}")
        return {
            "operation": "UPDATE",
            "status": "success",
            "entity": entity,
            "filters_applied": filters,
            "updated_by_database": updated_summary,
            "total_updated": 0,
            "transaction": None,
        }

    tx_result = tx_coordinator.run(
        operation="UPDATE",
        entity=entity,
        participants=participants,
        steps=steps,
        metadata=tx_metadata,
    )

    if not tx_result.get("success"):
        updated_summary = {"SQL": 0, "MONGO": 0, "Unknown": 0}
    total_updated = sum(updated_summary.values()) if tx_result.get("success") else 0
    
    print(f"\n{'='*60}")
    print("[SUMMARY] Total records updated:")
    print(f"  SQL: {updated_summary.get('SQL', 0)}")
    print(f"  MONGO: {updated_summary.get('MONGO', 0)}")
    print(f"  Unknown: {updated_summary.get('Unknown', 0)}")
    print(f"  TOTAL: {total_updated}")
    print(f"{'='*60}\n")
    
    detector.commit(tx_id)
    print(f"[TRANSACTION COMMITTED] TX ID: {tx_id[:8]}")
    
    return {
        "operation": "UPDATE",
        "status": "success" if tx_result.get("success") else "failed",
        "entity": entity,
        "filters_applied": filters,
        "updated_by_database": updated_summary,
        "total_updated": total_updated,
        "transaction": {
            "transaction_id": tx_result.get("transaction_id"),
            "state": tx_result.get("state"),
            "error": tx_result.get("error"),
            "compensation_errors": tx_result.get("compensation_errors", []),
        },
    }
    
def delete_operation(parsed_query, db_analysis):
    """
    Delete records using two-phase approach:
    Phase 1: Find all matching record_ids based on filters
    Phase 2: Delete records using those record_ids
    
    If filters = {}, deletes ALL records from all databases.
    """
    print(f"\n{'='*60}")
    print("[DELETE OPERATION - TWO PHASE APPROACH]")
    print(f"{'='*60}")
    
    entity = parsed_query.get("entity")
    filters = parsed_query.get("filters", {})
    databases_needed = db_analysis.get("databases_needed", [])
    field_locations = db_analysis.get("field_locations", {})
    
    if not filters:
        print(f"\n[WARNING] No filters specified - will DELETE ALL records from all databases")
    
    print(f"\n[DEBUG] Entity: {entity}, Filters: {filters}, Databases: {databases_needed}")
    
    # ============================================================================
    # PHASE 1: FIND record_ids matching filters
    # ============================================================================
    print(f"\n{'─'*60}")
    print("[PHASE 1] Finding matching record_ids to delete...")
    print(f"{'─'*60}")
    
    matching_record_ids = {}
    
    if "SQL" in databases_needed and sql_available:
        try:
            Model = sql_engine.models.get(entity)
            if Model:
                query = sql_engine.session.query(Model.record_id)
                sql_filters = {k: v for k, v in filters.items() if field_locations.get(k) == "SQL"}
                if sql_filters:
                    for field_name, field_value in sql_filters.items():
                        query = query.filter(getattr(Model, field_name) == field_value)
                        print(f"[SQL Filter] {field_name} = {field_value}")
                record_ids = [rid[0] for rid in query.all()]
                matching_record_ids["SQL"] = record_ids
                print(f"[SQL] Found {len(record_ids)} matching record_ids: {record_ids[:5]}{'...' if len(record_ids) > 5 else ''}")
        except Exception as e:
            print(f"[SQL] Error in Phase 1: {e}")
            matching_record_ids["SQL"] = []
    elif "SQL" in databases_needed:
        print(f"[SQL] Skipped (SQL Engine not available)")
        matching_record_ids["SQL"] = []
    
    if "MONGO" in databases_needed and mongo_available:
        try:
            collection = mongo_db[entity]
            mongo_filters = {k: v for k, v in filters.items() if field_locations.get(k) == "MONGO"}
            docs = list(collection.find(mongo_filters, {"_id": 1}))
            record_ids = [doc.get("_id") for doc in docs if "_id" in doc]
            matching_record_ids["MONGO"] = record_ids
            print(f"[MONGO] Found {len(record_ids)} matching record_ids: {record_ids[:5]}{'...' if len(record_ids) > 5 else ''}")
        except Exception as e:
            print(f"[MONGO] Error in Phase 1: {e}")
            matching_record_ids["MONGO"] = []
    elif "MONGO" in databases_needed:
        print(f"[MONGO] Skipped (MongoDB not available)")
        matching_record_ids["MONGO"] = []
    
    if "Unknown" in databases_needed:
        try:
            unknown_file = os.path.join(DATA_DIR, "unknown_data.json")
            if os.path.exists(unknown_file):
                with open(unknown_file, 'r') as f:
                    data = json.load(f)
                unknown_filters = {k: v for k, v in filters.items() if field_locations.get(k) == "Unknown"}
                all_records = data if isinstance(data, list) else [data]
                matching = [r for r in all_records if all(r.get(k) == v for k, v in unknown_filters.items())] if unknown_filters else all_records
                record_ids = [r.get("record_id") for r in matching if "record_id" in r]
                matching_record_ids["Unknown"] = record_ids
                print(f"[Unknown] Found {len(record_ids)} matching record_ids: {record_ids[:5]}{'...' if len(record_ids) > 5 else ''}")
        except Exception as e:
            print(f"[Unknown] Error in Phase 1: {e}")
            matching_record_ids["Unknown"] = []
    
    # ============================================================================
    # PHASE 2: DELETE records using record_ids
    # ============================================================================
    print(f"\n{'─'*60}")
    print("[PHASE 2] Deleting records by record_id...")
    print(f"{'─'*60}")

    matched_record_ids = sorted(
        {
            record_id
            for ids in matching_record_ids.values()
            for record_id in ids
            if record_id is not None
        }
    )
    print(f"[PHASE 2] Unified matched record_ids: {len(matched_record_ids)}")
    
    deleted_summary = {"SQL": 0, "MONGO": 0, "Unknown": 0}

    participants = []
    steps = []
    tx_metadata = {}

    sql_ids = matched_record_ids
    if sql_available and sql_ids:
        participants.append("SQL")
        sql_before = _sql_records_by_ids(entity, sql_ids)
        tx_metadata["sql_record_count"] = len(sql_before)

        def apply_sql_delete():
            Model = sql_engine.models.get(entity)
            query = sql_engine.session.query(Model).filter(Model.record_id.in_(sql_ids))
            count = query.count()
            query.delete()
            sql_engine.session.commit()
            deleted_summary["SQL"] = count
            return count

        def compensate_sql_delete():
            _sql_restore_records(entity, sql_before)

        steps.append(
            TransactionStep(
                name="delete_sql_records",
                participant="SQL",
                apply_fn=apply_sql_delete,
                compensate_fn=compensate_sql_delete,
                verify_fn=lambda c: c >= 0,
            )
        )

    mongo_ids = matched_record_ids
    if mongo_available and mongo_ids:
        participants.append("MONGO")
        mongo_collection = mongo_db[entity]
        mongo_before = list(mongo_collection.find({"_id": {"$in": mongo_ids}}))
        tx_metadata["mongo_record_count"] = len(mongo_before)

        def apply_mongo_delete():
            result = mongo_collection.delete_many({"_id": {"$in": mongo_ids}})
            deleted_summary["MONGO"] = result.deleted_count
            return result

        def compensate_mongo_delete():
            if mongo_before:
                mongo_collection.insert_many(mongo_before, ordered=False)

        steps.append(
            TransactionStep(
                name="delete_mongo_documents",
                participant="MONGO",
                apply_fn=apply_mongo_delete,
                compensate_fn=compensate_mongo_delete,
                verify_fn=lambda r: r.acknowledged,
            )
        )

    unknown_ids = matched_record_ids
    if unknown_ids:
        participants.append("Unknown")
        unknown_before = _load_unknown_records()
        tx_metadata["unknown_record_count"] = len(unknown_before)

        def apply_unknown_delete():
            current = _load_unknown_records()
            remaining = [r for r in current if r.get("record_id") not in unknown_ids]
            deleted_summary["Unknown"] = len(current) - len(remaining)
            _write_unknown_records(remaining)
            return deleted_summary["Unknown"]

        def compensate_unknown_delete():
            _write_unknown_records(unknown_before)

        steps.append(
            TransactionStep(
                name="delete_unknown_records",
                participant="Unknown",
                apply_fn=apply_unknown_delete,
                compensate_fn=compensate_unknown_delete,
                verify_fn=lambda c: c >= 0,
            )
        )

    if not steps:
        return {
            "operation": "DELETE",
            "status": "success",
            "entity": entity,
            "filters_applied": filters,
            "deleted_by_database": deleted_summary,
            "total_deleted": 0,
            "transaction": None,
        }

    tx_result = tx_coordinator.run(
        operation="DELETE",
        entity=entity,
        participants=participants,
        steps=steps,
        metadata=tx_metadata,
    )

    if not tx_result.get("success"):
        deleted_summary = {"SQL": 0, "MONGO": 0, "Unknown": 0}
    total_deleted = sum(deleted_summary.values()) if tx_result.get("success") else 0
    
    print(f"\n{'='*60}")
    print("[SUMMARY] Total records deleted:")
    print(f"  SQL: {deleted_summary.get('SQL', 0)}")
    print(f"  MONGO: {deleted_summary.get('MONGO', 0)}")
    print(f"  Unknown: {deleted_summary.get('Unknown', 0)}")
    print(f"  TOTAL: {total_deleted}")
    print(f"{'='*60}\n")
    
    return {
        "operation": "DELETE",
        "status": "success" if tx_result.get("success") else "failed",
        "entity": entity,
        "filters_applied": filters,
        "deleted_by_database": deleted_summary,
        "total_deleted": total_deleted,
        "transaction": {
            "transaction_id": tx_result.get("transaction_id"),
            "state": tx_result.get("state"),
            "error": tx_result.get("error"),
            "compensation_errors": tx_result.get("compensation_errors", []),
        },
    }