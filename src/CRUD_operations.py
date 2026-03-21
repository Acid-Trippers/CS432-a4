import json
import os
import time
from .config import DATA_DIR, METADATA_FILE
from .sql_engine import SQLEngine
from .mongo_engine import determineMongoStrategy, processNode
from pymongo import MongoClient
from .config import DATA_DIR, METADATA_FILE, COUNTER_FILE

# Initialize SQL Engine gracefully (non-blocking if PostgreSQL unavailable)
sql_engine = SQLEngine()
try:
    sql_engine.initialize()
    sql_available = True
except Exception as e:
    print(f"[WARNING] SQL Engine initialization failed: {str(e)[:100]}...")
    print("[WARNING] Continuing with MongoDB and Unknown data sources only")
    sql_available = False

mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
db_name = os.getenv("MONGO_DB_NAME", "cs432_db")
mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=2000)
mongo_db = mongo_client[db_name]

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
    databases_needed = db_analysis.get("databases_needed", [])
    field_locations = db_analysis.get("field_locations", {})
    
    print(f"\n[DEBUG] Entity: {entity}, Filters: {filters}, Databases: {databases_needed}")
    
    # ============================================================================
    # PHASE 1: FIND record_ids matching filters
    # ============================================================================
    print(f"\n{'─'*60}")
    print("[PHASE 1] Finding matching record_ids...")
    print(f"{'─'*60}")
    
    matching_record_ids = {}
    
    # Query SQL for matching record_ids
    if "SQL" in databases_needed and sql_available:
        try:
            Model = sql_engine.models.get(entity)
            if Model:
                query = sql_engine.session.query(Model.record_id)
                
                # Apply SQL filters
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
    
    # Query MongoDB for matching record_ids
    if "MongoDB" in databases_needed:
        try:
            collection = mongo_db[entity]
            mongo_filters = {k: v for k, v in filters.items() if field_locations.get(k) == "MongoDB"}
            
            projection = {"record_id": 1}
            docs = list(collection.find(mongo_filters, projection))
            record_ids = [doc.get("record_id") for doc in docs if "record_id" in doc]
            matching_record_ids["MongoDB"] = record_ids
            print(f"[MongoDB] Found {len(record_ids)} matching record_ids: {record_ids[:5]}{'...' if len(record_ids) > 5 else ''}")
        except Exception as e:
            print(f"[MongoDB] Error in Phase 1: {e}")
            matching_record_ids["MongoDB"] = []
    
    # Query Unknown for matching record_ids
    if "Unknown" in databases_needed:
        try:
            unknown_file = os.path.join(DATA_DIR, "unknown_data.json")
            if os.path.exists(unknown_file):
                with open(unknown_file, 'r') as f:
                    data = json.load(f)
                
                unknown_filters = {k: v for k, v in filters.items() if field_locations.get(k) == "Unknown"}
                all_records = data if isinstance(data, list) else [data]
                
                if unknown_filters:
                    matching = [r for r in all_records if all(r.get(k) == v for k, v in unknown_filters.items())]
                else:
                    matching = all_records
                
                record_ids = [r.get("record_id") for r in matching if "record_id" in r]
                matching_record_ids["Unknown"] = record_ids
                print(f"[Unknown] Found {len(record_ids)} matching record_ids: {record_ids[:5]}{'...' if len(record_ids) > 5 else ''}")
        except Exception as e:
            print(f"[Unknown] Error in Phase 1: {e}")
            matching_record_ids["Unknown"] = []
    
    # ============================================================================
    # PHASE 2: FETCH full records using record_ids
    # ============================================================================
    print(f"\n{'─'*60}")
    print("[PHASE 2] Fetching full records by record_id...")
    print(f"{'─'*60}")
    
    results = {}
    
    # Fetch from SQL
    if "SQL" in databases_needed and sql_available and matching_record_ids.get("SQL"):
        try:
            Model = sql_engine.models.get(entity)
            if Model:
                records = sql_engine.session.query(Model).filter(
                    Model.record_id.in_(matching_record_ids["SQL"])
                ).all()
                
                from sqlalchemy import inspect as sql_inspect
                results["SQL"] = [
                    {col.name: getattr(record, col.name) for col in sql_inspect(Model).columns}
                    for record in records
                ]
                print(f"[SQL] Fetched {len(results['SQL'])} full records")
        except Exception as e:
            print(f"[SQL] Error in Phase 2: {e}")
            results["SQL"] = []
    else:
        results["SQL"] = []
    
    # Fetch from MongoDB
    if "MongoDB" in databases_needed and matching_record_ids.get("MongoDB"):
        try:
            collection = mongo_db[entity]
            records = list(collection.find({"record_id": {"$in": matching_record_ids["MongoDB"]}}))
            results["MongoDB"] = [
                {"_id": str(r["_id"]), **{k: v for k, v in r.items() if k != "_id"}}
                for r in records
            ]
            print(f"[MongoDB] Fetched {len(results['MongoDB'])} full documents")
        except Exception as e:
            print(f"[MongoDB] Error in Phase 2: {e}")
            results["MongoDB"] = []
    else:
        results["MongoDB"] = []
    
    # Fetch from Unknown
    if "Unknown" in databases_needed and matching_record_ids.get("Unknown"):
        try:
            unknown_file = os.path.join(DATA_DIR, "unknown_data.json")
            if os.path.exists(unknown_file):
                with open(unknown_file, 'r') as f:
                    data = json.load(f)
                
                all_records = data if isinstance(data, list) else [data]
                results["Unknown"] = [
                    r for r in all_records if r.get("record_id") in matching_record_ids["Unknown"]
                ]
                print(f"[Unknown] Fetched {len(results['Unknown'])} full records")
        except Exception as e:
            print(f"[Unknown] Error in Phase 2: {e}")
            results["Unknown"] = []
    else:
        results["Unknown"] = []
    
    print(f"\n{'='*60}")
    print("[SUMMARY] Total records fetched:")
    print(f"  SQL: {len(results.get('SQL', []))}")
    print(f"  MongoDB: {len(results.get('MongoDB', []))}")
    print(f"  Unknown: {len(results.get('Unknown', []))}")
    print(f"{'='*60}\n")
    
    return {"operation": "READ", "entity": entity, "results": results}
    
def create_operation(parsed_query, db_analysis):
    """
    Create a new record using metadata routing:
    Phase 1: Route fields based on metadata decisions
    Phase 2: Insert into appropriate databases
    """
    
    print(f"\n{'='*60}")
    print("[CREATE OPERATION - MULTI-DATABASE ROUTING]")
    print(f"{'='*60}")
    
    entity = parsed_query.get("entity")
    payload = parsed_query.get("payload", {})
    
    print(f"\n[DEBUG] Entity: {entity}, Payload Size: {len(payload)} fields")
    
    # Fetch the next sequential record_id from the global counter
    record_id = 0
    if os.path.exists(COUNTER_FILE):
        try:
            with open(COUNTER_FILE, 'r') as f:
                record_id = int(f.read().strip() or 0)
        except Exception as e:
            print(f"[WARNING] Could not read counter file: {e}")

    payload["record_id"] = record_id
    print(f"[INFO] Assigned sequential record_id: {record_id}")

    # Increment and save the counter for the next operation
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
    
    try:
        with open(METADATA_FILE, 'r') as f:
            metadata = json.load(f)
        fields = metadata.get("fields", [])
        field_map = {f.get("field_name"): f.get("decision") for f in fields}
        strategy_map = determineMongoStrategy(fields)
    except Exception as e:
        print(f"[WARNING] Could not load metadata, falling back to Unknown: {e}")
        field_map = {}
        strategy_map = {}

    for key, value in payload.items():
        if key == "record_id":
            continue
            
        decision = field_map.get(key, "UNKNOWN")
        
        if decision in ["SQL", "BOTH"]:
            sql_payload[key] = value
        if decision in ["MONGO", "BOTH"]:
            mongo_payload[key] = value
        if decision == "UNKNOWN":
            unknown_payload[key] = value
            
    print(f"[Routing] SQL: {len(sql_payload)-1} fields, MongoDB: {len(mongo_payload)-1} fields, Unknown: {len(unknown_payload)-1} fields")

    # ============================================================================
    # PHASE 2: INSERT INTO DATABASES
    # ============================================================================
    print(f"\n{'─'*60}")
    print("[PHASE 2] Executing database insertions...")
    print(f"{'─'*60}")

    results = {"record_id": record_id, "inserted_into": []}

    # Insert into SQL
    if len(sql_payload) > 1:
        if sql_available:
            try:
                sql_engine.insert_record(sql_payload)
                results["inserted_into"].append("SQL")
                print(f"[SQL] Successfully inserted record {record_id}")
            except Exception as e:
                results["sql_error"] = str(e)
                print(f"[SQL] Error in insertion: {e}")
        else:
            print(f"[SQL] Skipped (SQL Engine not available)")
    
    # Insert into MongoDB
    if len(mongo_payload) > 1:
        try:
            processed_mongo_record = processNode(mongo_payload, "", mongo_db, strategy_map)
            mongo_db[entity].insert_one(processed_mongo_record)
            results["inserted_into"].append("MongoDB")
            print(f"[MongoDB] Successfully inserted document into '{entity}' collection")
        except Exception as e:
            results["mongo_error"] = str(e)
            print(f"[MongoDB] Error in insertion: {e}")

    # Insert into Unknown buffer
    if len(unknown_payload) > 1:
        try:
            unknown_file = os.path.join(DATA_DIR, "unknown_data.json")
            existing_data = []
            
            if os.path.exists(unknown_file):
                with open(unknown_file, 'r') as f:
                    try:
                        existing_data = json.load(f)
                    except json.JSONDecodeError:
                        pass
            
            if not isinstance(existing_data, list):
                existing_data = [existing_data] if existing_data else []
                
            existing_data.append(unknown_payload)
            
            with open(unknown_file, 'w') as f:
                json.dump(existing_data, f, indent=2)
                
            results["inserted_into"].append("Unknown")
            print(f"[Unknown] Successfully appended to unknown_data.json")
        except Exception as e:
            results["unknown_error"] = str(e)
            print(f"[Unknown] Error in insertion: {e}")

    print(f"\n{'='*60}")
    print("[SUMMARY] Create Operation Completed")
    print(f"  Inserted into: {', '.join(results['inserted_into']) if results['inserted_into'] else 'None'}")
    print(f"{'='*60}\n")

    return {
        "operation": "CREATE",
        "entity": entity,
        "status": "success" if results["inserted_into"] else "failed",
        "details": results
    }
    
def update_operation(parsed_query, db_analysis):
    """Update records."""
    print(f"\n{'='*60}")
    print("[UPDATE OPERATION]")
    print(f"{'='*60}")
    return {"operation": "UPDATE", "status": "not_implemented"} 
    
def delete_operation(parsed_query, db_analysis):
    """Delete records."""
    print(f"\n{'='*60}")
    print("[DELETE OPERATION]")
    print(f"{'='*60}")
    return {"operation": "DELETE", "status": "not_implemented"}
    