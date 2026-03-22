import json
import os
import time
from src.config import DATA_DIR, METADATA_FILE, COUNTER_FILE
from src.phase_5.sql_engine import SQLEngine
from src.phase_5.mongo_engine import determineMongoStrategy, processNode
from pymongo import MongoClient

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
    """
    Update records using a three-phase approach:
    Phase 1: Route payload fields based on metadata
    Phase 2: Find all matching record_ids based on filters
    Phase 3: Apply the updates to the respective databases
    """
    
    print(f"\n{'='*60}")
    print("[UPDATE OPERATION - HYBRID ROUTING & EXECUTION]")
    print(f"{'='*60}")
    
    entity = parsed_query.get("entity")
    filters = parsed_query.get("filters", {})
    payload = parsed_query.get("payload", {})
    databases_needed = db_analysis.get("databases_needed", [])
    field_locations = db_analysis.get("field_locations", {})
    
    if not payload:
        print("[WARNING] No payload provided for update.")
        return {"operation": "UPDATE", "status": "failed", "reason": "empty payload"}
        
    print(f"\n[DEBUG] Entity: {entity}, Filters: {filters}")
    print(f"[DEBUG] Payload Size: {len(payload)} fields")

    # ============================================================================
    # PHASE 1: ROUTE PAYLOAD FIELDS
    # ============================================================================
    print(f"\n{'─'*60}")
    print("[PHASE 1] Routing update payload fields...")
    print(f"{'─'*60}")

    sql_payload = {}
    mongo_payload = {}
    unknown_payload = {}
    
    try:
        with open(METADATA_FILE, 'r') as f:
            metadata = json.load(f)
        fields = metadata.get("fields", [])
        field_map = {f.get("field_name"): f.get("decision") for f in fields}
        strategy_map = determineMongoStrategy(fields)
    except Exception as e:
        print(f"[WARNING] Could not load metadata: {e}")
        field_map = {}
        strategy_map = {}

    for key, value in payload.items():
        if key == "record_id":
            continue  # Do not allow updating the primary key
            
        decision = field_map.get(key, "UNKNOWN")
        
        if decision in ["SQL", "BOTH"]:
            sql_payload[key] = value
        if decision in ["MONGO", "BOTH"]:
            mongo_payload[key] = value
        if decision == "UNKNOWN":
            unknown_payload[key] = value
            
    print(f"[Routing] SQL: {len(sql_payload)} fields, MongoDB: {len(mongo_payload)} fields, Unknown: {len(unknown_payload)} fields")

    # ============================================================================
    # PHASE 2: FIND record_ids matching filters
    # ============================================================================
    print(f"\n{'─'*60}")
    print("[PHASE 2] Finding matching record_ids to update...")
    print(f"{'─'*60}")
    
    matching_record_ids = set()
    
    # Query SQL
    if "SQL" in databases_needed and sql_available:
        try:
            Model = sql_engine.models.get(entity)
            if Model:
                query = sql_engine.session.query(Model.record_id)
                sql_filters = {k: v for k, v in filters.items() if field_locations.get(k) == "SQL"}
                for field_name, field_value in sql_filters.items():
                    query = query.filter(getattr(Model, field_name) == field_value)
                
                record_ids = [rid[0] for rid in query.all()]
                matching_record_ids.update(record_ids)
                print(f"[SQL] Found {len(record_ids)} matching record_ids")
        except Exception as e:
            print(f"[SQL] Error finding records: {e}")

    # Query MongoDB
    if "MongoDB" in databases_needed:
        try:
            collection = mongo_db[entity]
            mongo_filters = {k: v for k, v in filters.items() if field_locations.get(k) == "MongoDB"}
            docs = list(collection.find(mongo_filters, {"record_id": 1}))
            record_ids = [doc.get("record_id") for doc in docs if "record_id" in doc]
            matching_record_ids.update(record_ids)
            print(f"[MongoDB] Found {len(record_ids)} matching record_ids")
        except Exception as e:
            print(f"[MongoDB] Error finding records: {e}")

    # Query Unknown
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
                matching_record_ids.update(record_ids)
                print(f"[Unknown] Found {len(record_ids)} matching record_ids")
        except Exception as e:
            print(f"[Unknown] Error finding records: {e}")

    # If filters were empty, we might not have collected IDs, but UPDATE usually requires filters.
    if not matching_record_ids and filters:
        print("[INFO] No matching records found to update.")
        return {"operation": "UPDATE", "status": "success", "updated_count": 0}

    record_ids_list = list(matching_record_ids)

    # ============================================================================
    # PHASE 3: APPLY UPDATES
    # ============================================================================
    print(f"\n{'─'*60}")
    print(f"[PHASE 3] Applying updates to {len(record_ids_list)} records...")
    print(f"{'─'*60}")
    
    update_summary = {}

    # Update SQL
    if sql_payload and sql_available and record_ids_list:
        try:
            Model = sql_engine.models.get(entity)
            if Model:
                update_query = sql_engine.session.query(Model).filter(
                    Model.record_id.in_(record_ids_list)
                )
                
                # SQLAlchemy bulk update
                count = update_query.update(sql_payload, synchronize_session=False)
                sql_engine.session.commit()
                update_summary["SQL"] = count
                print(f"[SQL] Updated {count} records")
        except Exception as e:
            print(f"[SQL] Error applying update: {e}")
            sql_engine.session.rollback()
            update_summary["SQL"] = 0

    # Update MongoDB
    if mongo_payload and record_ids_list:
        try:
            collection = mongo_db[entity]
            # Process the payload to handle any necessary embedding/referencing structures
            processed_mongo_payload = processNode(mongo_payload, "", mongo_db, strategy_map)
            
            result = collection.update_many(
                {"record_id": {"$in": record_ids_list}},
                {"$set": processed_mongo_payload}
            )
            update_summary["MongoDB"] = result.modified_count
            print(f"[MongoDB] Updated {result.modified_count} documents")
        except Exception as e:
            print(f"[MongoDB] Error applying update: {e}")
            update_summary["MongoDB"] = 0

    # Update Unknown
    if unknown_payload and record_ids_list:
        try:
            unknown_file = os.path.join(DATA_DIR, "unknown_data.json")
            if os.path.exists(unknown_file):
                with open(unknown_file, 'r') as f:
                    data = json.load(f)
                
                all_records = data if isinstance(data, list) else [data]
                update_count = 0
                
                for r in all_records:
                    if r.get("record_id") in record_ids_list:
                        r.update(unknown_payload)
                        update_count += 1
                        
                with open(unknown_file, 'w') as f:
                    json.dump(all_records, f, indent=2)
                    
                update_summary["Unknown"] = update_count
                print(f"[Unknown] Updated {update_count} records")
        except Exception as e:
            print(f"[Unknown] Error applying update: {e}")
            update_summary["Unknown"] = 0

    print(f"\n{'='*60}")
    print("[SUMMARY] Update Operation Completed")
    print(f"  SQL: {update_summary.get('SQL', 0)} updated")
    print(f"  MongoDB: {update_summary.get('MongoDB', 0)} updated")
    print(f"  Unknown: {update_summary.get('Unknown', 0)} updated")
    print(f"{'='*60}\n")

    return {
        "operation": "UPDATE",
        "entity": entity,
        "status": "success",
        "matched_records": len(record_ids_list),
        "updates_applied": update_summary
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
    
    # If no filters, delete ALL records (user explicitly stated they want this)
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
    # PHASE 2: DELETE records using record_ids
    # ============================================================================
    print(f"\n{'─'*60}")
    print("[PHASE 2] Deleting records by record_id...")
    print(f"{'─'*60}")
    
    deleted_summary = {}
    total_deleted = 0
    
    # Delete from SQL
    if "SQL" in databases_needed and sql_available and matching_record_ids.get("SQL"):
        try:
            Model = sql_engine.models.get(entity)
            if Model:
                delete_query = sql_engine.session.query(Model).filter(
                    Model.record_id.in_(matching_record_ids["SQL"])
                )
                count = delete_query.count()
                delete_query.delete()
                sql_engine.session.commit()
                deleted_summary["SQL"] = count
                total_deleted += count
                print(f"[SQL] Deleted {count} records")
        except Exception as e:
            print(f"[SQL] Error in Phase 2: {e}")
            sql_engine.session.rollback()
            deleted_summary["SQL"] = 0
    else:
        deleted_summary["SQL"] = 0
    
    # Delete from MongoDB
    if "MongoDB" in databases_needed and matching_record_ids.get("MongoDB"):
        try:
            collection = mongo_db[entity]
            result = collection.delete_many({"record_id": {"$in": matching_record_ids["MongoDB"]}})
            deleted_summary["MongoDB"] = result.deleted_count
            total_deleted += result.deleted_count
            print(f"[MongoDB] Deleted {result.deleted_count} documents")
        except Exception as e:
            print(f"[MongoDB] Error in Phase 2: {e}")
            deleted_summary["MongoDB"] = 0
    else:
        deleted_summary["MongoDB"] = 0
    
    # Delete from Unknown
    if "Unknown" in databases_needed and matching_record_ids.get("Unknown"):
        try:
            unknown_file = os.path.join(DATA_DIR, "unknown_data.json")
            if os.path.exists(unknown_file):
                with open(unknown_file, 'r') as f:
                    data = json.load(f)
                
                all_records = data if isinstance(data, list) else [data]
                
                # Filter out records with matching record_ids
                remaining_records = [
                    r for r in all_records 
                    if r.get("record_id") not in matching_record_ids["Unknown"]
                ]
                
                # Write back the remaining records
                with open(unknown_file, 'w') as f:
                    json.dump(remaining_records, f, indent=2)
                
                deleted_count = len(all_records) - len(remaining_records)
                deleted_summary["Unknown"] = deleted_count
                total_deleted += deleted_count
                print(f"[Unknown] Deleted {deleted_count} records")
        except Exception as e:
            print(f"[Unknown] Error in Phase 2: {e}")
            deleted_summary["Unknown"] = 0
    else:
        deleted_summary["Unknown"] = 0
    
    print(f"\n{'='*60}")
    print("[SUMMARY] Total records deleted:")
    print(f"  SQL: {deleted_summary.get('SQL', 0)}")
    print(f"  MongoDB: {deleted_summary.get('MongoDB', 0)}")
    print(f"  Unknown: {deleted_summary.get('Unknown', 0)}")
    print(f"  TOTAL: {total_deleted}")
    print(f"{'='*60}\n")
    
    return {
        "operation": "DELETE",
        "status": "success",
        "entity": entity,
        "filters_applied": filters,
        "deleted_by_database": deleted_summary,
        "total_deleted": total_deleted
    }
    