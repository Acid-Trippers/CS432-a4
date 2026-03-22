import json
import os
import time
from src.config import DATA_DIR, METADATA_FILE, COUNTER_FILE, MONGO_URI, MONGO_DB_NAME
from src.phase_5.sql_engine import SQLEngine
from src.phase_5.mongo_engine import determineMongoStrategy, processNode
from pymongo import MongoClient, errors

# Initialize SQL Engine gracefully (non-blocking if PostgreSQL unavailable)
sql_engine = SQLEngine()
sql_available = False
try:
    sql_engine.initialize()
    sql_available = True
    print("[SQL] Engine initialized successfully")
except Exception as e:
    print(f"[WARNING] SQL Engine initialization failed: {str(e)[:100]}...")
    print("[WARNING] Continuing with MongoDB and Unknown data sources only")

# Initialize MongoDB connection with authentication
mongo_db = None
mongo_available = False
try:
    print(f"[MONGO] Connecting to {MONGO_URI.replace(MONGO_URI.split('@')[0].split('//')[1], '***')}...")
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    # Test the connection by pinging
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
    
    # Process SQL results
    for record in results_by_db.get("SQL", []):
        record_id = record.get("record_id")
        if record_id is not None:
            if record_id not in merged:
                merged[record_id] = {}
            merged[record_id].update(record)
            merged[record_id]["_source"] = merged[record_id].get("_source", [])
            if "SQL" not in merged[record_id]["_source"]:
                merged[record_id]["_source"].append("SQL")
    
    # Process MONGO results
    for record in results_by_db.get("MONGO", []):
        record_id = record.get("record_id") or record.get("_id")
        if record_id is not None:
            if record_id not in merged:
                merged[record_id] = {}
            # For MONGO, convert ObjectId to string if needed
            mongo_record = {k: str(v) if k == "_id" else v for k, v in record.items()}
            merged[record_id].update(mongo_record)
            merged[record_id]["_source"] = merged[record_id].get("_source", [])
            if "MONGO" not in merged[record_id]["_source"]:
                merged[record_id]["_source"].append("MONGO")
    
    # Process Unknown results
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
    
    # Query MONGO for matching record_ids in UPDATE
    if "MONGO" in databases_needed and mongo_available:
        try:
            collection = mongo_db[entity]
            mongo_filters = {k: v for k, v in filters.items() if field_locations.get(k) == "MONGO"}
            
            # Important: In MONGO, record_id is stored as _id
            projection = {"_id": 1}
            docs = list(collection.find(mongo_filters, projection))
            record_ids = [doc.get("_id") for doc in docs if "_id" in doc]
            matching_record_ids["MONGO"] = record_ids
            print(f"[MONGO] Found {len(record_ids)} matching record_ids: {record_ids[:5]}{'...' if len(record_ids) > 5 else ''}")
        except Exception as e:
            print(f"[MONGO] Error in Phase 1: {e}")
            matching_record_ids["MONGO"] = []
    elif "MONGO" in databases_needed:
        print(f"[MONGO] Skipped (MongoDB not available)")
        matching_record_ids["MONGO"] = []
    
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
    
    # Fetch from MONGO
    if "MONGO" in databases_needed and matching_record_ids.get("MONGO") and mongo_available:
        try:
            collection = mongo_db[entity]
            # Important: Use _id field for MONGO queries since record_id is stored as _id
            records = list(collection.find({"_id": {"$in": matching_record_ids["MONGO"]}}))
            results["MONGO"] = [
                {"record_id": r["_id"], **{k: v for k, v in r.items() if k != "_id"}}
                for r in records
            ]
            print(f"[MONGO] Fetched {len(results['MONGO'])} full documents")
        except Exception as e:
            print(f"[MONGO] Error in Phase 2: {e}")
            results["MONGO"] = []
    else:
        results["MONGO"] = []
    
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
    print(f"  MONGO: {len(results.get('MONGO', []))}") 
    # ============================================================================
    # PHASE 3: MERGE results by record_id
    # ============================================================================
    print(f"\n{'─'*60}")
    print("[PHASE 3] Merging results by record_id...")
    print(f"{'─'*60}")
    
    merged_results = merge_results_by_record_id(results)
    print(f"[MERGE] Merged {len(merged_results)} unique records")
    print(f"[MERGE] Sample keys: {list(merged_results.keys())[:5]}")
    
    return {"operation": "READ", "entity": entity, "data": merged_results}
    
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
    
    # Use field_locations already computed by CRUD_runner to avoid routing disconnect
    field_locations = db_analysis.get("field_locations", {})
    # for MongoDB, we need strategy_map from metadata
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
            
        # Use field_locations from db_analysis for consistency
        decision = field_locations.get(key, "Unknown")
        
        if decision == "SQL":
            sql_payload[key] = value
        elif decision == "MONGO":
            mongo_payload[key] = value
        else:  # "Unknown" or any other value
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
        if mongo_available:
            try:
                processed_mongo_record = processNode(mongo_payload, "", mongo_db, strategy_map)
                mongo_db[entity].insert_one(processed_mongo_record)
                results["inserted_into"].append("MONGO")
                print(f"[MONGO] Successfully inserted document into '{entity}' collection")
            except Exception as e:
                results["mongo_error"] = str(e)
                print(f"[MONGO] Error in insertion: {e}")
        else:
            print(f"[MONGO] Skipped (MongoDB not available)")

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
    # PHASE 1: FIND record_ids matching filters
    # ============================================================================
    print(f"\n{'─'*60}")
    print("[PHASE 1] Finding matching record_ids to update...")
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
    
    # Query MONGO for matching record_ids in DELETE
    if "MONGO" in databases_needed:
        try:
            collection = mongo_db[entity]
            mongo_filters = {k: v for k, v in filters.items() if field_locations.get(k) == "MONGO"}
            
            # Important: In MONGO, record_id is stored as _id
            projection = {"_id": 1}
            docs = list(collection.find(mongo_filters, projection))
            record_ids = [doc.get("_id") for doc in docs if "_id" in doc]
            matching_record_ids["MONGO"] = record_ids
            print(f"[MONGO] Found {len(record_ids)} matching record_ids: {record_ids[:5]}{'...' if len(record_ids) > 5 else ''}")
        except Exception as e:
            print(f"[MONGO] Error in Phase 1: {e}")
            matching_record_ids["MONGO"] = []
    
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
    # PHASE 2: UPDATE records using record_ids
    # ============================================================================
    print(f"\n{'─'*60}")
    print("[PHASE 2] Updating records by record_id...")
    print(f"{'─'*60}")
    
    # Use field_locations already computed by CRUD_runner to avoid routing disconnect
    field_locations = db_analysis.get("field_locations", {})
    # for MongoDB, we need strategy_map from metadata
    strategy_map = {}
    try:
        with open(METADATA_FILE, 'r') as f:
            metadata = json.load(f)
        fields = metadata.get("fields", [])
        strategy_map = determineMongoStrategy(fields)
    except Exception as e:
        print(f"[WARNING] Could not load MongoDB strategy: {e}")
    
    # Route payload fields to appropriate databases
    sql_updates = {}
    mongo_updates = {}
    unknown_updates = {}
    
    for key, value in payload.items():
        if key == "record_id":
            continue
        
        # Use field_locations from db_analysis for consistency
        decision = field_locations.get(key, "Unknown")
        
        if decision == "SQL":
            sql_updates[key] = value
        elif decision == "MONGO":
            mongo_updates[key] = value
        else:  # "Unknown" or any other value
            unknown_updates[key] = value
    
    print(f"[Routing] SQL: {len(sql_updates)} fields, MongoDB: {len(mongo_updates)} fields, Unknown: {len(unknown_updates)} fields")
    
    updated_summary = {}
    total_updated = 0
    
    # Update SQL records
    if "SQL" in databases_needed and sql_available and matching_record_ids.get("SQL") and sql_updates:
        try:
            Model = sql_engine.models.get(entity)
            if Model:
                update_query = sql_engine.session.query(Model).filter(
                    Model.record_id.in_(matching_record_ids["SQL"])
                )
                count = update_query.count()
                
                # Update each field
                for field_name, field_value in sql_updates.items():
                    try:
                        update_query.update({getattr(Model, field_name): field_value})
                    except Exception as e:
                        print(f"[SQL] Could not update field {field_name}: {e}")
                
                sql_engine.session.commit()
                updated_summary["SQL"] = count
                total_updated += count
                print(f"[SQL] Updated {count} records with {len(sql_updates)} fields")
        except Exception as e:
            print(f"[SQL] Error in Phase 2: {e}")
            sql_engine.session.rollback()
            updated_summary["SQL"] = 0
    else:
        updated_summary["SQL"] = 0
    
    # Update MONGO documents
    if "MONGO" in databases_needed and matching_record_ids.get("MONGO") and mongo_updates and mongo_available:
        try:
            collection = mongo_db[entity]
            # Important: Use _id for MongoDB queries since record_id is stored as _id
            result = collection.update_many(
                {"_id": {"$in": matching_record_ids["MONGO"]}},
                {"$set": mongo_updates}
            )
            updated_summary["MONGO"] = result.modified_count
            total_updated += result.modified_count
            print(f"[MONGO] Updated {result.modified_count} documents with {len(mongo_updates)} fields")
        except Exception as e:
            print(f"[MONGO] Error in Phase 2: {e}")
            updated_summary["MONGO"] = 0
    elif "MONGO" in databases_needed:
        if not mongo_available:
            print(f"[MONGO] Skipped (MongoDB not available)")
        updated_summary["MONGO"] = 0
    
    # Update Unknown records
    if "Unknown" in databases_needed and matching_record_ids.get("Unknown") and unknown_updates:
        try:
            unknown_file = os.path.join(DATA_DIR, "unknown_data.json")
            if os.path.exists(unknown_file):
                with open(unknown_file, 'r') as f:
                    data = json.load(f)
                
                all_records = data if isinstance(data, list) else [data]
                
                # Update matching records
                updated_count = 0
                for record in all_records:
                    if record.get("record_id") in matching_record_ids["Unknown"]:
                        for key, value in unknown_updates.items():
                            record[key] = value
                        updated_count += 1
                
                # Write back the updated records
                with open(unknown_file, 'w') as f:
                    json.dump(all_records, f, indent=2)
                
                updated_summary["Unknown"] = updated_count
                total_updated += updated_count
                print(f"[Unknown] Updated {updated_count} records with {len(unknown_updates)} fields")
        except Exception as e:
            print(f"[Unknown] Error in Phase 2: {e}")
            updated_summary["Unknown"] = 0
    else:
        updated_summary["Unknown"] = 0
    
    print(f"\n{'='*60}")
    print("[SUMMARY] Total records updated:")
    print(f"  SQL: {updated_summary.get('SQL', 0)}")
    print(f"  MONGO: {updated_summary.get('MONGO', 0)} records updated")
    print(f"  Unknown: {updated_summary.get('Unknown', 0)}")
    print(f"  TOTAL: {total_updated}")
    print(f"{'='*60}\n")
    
    return {
        "operation": "UPDATE",
        "status": "success",
        "entity": entity,
        "filters_applied": filters,
        "updated_by_database": updated_summary,
        "total_updated": total_updated
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
    
    # Query MONGO for matching record_ids
    if "MONGO" in databases_needed and mongo_available:
        try:
            collection = mongo_db[entity]
            mongo_filters = {k: v for k, v in filters.items() if field_locations.get(k) == "MONGO"}
            
            # Important: In MONGO, record_id is stored as _id
            projection = {"_id": 1}
            docs = list(collection.find(mongo_filters, projection))
            record_ids = [doc.get("_id") for doc in docs if "_id" in doc]
            matching_record_ids["MONGO"] = record_ids
            print(f"[MONGO] Found {len(record_ids)} matching record_ids: {record_ids[:5]}{'...' if len(record_ids) > 5 else ''}")
        except Exception as e:
            print(f"[MONGO] Error in Phase 1: {e}")
            matching_record_ids["MONGO"] = []
    elif "MONGO" in databases_needed:
        print(f"[MONGO] Skipped (MongoDB not available)")
        matching_record_ids["MONGO"] = []
    
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
    
    # Delete from MONGO
    if "MONGO" in databases_needed and matching_record_ids.get("MONGO") and mongo_available:
        try:
            collection = mongo_db[entity]
            # Important: Use _id for MongoDB queries since record_id is stored as _id
            result = collection.delete_many({"_id": {"$in": matching_record_ids["MONGO"]}})
            deleted_summary["MONGO"] = result.deleted_count
            total_deleted += result.deleted_count
            print(f"[MONGO] Deleted {result.deleted_count} documents")
        except Exception as e:
            print(f"[MONGO] Error in Phase 2: {e}")
            deleted_summary["MONGO"] = 0
    elif "MONGO" in databases_needed:
        print(f"[MONGO] Skipped (MongoDB not available)")
        deleted_summary["MONGO"] = 0
    
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
    print(f"  MONGO: {deleted_summary.get('MONGO', 0)} records deleted")
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
    