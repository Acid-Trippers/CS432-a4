import json
from .config import DATA_DIR
from .sql_engine import SQLEngine
from pymongo import MongoClient
import os

sql_engine = SQLEngine()
sql_engine.initialize()

mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
db_name = os.getenv("MONGO_DB_NAME", "cs432_db")
mongo_client = MongoClient(mongo_uri)
mongo_db = mongo_client[db_name]

def read_operation(parsed_query, db_analysis):
    """Read records matching filters from all databases."""
    print(f"\n{'='*60}")
    print("[READ OPERATION]")
    print(f"{'='*60}")
    
    entity = parsed_query.get("entity")
    filters = parsed_query.get("filters", {})  # Default to empty dict if no filters
    databases_needed = db_analysis.get("databases_needed", [])
    field_locations = db_analysis.get("field_locations", {})
    
    print(f"\n[DEBUG] Entity: {entity}, Filters: {filters}, Databases: {databases_needed}")
    
    results = {}
    
    # READ from SQL
    if "SQL" in databases_needed:
        try:
            Model = sql_engine.models.get(entity)
            print(f"[DEBUG] Model found: {Model is not None}")
            
            if Model:
                query = sql_engine.session.query(Model)
                print(f"[DEBUG] Initial query created")
                
                # Apply filters if any exist
                if filters:
                    for field_name, field_value in filters.items():
                        if field_locations.get(field_name) == "SQL":
                            query = query.filter(getattr(Model, field_name) == field_value)
                            print(f"[DEBUG] Applied filter: {field_name} = {field_value}")
                else:
                    print(f"[DEBUG] No filters - reading all records")
                
                records = query.all()
                print(f"[DEBUG] Query executed, found {len(records)} records")
                
                from sqlalchemy import inspect
                results["SQL"] = [
                    {col.name: getattr(record, col.name) for col in inspect(Model).columns}
                    for record in records
                ]
                print(f"[SQL] Found {len(results['SQL'])} records")
        except Exception as e:
            print(f"[SQL] Error: {e}")
            import traceback
            traceback.print_exc()
            results["SQL"] = []
            
    # READ from MongoDB
    if "MongoDB" in databases_needed:
        try:
            collection = mongo_db[entity]
            mongo_filters = {k: v for k, v in filters.items() if field_locations.get(k) == "MongoDB"}
            records = list(collection.find(mongo_filters))
            results["MongoDB"] = [{"_id": str(r["_id"]), **{k: v for k, v in r.items() if k != "_id"}} for r in records]
            print(f"[MongoDB] Found {len(results['MongoDB'])} documents")
        except Exception as e:
            print(f"[MongoDB] Error: {e}")
            results["MongoDB"] = []
            
    # READ from Unknown
    if "Unknown" in databases_needed:
        try:
            unknown_file = os.path.join(DATA_DIR, "unknown_data.json")
            if os.path.exists(unknown_file):
                with open(unknown_file, 'r') as f:
                    data = json.load(f)
                
                unknown_filters = {k: v for k, v in filters.items() if field_locations.get(k) == "Unknown"}
                if isinstance(data, list):
                    if unknown_filters:
                        results["Unknown"] = [r for r in data if all(r.get(k) == v for k, v in unknown_filters.items())]
                    else:
                        results["Unknown"] = data  # Return all if no filters
                else:
                    if unknown_filters:
                        results["Unknown"] = [data] if all(data.get(k) == v for k, v in unknown_filters.items()) else []
                    else:
                        results["Unknown"] = [data]
                
                print(f"[Unknown] Found {len(results['Unknown'])} records")
        except Exception as e:
            print(f"[Unknown] Error: {e}")
            results["Unknown"] = []
    
    return {"operation": "READ", "entity": entity, "results": results}
    
def create_operation(parsed_query, db_analysis):
    """Create new records."""
    print(f"\n{'='*60}")
    print("[CREATE OPERATION]")
    print(f"{'='*60}")
    return {"operation": "CREATE", "status": "not_implemented"}
    
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
    