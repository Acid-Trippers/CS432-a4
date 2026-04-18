"""
Session data exploration endpoints.

Provides APIs for:
1. Listing logical entities within a session
2. Viewing instances of each entity
3. Inspecting field names and values
4. Displaying results of executed logical queries
5. Viewing query execution history
"""

import json
import os
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response

from dashboard.dependencies import get_session_id, get_session_manager, get_mongo_db, get_sql_engine
from src.config import METADATA_FILE
from src.phase_6.CRUD_runner import query_runner


router = APIRouter(prefix="/api/sessions")


def _attach_session_header(response: Response, session_id: str) -> None:
    response.headers["X-Session-ID"] = session_id


def _validate_session_access(request: Request, session_id: str, target_session_id: str) -> bool:
    """
    Check if user/admin can access the target session.
    For now, we enforce: session must own the target or be admin.
    
    Admin token check can be added here if needed.
    """
    # Simple check: user can access their own session
    return session_id == target_session_id


def _load_metadata() -> dict[str, Any]:
    """Load metadata.json to get field routing information."""
    try:
        if not os.path.exists(METADATA_FILE):
            return {}
        with open(METADATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


# ============================================================================
# Feature 2 Helper: Query instances from SQL/MongoDB with pagination
# ============================================================================

def _query_instances_from_sql(sql_engine, entity_name: str, limit: int, offset: int) -> List[Dict[str, Any]]:
    """Query instances from SQL database."""
    try:
        if not sql_engine or not hasattr(sql_engine, 'models'):
            print(f"[SQL Query Error] SQL engine not available or no models attribute")
            return []
        
        Model = sql_engine.models.get(entity_name)
        if not Model:
            print(f"[SQL Query Error] Model not found for entity: {entity_name}")
            print(f"[SQL Query Error] Available models: {list(sql_engine.models.keys())}")
            return []
        
        records = (
            sql_engine.session.query(Model)
            .offset(offset)
            .limit(limit)
            .all()
        )
        
        print(f"[SQL Query] Found {len(records)} records for {entity_name}")
        
        from sqlalchemy import inspect as sql_inspect
        result = []
        for record in records:
            row_dict = {}
            for col in sql_inspect(Model).columns:
                val = getattr(record, col.name)
                row_dict[col.name] = val
            result.append(row_dict)
        
        return result
    except Exception as e:
        print(f"[SQL Query Error]: {e}")
        import traceback
        traceback.print_exc()
        return []


def _resolve_mongo_collection_name(mongo_db, entity_name: str) -> str:
    """
    Resolve the actual MongoDB collection name for an entity.
    Tries exact match first, then several fuzzy fallbacks.
    """
    try:
        all_collections = mongo_db.list_collection_names()
        print(f"[Mongo Resolve] Available collections: {all_collections}")
    except Exception as e:
        print(f"[Mongo Resolve] Could not list collections: {e}")
        return entity_name

    if entity_name in all_collections:
        return entity_name

    entity_lower = entity_name.lower()

    # Case-insensitive exact
    for col in all_collections:
        if col.lower() == entity_lower:
            print(f"[Mongo Resolve] Case-insensitive match: '{col}'")
            return col

    # Normalised (strip _ and -)
    entity_norm = entity_lower.replace("_", "").replace("-", "")
    for col in all_collections:
        if col.lower().replace("_", "").replace("-", "") == entity_norm:
            print(f"[Mongo Resolve] Normalised match: '{col}'")
            return col

    # Suffix match
    for col in all_collections:
        col_lower = col.lower()
        if col_lower.endswith(entity_lower) or col_lower.endswith(entity_norm):
            print(f"[Mongo Resolve] Suffix match: '{col}'")
            return col

    # Substring match
    for col in all_collections:
        if entity_lower in col.lower():
            print(f"[Mongo Resolve] Substring match: '{col}'")
            return col

    # Last resort: return first non-system collection if only one exists
    user_collections = [c for c in all_collections if not c.startswith("system.")]
    if len(user_collections) == 1:
        print(f"[Mongo Resolve] Single collection fallback: '{user_collections[0]}'")
        return user_collections[0]

    print(f"[Mongo Resolve] No match found for '{entity_name}' among {all_collections}")
    return entity_name


def _query_instances_from_mongo(mongo_db, entity_name: str, limit: int, offset: int) -> List[Dict[str, Any]]:
    """Query instances from MongoDB."""
    try:
        if mongo_db is None:
            print(f"[Mongo Query Error] MongoDB not available")
            return []

        resolved_name = _resolve_mongo_collection_name(mongo_db, entity_name)
        collection = mongo_db[resolved_name]
        doc_count = collection.count_documents({})
        print(f"[Mongo Query] Collection '{resolved_name}' has {doc_count} documents")

        if doc_count == 0:
            print(f"[Mongo Query] No documents found in collection '{resolved_name}'")
            return []

        docs = list(collection.find().skip(offset).limit(limit))
        print(f"[Mongo Query] Found {len(docs)} documents (offset={offset}, limit={limit})")

        from bson import ObjectId
        def _serialize(obj):
            if isinstance(obj, ObjectId):
                return str(obj)
            if isinstance(obj, dict):
                return {k: _serialize(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [_serialize(i) for i in obj]
            return obj

        docs = [_serialize(doc) for doc in docs]
        return docs
    except Exception as e:
        print(f"[Mongo Query Error]: {e}")
        import traceback
        traceback.print_exc()
        return []


def _get_collection_count(sql_engine, mongo_db, entity_name: str, from_source: str) -> int:
    """Get count of records in a collection."""
    try:
        sql_count = 0
        mongo_count = 0

        if from_source in ("SQL", "BOTH"):
            try:
                if sql_engine is not None and hasattr(sql_engine, 'models'):
                    Model = sql_engine.models.get(entity_name)
                    if Model:
                        sql_count = sql_engine.session.query(Model).count()
            except Exception:
                sql_count = 0

        if from_source in ("MONGO", "BOTH"):
            try:
                if mongo_db is not None:
                    resolved_name = _resolve_mongo_collection_name(mongo_db, entity_name)
                    mongo_count = mongo_db[resolved_name].count_documents({})
            except Exception:
                mongo_count = 0

        if from_source == "SQL":
            return sql_count
        if from_source == "MONGO":
            return mongo_count
        return max(sql_count, mongo_count)
    except Exception:
        return 0


# ============================================================================
# Feature 3 Helper: Extract sample values from data
# ============================================================================

def _get_sample_values(sql_engine, mongo_db, metadata_fields: List[Dict], limit: int = 10) -> Dict[str, List[Any]]:
    """Extract sample values for each field from the data."""
    samples = {}
    
    try:
        # For now, we'll get basic samples from metadata if available
        for field in metadata_fields:
            field_name = field.get("field_name")
            if not field_name:
                continue
            
            routing = field.get("decision", "UNKNOWN")
            
            # Initialize sample list
            if field_name not in samples:
                samples[field_name] = []
            
            # Try to get samples based on routing
            if routing == "SQL" and sql_engine:
                # For SQL fields, we'd need to query the actual table
                # For now, we'll leave as empty
                pass
            elif routing == "MONGO" and mongo_db:
                # For Mongo fields, we'd need to query the actual collection
                # For now, we'll leave as empty
                pass
    except Exception as e:
        print(f"[Sample extraction error]: {e}")
    
    return samples


# ============================================================================
# Feature 1: List Logical Entities Within a Session
# ============================================================================

@router.get("/{session_id}/entities")
async def list_entities(
    session_id: str,
    request: Request,
    response: Response,
    current_session_id: str = Depends(get_session_id),
):
    """
    List all logical entities (tables/collections) that have been queried in this session.
    
    Returns:
    - entity_name: Name of the entity
    - query_count: Number of queries executed on this entity
    - operations: List of operations performed (READ, CREATE, UPDATE, DELETE)
    - last_queried: ISO timestamp of the last query on this entity
    """
    if not _validate_session_access(request, current_session_id, session_id):
        raise HTTPException(status_code=403, detail="unauthorized access to session")

    session_manager = get_session_manager(request)
    entities = session_manager.get_entities_in_session(session_id)

    return {
        "session_id": session_id,
        "entity_count": len(entities),
        "entities": entities,
    }


# ============================================================================
# Feature 5: Viewing Query Execution History
# ============================================================================

@router.get("/{session_id}/query-history")
async def get_query_history(
    session_id: str,
    request: Request,
    response: Response,
    current_session_id: str = Depends(get_session_id),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    operation: str | None = Query(None),
    entity: str | None = Query(None),
    status: str | None = Query(None),
):
    """
    Get paginated query execution history from a session.
    
    Query Parameters:
    - limit: Number of queries to return (1-200, default 50)
    - offset: Pagination offset
    - operation: Filter by operation type (READ, CREATE, UPDATE, DELETE)
    - entity: Filter by entity name
    - status: Filter by status (success, failed)
    
    Returns: List of queries with execution metadata
    """
    if not _validate_session_access(request, current_session_id, session_id):
        raise HTTPException(status_code=403, detail="unauthorized access to session")

    session_manager = get_session_manager(request)
    history = session_manager.get_query_history_paginated(
        session_id=session_id,
        limit=limit,
        offset=offset,
        operation_filter=operation,
        entity_filter=entity,
        status_filter=status,
    )

    return {
        "session_id": session_id,
        **history,
    }


# ============================================================================
# Feature 4: Displaying Results of Executed Logical Queries
# ============================================================================

@router.get("/{session_id}/query-results/{query_id}")
async def get_query_result(
    session_id: str,
    query_id: str,
    request: Request,
    response: Response,
    current_session_id: str = Depends(get_session_id),
    force_reexecute: bool = Query(False),
):
    """
    Get the result of a specific executed query.
    
    Query Parameters:
    - force_reexecute: If true, re-execute the query instead of returning cached result
    
    Returns: Query execution result with data (for READ operations)
    """
    if not _validate_session_access(request, current_session_id, session_id):
        raise HTTPException(status_code=403, detail="unauthorized access to session")

    session_manager = get_session_manager(request)
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="session not found")

    query_history = session.get("query_history", [])
    if not isinstance(query_history, list):
        raise HTTPException(status_code=404, detail="query not found")

    # Find the query entry
    query_entry = None
    for entry in query_history:
        if isinstance(entry, dict) and entry.get("query_id") == query_id:
            query_entry = entry
            break

    if not query_entry:
        raise HTTPException(status_code=404, detail="query not found")

    payload = query_entry.get("payload", {})
    result = query_entry.get("result", {})

    if not force_reexecute:
        # Return cached result
        return {
            "query_id": query_id,
            "session_id": session_id,
            "timestamp": query_entry.get("at"),
            "status": query_entry.get("status"),
            "payload": payload,
            "result": result,
        }

    # Re-execute the query
    if not isinstance(payload, dict) or "operation" not in payload:
        raise HTTPException(status_code=400, detail="invalid query payload")

    try:
        # Re-execute the query using query_runner
        session_manager = get_session_manager(request)
        new_result = query_runner(
            query_dict=payload,
            session_id=session_id,
            session_manager=session_manager,
        )

        return {
            "query_id": query_id,
            "session_id": session_id,
            "timestamp": query_entry.get("at"),
            "status": new_result.get("status", "success"),
            "payload": payload,
            "result": new_result,
            "reexecuted": True,
        }
    except Exception as e:
        print(f"[Query re-execution error]: {e}")
        # If re-execution fails, return cached result with error flag
        return {
            "query_id": query_id,
            "session_id": session_id,
            "timestamp": query_entry.get("at"),
            "status": query_entry.get("status"),
            "payload": payload,
            "result": result,
            "reexecuted": False,
            "reexecution_error": str(e),
        }


# ============================================================================
# Feature 3: Inspecting Field Names and Values of Logical Objects
# ============================================================================

@router.get("/{session_id}/entities/{entity_name}/schema")
async def get_entity_schema(
    session_id: str,
    entity_name: str,
    request: Request,
    response: Response,
    current_session_id: str = Depends(get_session_id),
    include_samples: bool = Query(True),
    include_stats: bool = Query(True),
):
    """
    Get field definitions and metadata for an entity.
    
    Query Parameters:
    - include_samples: Include sample values from actual data
    - include_stats: Include frequency and cardinality statistics
    
    Returns: Field definitions with optional sample values and stats
    """
    if not _validate_session_access(request, current_session_id, session_id):
        raise HTTPException(status_code=403, detail="unauthorized access to session")

    session_manager = get_session_manager(request)
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="session not found")

    # Load metadata
    metadata = _load_metadata()
    fields_metadata = metadata.get("fields", [])

    if not fields_metadata:
        raise HTTPException(status_code=500, detail="metadata not available")

    # Build field response
    response_fields = []
    for field in fields_metadata:
        if not isinstance(field, dict):
            continue

        field_info = {
            "field_name": field.get("field_name"),
            "dominant_type": field.get("dominant_type"),
            "routing": field.get("decision"),  # SQL, MONGO, UNKNOWN
            "is_primary_key_candidate": field.get("is_primary_key_candidate", False),
        }

        if include_stats:
            field_info["frequency"] = field.get("frequency")
            field_info["cardinality"] = field.get("cardinality")
            field_info["type_stability"] = field.get("type_stability")
            field_info["nesting_depth"] = field.get("nesting_depth", 0)

        # Include sample values if requested
        if include_samples:
            field_info["sample_values"] = []
            # Samples would be extracted from live queries here
            # For now, return empty list

        response_fields.append(field_info)

    return {
        "session_id": session_id,
        "entity": entity_name,
        "field_count": len(response_fields),
        "fields": response_fields,
    }


# ============================================================================
# Feature 2: Viewing Instances of Each Entity
# ============================================================================

@router.get("/{session_id}/entities/{entity_name}/instances")
async def get_entity_instances(
    session_id: str,
    entity_name: str,
    request: Request,
    response: Response,
    current_session_id: str = Depends(get_session_id),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    from_source: str = Query("BOTH", pattern="^(SQL|MONGO|BOTH)$"),
):
    """
    Get paginated list of instances (records/documents) from an entity.
    
    Query Parameters:
    - limit: Number of records to return (1-100, default 20)
    - offset: Pagination offset
    - from_source: Query source - SQL, MONGO, or BOTH (default BOTH)
    
    Returns: Paginated list of entity instances
    """
    if not _validate_session_access(request, current_session_id, session_id):
        raise HTTPException(status_code=403, detail="unauthorized access to session")

    session_manager = get_session_manager(request)
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="session not found")

    # Query instances from requested sources
    instances = []
    total_count = 0

    try:
        sql_engine = None
        mongo_db = None

        if from_source in ("SQL", "BOTH"):
            sql_engine = get_sql_engine(request)
            sql_instances = _query_instances_from_sql(sql_engine, entity_name, limit, offset)
            instances = sql_instances
            print(f"[Instance Query] Got {len(instances)} from SQL")

        if from_source in ("MONGO", "BOTH"):
            mongo_db = get_mongo_db(request)
            mongo_instances = _query_instances_from_mongo(mongo_db, entity_name, limit, offset)
            print(f"[Instance Query] Got {len(mongo_instances)} from MONGO")

            if from_source == "MONGO":
                instances = mongo_instances

            elif from_source == "BOTH":
                if not mongo_instances:
                    # Nothing in Mongo, keep SQL rows as-is
                    pass
                elif not instances:
                    # Nothing in SQL, use Mongo rows as-is
                    instances = mongo_instances
                else:
                    # Merge: join SQL rows and Mongo docs by record_id.
                    # Build a lookup of Mongo docs keyed by every plausible id field.
                    mongo_by_id: dict = {}
                    for doc in mongo_instances:
                        for key in ("record_id", "id", "_id"):
                            val = doc.get(key)
                            if val is not None:
                                # Normalise to int when possible so "1" == 1
                                try:
                                    mongo_by_id[int(val)] = doc
                                except (ValueError, TypeError):
                                    pass
                                mongo_by_id[str(val)] = doc
                                break

                    merged = []
                    for row in instances:
                        combined = dict(row)
                        # Find matching Mongo doc
                        mongo_doc = None
                        for key in ("record_id", "id"):
                            val = row.get(key)
                            if val is not None:
                                mongo_doc = mongo_by_id.get(int(val) if str(val).isdigit() else str(val))
                                if mongo_doc:
                                    break
                        if mongo_doc:
                            # Merge Mongo fields in, skip _id to avoid confusion
                            for k, v in mongo_doc.items():
                                if k != "_id" and k not in combined:
                                    combined[k] = v
                        merged.append(combined)
                    instances = merged
                    print(f"[Instance Query] Merged into {len(instances)} combined records")

        # Get total count
        total_count = _get_collection_count(sql_engine, mongo_db, entity_name, from_source)
        print(f"[Instance Query] Total count: {total_count}, instances to return: {len(instances)}")
        instances = [{k: v for k, v in row.items() if k not in ("record_id", "_id")} for row in instances]

    except Exception as e:
        print(f"[Error querying instances]: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"error querying instances: {str(e)}")
    
    return {
        "session_id": session_id,
        "entity": entity_name,
        "source": from_source,
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "instances": instances,
    }