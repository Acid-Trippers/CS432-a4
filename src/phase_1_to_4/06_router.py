"""
Splits the globally cleaned dataset into distinct operational payloads for different database engines.

- Ingests the final SQL, MONGO, or UNKNOWN database routing decisions from the metadata playbook.
- Compiles a hierarchical map to safely allocate top-level parent keys and their entire nested structures.
- Iterates over the master cleaned dataset and horizontally shards properties according to their destination.
- Forces identified primary key candidates into both SQL and Mongo pipelines to support joins.
- Serializes the fragmented output into engine-specific datasets (`sql_data.json`, `mongo_data.json`, etc.).
"""

import json
import os
from typing import Dict, List

from src.config import CLEANED_DATA_FILE, DATA_DIR, METADATA_FILE, BUFFER_FILE
SQL_OUTPUT_FILE = os.path.join(DATA_DIR, "sql_data.json")
MONGO_OUTPUT_FILE = os.path.join(DATA_DIR, "mongo_data.json")
UNKNOWN_OUTPUT_FILE = os.path.join(DATA_DIR, "unknown_data.json")


def _read_json(file_path: str):
    with open(file_path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(file_path: str, payload) -> None:
    with open(file_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def _build_field_routes(field_metadata: List[Dict]) -> Dict[str, str]:
    routes: Dict[str, str] = {}

    for field in field_metadata:
        field_name = field.get("field_name") or field.get("fieldName")
        if not field_name:
            continue

        top_level_name = str(field_name).split(".", 1)[0].replace("[]", "")
        decision = str(field.get("decision", "UNKNOWN")).upper()
        
        # ADDITION: Check for probation/discovery flag
        is_buffer = field.get("is_discovered_buffer", False)

        if decision not in {"SQL", "MONGO", "UNKNOWN"}:
            decision = "UNKNOWN"

        # Force PK candidates or record_id to BOTH
        if field.get("is_primary_key_candidate") is True or top_level_name == "record_id":
            decision = "BOTH"
        elif is_buffer:
            decision = "BUFFER"

        if top_level_name not in routes or decision == "BOTH":
            routes[top_level_name] = decision

    return routes


def route_data() -> None:
    if not os.path.exists(METADATA_FILE) or not os.path.exists(CLEANED_DATA_FILE):
        return

    metadata = _read_json(METADATA_FILE)
    cleaned_data = _read_json(CLEANED_DATA_FILE)
    if not cleaned_data: return

    routes = _build_field_routes(metadata.get('fields', []))

    # SQL/Mongo payload files are batch-scoped: each routing pass should only
    # include newly cleaned records. This prevents reprocessing historical data
    # during fetch and keeps progress/output aligned with current batch size.
    sql_data = []
    mongo_data = []

    # Buffer remains cumulative as a long-lived quarantine log.
    if os.path.exists(BUFFER_FILE):
        try:
            buffer_data = _read_json(BUFFER_FILE)
        except Exception:
            buffer_data = []
    else:
        buffer_data = []

    for record in cleaned_data:
        sql_rec, mongo_rec, buf_rec = {}, {}, {}
        # Get the glue ID
        ref_id = record.get("record_id")

        for key, value in record.items():
            decision = routes.get(key, "UNKNOWN")
            
            if decision == "UNKNOWN":
                buffer_data.append({"record_id": ref_id, "field": key, "value": value})
            elif decision == "SQL":
                sql_rec[key] = value
            elif decision == "MONGO":
                mongo_rec[key] = value
            elif decision == "BOTH":
                sql_rec[key] = value
                mongo_rec[key] = value

        if sql_rec: sql_data.append(sql_rec)
        if mongo_rec: mongo_data.append(mongo_rec)

    # Write back the persistent shards
    _write_json(SQL_OUTPUT_FILE, sql_data)
    _write_json(MONGO_OUTPUT_FILE, mongo_data)
    _write_json(BUFFER_FILE, buffer_data)

    # FINAL FLUSH: Clear the cleaned_data.json
    _write_json(CLEANED_DATA_FILE, [])
    stats = {
        "sql": len(sql_data),
        "mongo": len(mongo_data),
        "buffer": len(buffer_data)
    }
    print(f"[*] Routed (batch): SQL({stats['sql']}) | Mongo({stats['mongo']}) | Buffer(total={stats['buffer']})")
    
    return stats


if __name__ == "__main__":
    route_data()