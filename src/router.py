import json
import os
from typing import Dict, List

from src.config import CLEANED_DATA_FILE, DATA_DIR, METADATA_FILE
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
        field_name = field.get("fieldName") or field.get("field_name")
        if not field_name:
            continue

        top_level_name = str(field_name).split(".", 1)[0].replace("[]", "")
        decision = str(field.get("decision", "UNKNOWN")).upper()

        if decision not in {"SQL", "MONGO", "UNKNOWN"}:
            decision = "UNKNOWN"

        # Keep first decision per top-level field; this stays deterministic and simple.
        if top_level_name not in routes:
            routes[top_level_name] = decision

    return routes


def route_data() -> None:
    if not os.path.exists(METADATA_FILE):
        raise FileNotFoundError(f"Missing metadata: {METADATA_FILE}")
    if not os.path.exists(CLEANED_DATA_FILE):
        raise FileNotFoundError(f"Missing cleaned input data: {CLEANED_DATA_FILE}")

    metadata = _read_json(METADATA_FILE)
    cleaned_data = _read_json(CLEANED_DATA_FILE)

    field_metadata = metadata.get('fields', [])

    if not isinstance(field_metadata, list):
        raise ValueError("field_metadata.json must contain a list of field decisions")
    if not isinstance(cleaned_data, list):
        raise ValueError("cleaned_data.json must contain a list of records")

    routes = _build_field_routes(field_metadata)

    sql_data: List[Dict] = []
    mongo_data: List[Dict] = []
    unknown_data: List[Dict] = []

    for record in cleaned_data:
        sql_record = {}
        mongo_record = {}
        unknown_record = {}

        for key, value in record.items():
            decision = routes.get(key, "UNKNOWN")
            if decision == "SQL":
                sql_record[key] = value
            elif decision == "MONGO":
                mongo_record[key] = value
            else:
                unknown_record[key] = value

        if sql_record:
            sql_data.append(sql_record)
        if mongo_record:
            mongo_data.append(mongo_record)
        if unknown_record:
            unknown_data.append(unknown_record)

    _write_json(SQL_OUTPUT_FILE, sql_data)
    _write_json(MONGO_OUTPUT_FILE, mongo_data)
    _write_json(UNKNOWN_OUTPUT_FILE, unknown_data)

    print(f"[+] SQL-routed records written to: {SQL_OUTPUT_FILE} ({len(sql_data)} records)")
    print(f"[+] Mongo-routed records written to: {MONGO_OUTPUT_FILE} ({len(mongo_data)} records)")
    print(f"[+] Unknown-routed records written to: {UNKNOWN_OUTPUT_FILE} ({len(unknown_data)} records)")


if __name__ == "__main__":
    route_data()