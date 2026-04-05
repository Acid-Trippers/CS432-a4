import json
from datetime import datetime, timezone
from typing import Any
from fastapi import APIRouter, Request
import httpx
from pathlib import Path

from src.config import (
    API_HOST,
    INITIAL_SCHEMA_FILE,
    METADATA_FILE,
    CHECKPOINT_FILE,
    TRANSACTION_LOG_FILE
)

router = APIRouter()

_DATA_DIR = Path(METADATA_FILE).resolve().parent
_PIPELINE_CHECKPOINT_FILE = _DATA_DIR / "pipeline_checkpoint.json"

def _read_json(path, default):
    try:
        p = Path(path)
        if not p.exists():
            return default
        with open(p, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, type(default)):
                return data
            return default
    except Exception:
        return default

def _safe_float(value, fallback=0.0):
    try:
        val = float(value)
        if val != val:  # check for NaN
            return fallback
        return val
    except (ValueError, TypeError):
        return fallback

def _safe_int(value, fallback=0):
    try:
        return int(value)
    except (ValueError, TypeError):
        return fallback

def _to_iso_timestamp(raw_timestamp):
    if raw_timestamp is None:
        return None
    if isinstance(raw_timestamp, (int, float)):
        return datetime.fromtimestamp(raw_timestamp, timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(raw_timestamp, str):
        try:
            return datetime.fromtimestamp(float(raw_timestamp), timezone.utc).isoformat().replace("+00:00", "Z")
        except ValueError:
            return raw_timestamp.strip()
    return None

def _field_status(field):
    if field.get("is_discovered_buffer"):
        return "pending"
    if field.get("user_constraints") is None:
        return "discovered"
    return "defined"

def _storage_type(field):
    if field.get("is_discovered_buffer"):
        return "pending"
    decision = field.get("decision")
    if decision == "SQL":
        return "structured"
    if decision == "MONGO":
        return "flexible"
    return "pending"

def _load_metadata_fields():
    metadata = _read_json(METADATA_FILE, {})
    raw_fields = metadata.get("fields", {})
    if isinstance(raw_fields, dict):
        return [v for k, v in raw_fields.items() if isinstance(v, dict)]
    elif isinstance(raw_fields, list):
        return [v for v in raw_fields if isinstance(v, dict)]
    return []

def _build_active_fields(fields):
    defined = 0
    discovered = 0
    pending = 0
    details = []

    for f in fields:
        st = _field_status(f)
        if st == "defined":
            defined += 1
        elif st == "discovered":
            discovered += 1
        else:
            pending += 1
        
        freq = _safe_float(f.get("frequency", 0.0))
        details.append({
            "field_name": f.get("name", "unknown"),
            "status": st,
            "frequency": round(freq, 4),
            "density": round(freq, 4),
            "storage_type": _storage_type(f)
        })

    details.sort(key=lambda x: x["field_name"])
    return {
        "total": defined + discovered,
        "defined": defined,
        "discovered": discovered,
        "pending": pending,
        "details": details
    }

def _compute_data_density(fields):
    active_fields = [f for f in fields if _field_status(f) in ("defined", "discovered")]
    if not active_fields:
        return 0.0
    total_freq = sum(_safe_float(f.get("frequency", 0.0)) for f in active_fields)
    avg = total_freq / len(active_fields)
    return round(avg * 100, 1)

def _compute_transaction_stats():
    log = _read_json(TRANSACTION_LOG_FILE, [])
    total = len(log)
    committed = 0
    rolled_back = 0
    failed = 0
    for entry in log:
        st = entry.get("state")
        if st == "committed":
            committed += 1
        elif st == "rolled_back":
            rolled_back += 1
        elif st == "failed_needs_recovery":
            failed += 1
        
    sr = round((committed / total * 100), 1) if total > 0 else 0.0
    return {
        "total": total,
        "committed": committed,
        "rolled_back": rolled_back,
        "failed": failed,
        "success_rate": sr
    }

def _load_last_fetch():
    ckpt = _read_json(_PIPELINE_CHECKPOINT_FILE, None)
    if ckpt is None or not isinstance(ckpt, dict):
        ckpt = _read_json(CHECKPOINT_FILE, {})
    
    return {
        "timestamp": _to_iso_timestamp(ckpt.get("timestamp")),
        "count": _safe_int(ckpt.get("count", 0))
    }

def _get_total_records_from_sql(request: Request):
    if getattr(request.app.state, "sql_initialized", False):
        try:
            sql_engine = getattr(request.app.state, "sql_engine", None)
            if sql_engine:
                return sql_engine.get_table_count("main_records")
        except Exception:
            pass
    metadata = _read_json(METADATA_FILE, {})
    return _safe_int(metadata.get("total_records", 0))

async def _check_external_api_reachable():
    try:
        async with httpx.AsyncClient(timeout=1.5) as client:
            response = await client.get(f"{API_HOST}/" if not API_HOST.endswith('/') else API_HOST)
            return response.is_success
    except Exception:
        return False

@router.get("/api/status")
async def get_status(request: Request):
    has_schema = bool(Path(INITIAL_SCHEMA_FILE).exists())
    has_metadata = bool(Path(METADATA_FILE).exists())
    pipeline_state = getattr(request.app.state, "pipeline_state", "fresh")
    pipeline_busy = bool(getattr(request.app.state, "pipeline_busy", False))

    return {
        "pipeline_state": str(pipeline_state) if pipeline_state else "fresh",
        "has_schema": has_schema,
        "has_metadata": has_metadata,
        "pipeline_busy": pipeline_busy,
    }

@router.get("/api/stats")
async def get_stats(request: Request):
    external_api_reachable = await _check_external_api_reachable()
    pipeline_busy = bool(getattr(request.app.state, "pipeline_busy", False))
    total_records = _get_total_records_from_sql(request)

    status = "pipeline_busy" if pipeline_busy else "ok"

    return {
        "status": status,
        "pipeline_busy": pipeline_busy,
        "total_records": total_records,
        "external_api_reachable": external_api_reachable,
    }

@router.get("/api/pipeline/stats")
async def get_pipeline_stats(request: Request):
    fields = _load_metadata_fields()
    
    return {
        "total_records": _get_total_records_from_sql(request),
        "active_fields": _build_active_fields(fields),
        "data_density": _compute_data_density(fields),
        "pipeline_state": str(getattr(request.app.state, "pipeline_state", "fresh")),
        "pipeline_busy": bool(getattr(request.app.state, "pipeline_busy", False)),
        "last_fetch": _load_last_fetch(),
        "transactions": _compute_transaction_stats()
    }
