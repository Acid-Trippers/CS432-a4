import json
from datetime import datetime, timezone
from typing import Any
from fastapi import APIRouter, Request, HTTPException
import httpx
from pathlib import Path

from src.config import (
    API_HOST,
    INITIAL_SCHEMA_FILE,
    METADATA_FILE,
    CHECKPOINT_FILE,
    TRANSACTION_LOG_FILE,
    QUERY_OUTPUT_FILE,
)

router = APIRouter()

_DATA_DIR = Path(METADATA_FILE).resolve().parent
_PIPELINE_CHECKPOINT_FILE = _DATA_DIR / "pipeline_checkpoint.json"
_PERF_REPORTS_DIR = _DATA_DIR / "performance_reports"
_WORKFLOW_METRICS_FILE = _DATA_DIR / "developer_workflow_metrics.json"
_DEV_LOGICAL_QUERY_METRICS_FILE = _DATA_DIR / "developer_logical_query_metrics.json"

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
        
    # Success = committed + rolled_back (both indicate proper operation)
    # Failed = only transactions in "failed_needs_recovery" state
    successful = committed + rolled_back
    sr = round((successful / total * 100), 1) if total > 0 else 0.0
    
    return {
        "total": total,
        "committed": committed,
        "rolled_back": rolled_back,
        "failed": failed,
        "success_rate": sr
    }


def _load_latest_query_metrics():
    payload = _read_json(QUERY_OUTPUT_FILE, {})
    if not isinstance(payload, dict):
        return {}

    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}

    latest = {
        "operation": payload.get("operation"),
        "status": payload.get("status", "unknown"),
        "metrics": metrics,
    }

    try:
        latest["updated_at"] = datetime.fromtimestamp(
            Path(QUERY_OUTPUT_FILE).stat().st_mtime,
            timezone.utc,
        ).isoformat().replace("+00:00", "Z")
    except Exception:
        latest["updated_at"] = None

    return latest


def _extract_avg_latency_ms(case_result: Any) -> float | None:
    if not isinstance(case_result, dict):
        return None
    avg_ms = case_result.get("avg_ms")
    if avg_ms is None:
        return None
    return round(_safe_float(avg_ms, fallback=0.0), 3)


def _load_logical_query_test_metrics():
    persisted = _read_json(_DEV_LOGICAL_QUERY_METRICS_FILE, {})
    if isinstance(persisted, dict) and persisted:
        return {
            "has_results": bool(persisted.get("has_results", False)),
            "read_ms": persisted.get("read_ms"),
            "create_ms": persisted.get("create_ms"),
            "update_ms": persisted.get("update_ms"),
            "delete_ms": persisted.get("delete_ms"),
            "updated_at": persisted.get("updated_at"),
        }

    files = sorted(
        [
            p
            for p in _PERF_REPORTS_DIR.glob("logical_query_response_time_*.json")
            if p.is_file()
        ],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    if not files:
        return {
            "has_results": False,
            "read_ms": None,
            "create_ms": None,
            "update_ms": None,
            "delete_ms": None,
            "updated_at": None,
        }

    latest_file = files[0]
    report = _read_json(latest_file, {})
    results = report.get("results") if isinstance(report.get("results"), dict) else {}

    read_ms = _extract_avg_latency_ms(results.get("READ_simple"))
    create_ms = _extract_avg_latency_ms(results.get("CREATE_with_cleanup"))
    update_ms = _extract_avg_latency_ms(results.get("UPDATE_with_cleanup"))
    delete_ms = _extract_avg_latency_ms(results.get("DELETE_with_cleanup"))

    try:
        updated_at = datetime.fromtimestamp(latest_file.stat().st_mtime, timezone.utc)
        updated_at = updated_at.isoformat().replace("+00:00", "Z")
    except Exception:
        updated_at = None

    return {
        "has_results": any(v is not None for v in [read_ms, create_ms, update_ms, delete_ms]),
        "read_ms": read_ms,
        "create_ms": create_ms,
        "update_ms": update_ms,
        "delete_ms": delete_ms,
        "updated_at": updated_at,
    }


def _load_actual_query_overview():
    latest_query = _load_latest_query_metrics()
    metrics = latest_query.get("metrics") if isinstance(latest_query.get("metrics"), dict) else {}
    operation = latest_query.get("operation")

    has_run = bool(operation)
    total_ms = metrics.get("total_time_ms")
    latency_ms = round(_safe_float(total_ms, fallback=0.0), 3) if total_ms is not None else None

    return {
        "has_run": has_run,
        "operation": operation,
        "latency_ms": latency_ms,
        "updated_at": latest_query.get("updated_at"),
    }


def _load_performance_report_summaries(limit: int = 12):
    if not _PERF_REPORTS_DIR.exists() or not _PERF_REPORTS_DIR.is_dir():
        return {"count": 0, "items": []}

    files = sorted(
        [p for p in _PERF_REPORTS_DIR.glob("*.json") if p.is_file()],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    items = []
    for p in files[:limit]:
        data = _read_json(p, {})
        items.append(
            {
                "filename": p.name,
                "experiment": data.get("experiment", "unknown"),
                "generated_at": data.get("generated_at"),
                "size_bytes": p.stat().st_size,
                "updated_at": datetime.fromtimestamp(p.stat().st_mtime, timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
            }
        )

    return {
        "count": len(files),
        "items": items,
    }


def _load_workflow_performance_metrics():
    payload = _read_json(_WORKFLOW_METRICS_FILE, {})
    if not isinstance(payload, dict):
        payload = {}

    initialize_metrics = payload.get("initialize") if isinstance(payload.get("initialize"), dict) else {}
    fetch_metrics = payload.get("fetch") if isinstance(payload.get("fetch"), dict) else {}

    return {
        "initialize": initialize_metrics,
        "fetch": {
            "has_fetched": bool(fetch_metrics),
            **fetch_metrics,
        },
    }

def _load_last_fetch():
    # Read as dict default so valid checkpoint objects are not discarded.
    ckpt = _read_json(_PIPELINE_CHECKPOINT_FILE, {})
    if not isinstance(ckpt, dict) or not ckpt:
        ckpt = _read_json(CHECKPOINT_FILE, {})

    timestamp = (
        ckpt.get("timestamp")
        or ckpt.get("time")
        or ckpt.get("last_fetch_timestamp")
    )

    return {
        "timestamp": _to_iso_timestamp(timestamp),
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
    
    # Get database connection status
    try:
        from src.phase_6.CRUD_operations import sql_available, mongo_available
        sql_connected = bool(sql_available)
        mongo_connected = bool(mongo_available)
    except Exception:
        sql_connected = False
        mongo_connected = False

    return {
        "pipeline_state": str(pipeline_state) if pipeline_state else "fresh",
        "has_schema": has_schema,
        "has_metadata": has_metadata,
        "pipeline_busy": pipeline_busy,
        "sql_connected": sql_connected,
        "mongo_connected": mongo_connected,
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


@router.get("/api/developer/metrics")
async def get_developer_metrics(request: Request):
    """Developer-only metrics payload for dashboard diagnostics and performance reports."""
    return {
        "latest_query": _load_latest_query_metrics(),
        "logical_query_tests": _load_logical_query_test_metrics(),
        "actual_query": _load_actual_query_overview(),
        "performance_reports": _load_performance_report_summaries(),
        "workflow_performance": _load_workflow_performance_metrics(),
        "transactions": _compute_transaction_stats(),
        "pipeline_state": str(getattr(request.app.state, "pipeline_state", "fresh")),
        "pipeline_busy": bool(getattr(request.app.state, "pipeline_busy", False)),
    }


def _run_performance_test(test_name: str) -> dict[str, Any]:
    """Run one named performance test and return structured payload for UI consumption."""
    logical_query_case_map = {
        "logical_query_read": "READ_simple",
        "logical_query_create": "CREATE_with_cleanup",
        "logical_query_update": "UPDATE_with_cleanup",
        "logical_query_delete": "DELETE_with_cleanup",
    }

    if test_name in logical_query_case_map:
        from performance_Evaluation.logical_query_response_time import benchmark_query_case

        case_name = logical_query_case_map[test_name]
        case_result = benchmark_query_case(case_name=case_name, runs=2, mode="direct")

        existing = _read_json(_DEV_LOGICAL_QUERY_METRICS_FILE, {})
        if not isinstance(existing, dict):
            existing = {}

        mapped_field = {
            "logical_query_read": "read_ms",
            "logical_query_create": "create_ms",
            "logical_query_update": "update_ms",
            "logical_query_delete": "delete_ms",
        }[test_name]

        existing[mapped_field] = _extract_avg_latency_ms(case_result)
        existing["has_results"] = bool(
            existing.get("read_ms") is not None
            or existing.get("create_ms") is not None
            or existing.get("update_ms") is not None
            or existing.get("delete_ms") is not None
        )
        existing["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        try:
            with open(_DEV_LOGICAL_QUERY_METRICS_FILE, "w", encoding="utf-8") as f:
                json.dump(existing, f, indent=2)
        except Exception:
            pass

        return {
            "case": case_name,
            "summary": case_result,
        }

    if test_name == "metadata_lookup_overhead":
        from performance_Evaluation.metadata_lookup_overhead import (
            benchmark_metadata_file_read,
            benchmark_metadata_parse,
            benchmark_field_lookup,
            benchmark_end_to_end_metadata_path,
        )

        return {
            "file_read": benchmark_metadata_file_read(runs=50),
            "json_parse": benchmark_metadata_parse(runs=50),
            "field_lookup": benchmark_field_lookup(runs=500),
            "end_to_end_get_field_locations": benchmark_end_to_end_metadata_path(runs=100),
        }

    if test_name == "transaction_coordination_overhead":
        from performance_Evaluation.transaction_coordination_overhead_sql_mongo import (
            benchmark_coordination_overhead,
            payload_distribution_insight,
        )

        return {
            "distribution_hint": payload_distribution_insight(),
            "results": benchmark_coordination_overhead(runs=3, mode="direct"),
        }

    raise HTTPException(status_code=404, detail=f"Unknown performance test: {test_name}")


@router.get("/api/developer/performance/tests")
async def get_developer_performance_tests():
    """List available performance tests for developer dashboard cards."""
    return {
        "tests": [
            {
                "id": "logical_query_read",
                "label": "Logical Query READ",
                "description": "Benchmarks READ query latency.",
            },
            {
                "id": "logical_query_create",
                "label": "Logical Query CREATE",
                "description": "Benchmarks CREATE query latency.",
            },
            {
                "id": "logical_query_update",
                "label": "Logical Query UPDATE",
                "description": "Benchmarks UPDATE query latency.",
            },
            {
                "id": "logical_query_delete",
                "label": "Logical Query DELETE",
                "description": "Benchmarks DELETE query latency.",
            },
            {
                "id": "metadata_lookup_overhead",
                "label": "Metadata Lookup Overhead",
                "description": "Measures file read, parse, and lookup latency paths.",
            },
            {
                "id": "transaction_coordination_overhead",
                "label": "Transaction Coordination Overhead",
                "description": "Compares coordinator-backed writes vs manual baseline.",
            },
        ]
    }


@router.post("/api/developer/performance/{test_name}")
async def run_developer_performance_test(test_name: str):
    """Run one performance test and return machine-friendly result + human status."""
    started = datetime.now(timezone.utc)
    result = _run_performance_test(test_name)
    finished = datetime.now(timezone.utc)

    return {
        "test": test_name,
        "status": "completed",
        "started_at": started.isoformat().replace("+00:00", "Z"),
        "completed_at": finished.isoformat().replace("+00:00", "Z"),
        "result": result,
    }


@router.post("/api/developer/performance/run_all")
async def run_all_developer_performance_tests():
    """Run all performance tests sequentially for quick developer diagnostics."""
    started = datetime.now(timezone.utc)
    ordered_tests = [
        "logical_query_read",
        "logical_query_create",
        "logical_query_update",
        "logical_query_delete",
        "metadata_lookup_overhead",
        "transaction_coordination_overhead",
    ]

    results = {}
    for test_name in ordered_tests:
        results[test_name] = _run_performance_test(test_name)

    finished = datetime.now(timezone.utc)
    return {
        "status": "completed",
        "started_at": started.isoformat().replace("+00:00", "Z"),
        "completed_at": finished.isoformat().replace("+00:00", "Z"),
        "results": results,
    }
