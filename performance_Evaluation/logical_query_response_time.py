import json
import statistics
import time
import uuid
from datetime import datetime
from pathlib import Path

import httpx
from src.phase_6 import CRUD_operations as crud_ops
from src.phase_6.CRUD_runner import query_runner


REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "performance_reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)


def _summarize_ms(samples_ms):
    """Aggregate millisecond latencies into summary statistics and throughput."""
    print(f"[SUMMARY] Building query stats for {len(samples_ms)} successful runs")
    if not samples_ms:
        return {
            "runs": 0,
            "avg_ms": 0.0,
            "min_ms": 0.0,
            "max_ms": 0.0,
            "p50_ms": 0.0,
            "p95_ms": 0.0,
            "throughput_ops_per_sec": 0.0,
        }

    samples_ms_sorted = sorted(samples_ms)
    runs = len(samples_ms)
    total_s = sum(samples_ms) / 1000.0
    p50 = samples_ms_sorted[int(0.50 * (runs - 1))]
    p95 = samples_ms_sorted[int(0.95 * (runs - 1))]

    return {
        "runs": runs,
        "avg_ms": round(statistics.mean(samples_ms), 3),
        "min_ms": round(min(samples_ms), 3),
        "max_ms": round(max(samples_ms), 3),
        "p50_ms": round(p50, 3),
        "p95_ms": round(p95, 3),
        "throughput_ops_per_sec": round(runs / total_s, 3) if total_s > 0 else 0.0,
    }


def _delete_record_everywhere(record_id, entity="main_records"):
    """Delete one record_id from both SQL and Mongo to keep benchmark runs side-effect free."""
    print(f"[CLEANUP] Deleting record_id={record_id} from SQL and Mongo for entity='{entity}'")
    crud_ops.refresh_connections()

    sql_deleted = 0
    mongo_deleted = 0

    if crud_ops.sql_available:
        model = crud_ops.sql_engine.models.get(entity)
        if model is not None:
            sql_deleted = (
                crud_ops.sql_engine.session.query(model)
                .filter(model.record_id == record_id)
                .delete(synchronize_session=False)
            )
            crud_ops.sql_engine.session.commit()

    if crud_ops.mongo_available:
        mongo_deleted = crud_ops.mongo_db[entity].delete_many(
            {"$or": [{"_id": record_id}, {"record_id": record_id}]}
        ).deleted_count

    deletion_result = {"sql_deleted": int(sql_deleted), "mongo_deleted": int(mongo_deleted)}
    print(f"[CLEANUP] Deletion result: {deletion_result}")
    return deletion_result


def _execute_query(payload, mode="direct", api_base_url="http://127.0.0.1:8080"):
    """Execute one query via direct backend call or API endpoint and return latency/result."""
    operation = payload.get("operation")
    print(f"[EXECUTE] Running operation={operation} in mode={mode}")
    t0 = time.perf_counter()

    if mode == "api":
        with httpx.Client(timeout=60.0) as client:
            resp = client.post(f"{api_base_url}/api/query", json=payload)
            resp.raise_for_status()
            result = resp.json()
    else:
        crud_ops.refresh_connections()
        result = query_runner(query_dict=payload, persist_output=False)

    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    print(f"[EXECUTE] Completed operation={operation} in {elapsed_ms:.3f}ms")
    return elapsed_ms, result


def _create_temp_record(mode, tag):
    """Create a temporary benchmark record and return its record_id for follow-up tests."""
    print(f"[TEMP CREATE] Creating temporary record with tag={tag}")
    payload = {
        "operation": "CREATE",
        "entity": "main_records",
        "payload": {
            "username": f"perf_user_{tag}",
            "city": "perf_city",
            "subscription": "free"
        }
    }
    _, result = _execute_query(payload, mode=mode)
    record_id = (((result or {}).get("details") or {}).get("record_id"))
    print(f"[TEMP CREATE] Created record_id={record_id}")
    return record_id, result


def _run_case(case_fn, runs=5):
    """Run one benchmark case repeatedly and collect latency/error summary."""
    print(f"[CASE] Starting case '{case_fn.__name__}' for runs={runs}")
    latencies = []
    errors = 0

    for run_idx in range(1, runs + 1):
        try:
            print(f"[CASE] {case_fn.__name__}: run {run_idx}/{runs}")
            ms = case_fn()
            latencies.append(ms)
        except Exception as exc:
            errors += 1
            print(f"[CASE] {case_fn.__name__}: run {run_idx} failed with error: {exc}")

    summary = _summarize_ms(latencies)
    summary["errors"] = errors
    print(f"[CASE] Finished case '{case_fn.__name__}' with summary: {summary}")
    return summary


def _build_query_benchmark_cases(mode="direct", api_base_url="http://127.0.0.1:8080"):
    def case_read_simple():
        """Benchmark a simple equality READ query."""
        payload = {
            "operation": "READ",
            "entity": "main_records",
            "filters": {"city": "perf_city"},
            "columns": ["username", "city", "subscription"]
        }
        ms, _ = _execute_query(payload, mode=mode, api_base_url=api_base_url)
        return ms

    def case_read_advanced():
        """Benchmark an advanced READ query with logical operators and typed predicates."""
        payload = {
            "operation": "READ",
            "entity": "main_records",
            "filters": {
                "$and": [
                    {"age": {"$gte": 18}},
                    {"$or": [{"city": {"$contains": "a"}}, {"subscription": "free"}]}
                ]
            },
            "columns": ["username", "age", "city", "subscription"]
        }
        ms, _ = _execute_query(payload, mode=mode, api_base_url=api_base_url)
        return ms

    def case_create_with_cleanup():
        """Benchmark CREATE and cleanup the inserted record in both backends."""
        tag = uuid.uuid4().hex[:10]
        payload = {
            "operation": "CREATE",
            "entity": "main_records",
            "payload": {
                "username": f"perf_create_{tag}",
                "city": "perf_city",
                "subscription": "free"
            }
        }
        ms, result = _execute_query(payload, mode=mode, api_base_url=api_base_url)
        record_id = (((result or {}).get("details") or {}).get("record_id"))
        if record_id is not None:
            _delete_record_everywhere(record_id)
        return ms

    def case_update_with_cleanup():
        """Benchmark UPDATE on a temp record and cleanup in both backends."""
        tag = uuid.uuid4().hex[:10]
        record_id, _ = _create_temp_record(mode, tag)
        if record_id is None:
            raise RuntimeError("could not create temp record for update case")

        payload = {
            "operation": "UPDATE",
            "entity": "main_records",
            "filters": {"record_id": record_id},
            "payload": {"subscription": "premium"}
        }
        ms, _ = _execute_query(payload, mode=mode, api_base_url=api_base_url)
        _delete_record_everywhere(record_id)
        return ms

    def case_delete_case():
        """Benchmark DELETE on a temp record and run safety cleanup in both backends."""
        tag = uuid.uuid4().hex[:10]
        record_id, _ = _create_temp_record(mode, tag)
        if record_id is None:
            raise RuntimeError("could not create temp record for delete case")

        payload = {
            "operation": "DELETE",
            "entity": "main_records",
            "filters": {"record_id": record_id}
        }
        ms, _ = _execute_query(payload, mode=mode, api_base_url=api_base_url)

        # Safety cleanup from both SQL and Mongo even if delete query failed.
        _delete_record_everywhere(record_id)
        return ms

    return {
        "READ_simple": case_read_simple,
        "READ_advanced": case_read_advanced,
        "CREATE_with_cleanup": case_create_with_cleanup,
        "UPDATE_with_cleanup": case_update_with_cleanup,
        "DELETE_with_cleanup": case_delete_case,
    }


def benchmark_query_case(case_name, runs=5, mode="direct", api_base_url="http://127.0.0.1:8080"):
    """Benchmark one logical query case and return the summarized metrics."""
    print(f"[BENCH] Running logical query benchmark case={case_name}, runs={runs}, mode={mode}")
    cases = _build_query_benchmark_cases(mode=mode, api_base_url=api_base_url)
    case_fn = cases.get(case_name)
    if case_fn is None:
        raise ValueError(f"Unknown query benchmark case: {case_name}")
    return _run_case(case_fn, runs=runs)


def benchmark_query_types(runs=5, mode="direct", api_base_url="http://127.0.0.1:8080"):
    """Benchmark READ/CREATE/UPDATE/DELETE query classes with automatic cleanup."""
    print(f"[BENCH] Running logical query benchmarks with runs={runs}, mode={mode}")
    cases = _build_query_benchmark_cases(mode=mode, api_base_url=api_base_url)
    results = {
        "READ_simple": _run_case(cases["READ_simple"], runs=runs),
        "READ_advanced": _run_case(cases["READ_advanced"], runs=runs),
        "CREATE_with_cleanup": _run_case(cases["CREATE_with_cleanup"], runs=runs),
        "UPDATE_with_cleanup": _run_case(cases["UPDATE_with_cleanup"], runs=runs),
        "DELETE_with_cleanup": _run_case(cases["DELETE_with_cleanup"], runs=runs),
    }
    print(f"[BENCH] Logical query benchmark completed with results: {results}")
    return results


def run_all(runs=5, mode="direct", api_base_url="http://127.0.0.1:8080"):
    """Run the full logical query response-time suite and persist the report to disk."""
    print(f"[RUN ALL] Starting logical query response-time suite (runs={runs}, mode={mode})")
    report = {
        "experiment": "logical_query_response_time",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "mode": mode,
        "runs_per_case": runs,
        "results": benchmark_query_types(runs=runs, mode=mode, api_base_url=api_base_url),
    }

    out_file = REPORT_DIR / f"logical_query_response_time_{int(time.time())}.json"
    print(f"[RUN ALL] Writing report to {out_file}")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(json.dumps(report, indent=2))
    print(f"Saved report: {out_file}")
    print("[RUN ALL] Logical query response-time suite finished")


if __name__ == "__main__":
    run_all()