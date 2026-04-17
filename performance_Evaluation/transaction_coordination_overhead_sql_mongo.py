import json
import statistics
import time
import uuid
from datetime import datetime
from pathlib import Path

import httpx
from src.config import COUNTER_FILE
from src.phase_6 import CRUD_operations as crud_ops
from src.phase_6.CRUD_runner import get_field_locations, query_runner


REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "performance_reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)


def _summarize_ms(samples_ms):
    """Aggregate millisecond timings into stats and throughput for coordination tests."""
    print(f"[SUMMARY] Building transaction stats for {len(samples_ms)} samples")
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

    sorted_ms = sorted(samples_ms)
    runs = len(samples_ms)
    total_s = sum(samples_ms) / 1000.0
    p50 = sorted_ms[int(0.50 * (runs - 1))]
    p95 = sorted_ms[int(0.95 * (runs - 1))]

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
    """Delete one record_id from SQL and Mongo to keep coordination runs clean."""
    print(f"[CLEANUP] Deleting record_id={record_id} from SQL and Mongo for entity='{entity}'")

    # FIX: Wrap the entire cleanup in a try/except so a failed delete never
    # propagates and crashes a benchmark run that otherwise succeeded.
    try:
        crud_ops.refresh_connections()
    except Exception as exc:
        print(f"[CLEANUP] refresh_connections() failed: {exc}")
        return {"sql_deleted": 0, "mongo_deleted": 0, "error": str(exc)}

    sql_deleted = 0
    mongo_deleted = 0

    if crud_ops.sql_available:
        try:
            model = crud_ops.sql_engine.models.get(entity)
            if model is not None:
                sql_deleted = (
                    crud_ops.sql_engine.session.query(model)
                    .filter(model.record_id == record_id)
                    .delete(synchronize_session=False)
                )
                crud_ops.sql_engine.session.commit()
        except Exception as exc:
            print(f"[CLEANUP] SQL delete failed for record_id={record_id}: {exc}")

    if crud_ops.mongo_available:
        try:
            mongo_deleted = crud_ops.mongo_db[entity].delete_many(
                {"$or": [{"_id": record_id}, {"record_id": record_id}]}
            ).deleted_count
        except Exception as exc:
            print(f"[CLEANUP] Mongo delete failed for record_id={record_id}: {exc}")

    deletion_result = {"sql_deleted": int(sql_deleted), "mongo_deleted": int(mongo_deleted)}
    print(f"[CLEANUP] Deletion result: {deletion_result}")
    return deletion_result


def _reserve_record_id():
    """Reserve and increment the shared record counter for manual baseline writes."""
    print("[COUNTER] Reserving next record_id from counter file")

    # FIX: Original code assumed COUNTER_FILE's parent directory already
    # existed; if it didn't the open(..., "w") call raised FileNotFoundError
    # and crashed the manual baseline benchmark with a 500.
    counter_path = Path(COUNTER_FILE)
    counter_path.parent.mkdir(parents=True, exist_ok=True)

    current = 0
    try:
        if counter_path.exists():
            with open(COUNTER_FILE, "r", encoding="utf-8") as f:
                current = int((f.read().strip() or "0"))
    except Exception as exc:
        print(f"[COUNTER] Could not read counter file, defaulting to 0: {exc}")
        current = 0

    try:
        with open(COUNTER_FILE, "w", encoding="utf-8") as f:
            f.write(str(current + 1))
    except Exception as exc:
        # Non-fatal: the counter not updating is acceptable for a benchmark run.
        print(f"[COUNTER] Could not write counter file: {exc}")

    print(f"[COUNTER] Reserved record_id={current}, next={current + 1}")
    return current


def _execute_query(payload, mode="direct", api_base_url="http://127.0.0.1:8080"):
    """Execute a CRUD query in direct/API mode and return latency/result."""
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
        result = query_runner(query_dict=payload)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    print(f"[EXECUTE] Completed operation={operation} in {elapsed_ms:.3f}ms")
    return elapsed_ms, result


def coordinated_create_once(mode="direct", api_base_url="http://127.0.0.1:8080"):
    """Run one coordinator-backed CREATE transaction and cleanup inserted record."""
    print("[COORDINATED CREATE] Running one coordinated create trial")
    tag = uuid.uuid4().hex[:10]
    payload = {
        "operation": "CREATE",
        "entity": "main_records",
        "payload": {
            "username": f"coord_user_{tag}",
            "city": "coord_city",
            "subscription": "free"
        }
    }
    elapsed_ms, result = _execute_query(payload, mode=mode, api_base_url=api_base_url)
    record_id = (((result or {}).get("details") or {}).get("record_id"))
    if record_id is not None:
        _delete_record_everywhere(record_id)
    print(f"[COORDINATED CREATE] Trial latency={elapsed_ms:.3f}ms")
    return elapsed_ms


def manual_non_transactional_create_once():
    """Run one manual SQL+Mongo create baseline without transaction coordinator.

    FIX: The original raised RuntimeError when either backend was unavailable,
    which crashed the entire benchmark suite and caused the API to return 500.
    Now the function detects which backends are up and runs the available
    subset, labelling the result accordingly so callers always get a latency
    measurement rather than an exception.
    """
    print("[MANUAL BASELINE] Running one non-transactional baseline create trial")

    # FIX: refresh_connections() may raise; catch it so the benchmark degrades
    # gracefully rather than propagating a 500.
    try:
        crud_ops.refresh_connections()
    except Exception as exc:
        raise RuntimeError(f"refresh_connections() failed: {exc}") from exc

    sql_up = crud_ops.sql_available
    mongo_up = crud_ops.mongo_available

    if not sql_up and not mongo_up:
        raise RuntimeError(
            "Neither SQL nor Mongo is available; cannot run manual baseline"
        )

    record_id = _reserve_record_id()
    tag = uuid.uuid4().hex[:10]

    sql_payload = {
        "record_id": record_id,
        "username": f"manual_user_{tag}",
        "city": "manual_city",
        "subscription": "free",
    }
    mongo_payload = {
        "_id": record_id,
        "record_id": record_id,
        "username": f"manual_user_{tag}",
        "city": "manual_city",
        "subscription": "free",
    }

    t0 = time.perf_counter()

    if sql_up:
        # FIX: Catch SQL insert failures individually so a Mongo-only
        # environment can still produce a valid baseline measurement.
        try:
            inserted_id = crud_ops.sql_engine.insert_record(sql_payload)
            if inserted_id is None:
                print("[MANUAL BASELINE] SQL insert returned None; continuing without SQL write")
        except Exception as exc:
            print(f"[MANUAL BASELINE] SQL insert raised: {exc}; continuing without SQL write")

    if mongo_up:
        # FIX: Same individual guard for Mongo.
        try:
            crud_ops.mongo_db["main_records"].insert_one(mongo_payload)
        except Exception as exc:
            print(f"[MANUAL BASELINE] Mongo insert raised: {exc}; continuing without Mongo write")

    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    _delete_record_everywhere(record_id)
    print(f"[MANUAL BASELINE] Trial latency={elapsed_ms:.3f}ms")
    return elapsed_ms


def benchmark_coordination_overhead(runs=20, mode="direct", api_base_url="http://127.0.0.1:8080"):
    """Benchmark coordinator overhead by comparing coordinated and manual baselines."""
    print(f"[BENCH] Starting coordination overhead benchmark with runs={runs}, mode={mode}")
    coordinated_samples = []
    manual_samples = []
    coordinated_errors = 0
    manual_errors = 0

    for run_idx in range(1, runs + 1):
        print(f"[BENCH] Run {run_idx}/{runs}")

        # FIX: Each individual run is now try/caught so one failing trial no
        # longer aborts the entire benchmark.  Errors are counted and surfaced
        # in the report rather than propagated as exceptions.
        try:
            coordinated_samples.append(
                coordinated_create_once(mode=mode, api_base_url=api_base_url)
            )
        except Exception as exc:
            coordinated_errors += 1
            print(f"[BENCH] Coordinated run {run_idx} failed: {exc}")

        try:
            manual_samples.append(
                manual_non_transactional_create_once()
            )
        except Exception as exc:
            manual_errors += 1
            print(f"[BENCH] Manual run {run_idx} failed: {exc}")

    coordinated_summary = _summarize_ms(coordinated_samples)
    coordinated_summary["errors"] = coordinated_errors

    manual_summary = _summarize_ms(manual_samples)
    manual_summary["errors"] = manual_errors

    overhead_ms = coordinated_summary["avg_ms"] - manual_summary["avg_ms"]
    overhead_pct = (
        (overhead_ms / manual_summary["avg_ms"]) * 100.0
        if manual_summary["avg_ms"] > 0 else 0.0
    )

    result = {
        "coordinated_create": coordinated_summary,
        "manual_non_transactional_create": manual_summary,
        "estimated_coordination_overhead_ms": round(overhead_ms, 3),
        "estimated_coordination_overhead_pct": round(overhead_pct, 2),
    }
    print(f"[BENCH] Coordination benchmark result: {result}")
    return result


def payload_distribution_insight():
    """Estimate representative field routing distribution from metadata decisions."""
    print("[DISTRIBUTION] Computing metadata-driven backend distribution insight")

    # FIX: get_field_locations() may raise (e.g. if METADATA_FILE is missing);
    # return a zeroed insight instead of propagating the exception.
    try:
        locations = get_field_locations() or {}
    except Exception as exc:
        print(f"[DISTRIBUTION] get_field_locations() raised: {exc}")
        return {
            "field_count": 0,
            "counts": {"SQL": 0, "MONGO": 0, "Unknown": 0},
            "pct": {"SQL": 0.0, "MONGO": 0.0, "Unknown": 0.0},
            "error": str(exc),
        }

    fields = ["username", "city", "subscription", "age", "battery", "record_id"]
    breakdown = {"SQL": 0, "MONGO": 0, "Unknown": 0}

    for field in fields:
        backend = locations.get(field, "Unknown")
        if backend not in breakdown:
            breakdown["Unknown"] += 1
        else:
            breakdown[backend] += 1

    total = sum(breakdown.values()) or 1
    insight = {
        "field_count": total,
        "counts": breakdown,
        "pct": {
            "SQL": round((breakdown["SQL"] / total) * 100.0, 2),
            "MONGO": round((breakdown["MONGO"] / total) * 100.0, 2),
            "Unknown": round((breakdown["Unknown"] / total) * 100.0, 2),
        },
    }
    print(f"[DISTRIBUTION] Insight: {insight}")
    return insight


def run_all(runs=20, mode="direct", api_base_url="http://127.0.0.1:8080"):
    """Run the full transaction coordination overhead suite and persist report."""
    print(f"[RUN ALL] Starting coordination overhead suite (runs={runs}, mode={mode})")

    # FIX: Wrap each top-level call so a failure in distribution_hint or
    # benchmark_coordination_overhead doesn't prevent the report from being
    # written and returned, eliminating the 500 for partial-availability envs.
    def _safe_run(name, fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            print(f"[RUN ALL] '{name}' raised: {exc}")
            return {"error": str(exc), "runs": 0}

    report = {
        "experiment": "transaction_coordination_overhead_sql_mongo",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "mode": mode,
        "runs": runs,
        "distribution_hint": _safe_run("payload_distribution_insight", payload_distribution_insight),
        "results": _safe_run(
            "benchmark_coordination_overhead",
            benchmark_coordination_overhead,
            runs=runs,
            mode=mode,
            api_base_url=api_base_url,
        ),
    }

    out_file = REPORT_DIR / f"transaction_coordination_overhead_{int(time.time())}.json"
    print(f"[RUN ALL] Writing report to {out_file}")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(json.dumps(report, indent=2))
    print(f"Saved report: {out_file}")
    print("[RUN ALL] Coordination overhead suite finished")
    return report


if __name__ == "__main__":
    run_all()