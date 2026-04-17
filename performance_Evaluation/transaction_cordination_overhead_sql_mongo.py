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


def _reserve_record_id():
    """Reserve and increment the shared record counter for manual baseline writes."""
    print("[COUNTER] Reserving next record_id from counter file")
    # Keep alignment with counter logic used by CREATE path.
    current = 0
    try:
        with open(COUNTER_FILE, "r", encoding="utf-8") as f:
            current = int((f.read().strip() or "0"))
    except Exception:
        current = 0

    with open(COUNTER_FILE, "w", encoding="utf-8") as f:
        f.write(str(current + 1))

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
    """Run one manual SQL+Mongo create baseline without transaction coordinator."""
    print("[MANUAL BASELINE] Running one non-transactional baseline create trial")
    # Baseline: direct SQL then direct Mongo without tx coordinator.
    crud_ops.refresh_connections()

    if not crud_ops.sql_available or not crud_ops.mongo_available:
        raise RuntimeError("Both SQL and Mongo must be available for this benchmark")

    record_id = _reserve_record_id()
    tag = uuid.uuid4().hex[:10]

    sql_payload = {
        "record_id": record_id,
        "username": f"manual_user_{tag}",
        "city": "manual_city",
        "subscription": "free"
    }

    mongo_payload = {
        "_id": record_id,
        "record_id": record_id,
        "username": f"manual_user_{tag}",
        "city": "manual_city",
        "subscription": "free"
    }

    t0 = time.perf_counter()

    inserted_id = crud_ops.sql_engine.insert_record(sql_payload)
    if inserted_id is None:
        raise RuntimeError("SQL insert failed in manual baseline")

    crud_ops.mongo_db["main_records"].insert_one(mongo_payload)

    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    _delete_record_everywhere(record_id)
    print(f"[MANUAL BASELINE] Trial latency={elapsed_ms:.3f}ms")
    return elapsed_ms


def benchmark_coordination_overhead(runs=20, mode="direct", api_base_url="http://127.0.0.1:8080"):
    """Benchmark coordinator overhead by comparing coordinated and manual baselines."""
    print(f"[BENCH] Starting coordination overhead benchmark with runs={runs}, mode={mode}")
    coordinated_samples = []
    manual_samples = []

    for run_idx in range(1, runs + 1):
        print(f"[BENCH] Run {run_idx}/{runs}")
        coordinated_samples.append(
            coordinated_create_once(mode=mode, api_base_url=api_base_url)
        )
        manual_samples.append(
            manual_non_transactional_create_once()
        )

    coordinated_summary = _summarize_ms(coordinated_samples)
    manual_summary = _summarize_ms(manual_samples)

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
    # Distribution of representative write fields by metadata routing.
    locations = get_field_locations()
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
    report = {
        "experiment": "transaction_coordination_overhead_sql_mongo",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "mode": mode,
        "runs": runs,
        "distribution_hint": payload_distribution_insight(),
        "results": benchmark_coordination_overhead(
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


if __name__ == "__main__":
    run_all()