import json
import statistics
import time
from datetime import datetime
from pathlib import Path

import main
from src.phase_6 import CRUD_operations as crud_ops


REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "performance_reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)


def _measure_call(fn, *args, **kwargs):
    """Execute a callable and return elapsed wall-clock time in seconds."""
    print(f"[MEASURE] Starting {fn.__name__} with args={args}, kwargs={kwargs}")
    t0 = time.perf_counter()
    fn(*args, **kwargs)
    t1 = time.perf_counter()
    elapsed = t1 - t0
    print(f"[MEASURE] Completed {fn.__name__} in {elapsed:.3f}s")
    return elapsed


def _summarize(samples):
    """Convert raw latency samples (seconds) into aggregate timing metrics."""
    print(f"[SUMMARY] Building stats for {len(samples)} samples")
    if not samples:
        return {
            "runs": 0,
            "avg_ms": 0.0,
            "min_ms": 0.0,
            "max_ms": 0.0,
            "p50_ms": 0.0,
            "p95_ms": 0.0,
            "throughput_ops_per_sec": 0.0,
        }

    samples_ms = [s * 1000.0 for s in samples]
    total_s = sum(samples)
    runs = len(samples)

    sorted_ms = sorted(samples_ms)
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


def get_backend_distribution(entity="main_records"):
    """Return current row/document distribution across SQL and Mongo for an entity."""
    print(f"[DIST] Refreshing connections to compute backend distribution for entity='{entity}'")
    crud_ops.refresh_connections()

    sql_count = 0
    mongo_count = 0

    if crud_ops.sql_available:
        model = crud_ops.sql_engine.models.get(entity)
        if model is not None:
            sql_count = crud_ops.sql_engine.session.query(model).count()

    if crud_ops.mongo_available:
        mongo_count = crud_ops.mongo_db[entity].count_documents({})

    total = sql_count + mongo_count
    distribution = {
        "sql_count": sql_count,
        "mongo_count": mongo_count,
        "sql_pct": round((sql_count / total) * 100.0, 2) if total > 0 else 0.0,
        "mongo_pct": round((mongo_count / total) * 100.0, 2) if total > 0 else 0.0,
        "combined_count": total,
    }
    print(f"[DIST] Distribution: {distribution}")
    return distribution


def benchmark_initialise_latency(counts=(200, 500), runs=3):
    """Benchmark initialise() latency for each requested count value."""
    print(f"[INIT BENCH] Starting initialise benchmark for counts={counts}, runs={runs}")
    results = {}
    for count in counts:
        print(f"[INIT BENCH] Evaluating count={count}")
        latencies = []
        records_per_sec = []

        for run_idx in range(1, runs + 1):
            print(f"[INIT BENCH] Run {run_idx}/{runs} for count={count}")
            elapsed_s = _measure_call(main.initialise, int(count))
            latencies.append(elapsed_s)
            records_per_sec.append((count / elapsed_s) if elapsed_s > 0 else 0.0)

        results[str(count)] = {
            "latency": _summarize(latencies),
            "records_per_sec_avg": round(statistics.mean(records_per_sec), 3),
            "distribution_after_run": get_backend_distribution(),
        }

    print("[INIT BENCH] Completed initialise benchmark")
    return results


def benchmark_fetch_latency(fetch_counts=(100, 250), runs=5, warmup_initialise_count=500):
    """Benchmark fetch() latency after a warmup initialise to ensure required state exists."""
    print(
        "[FETCH BENCH] Starting fetch benchmark "
        f"for fetch_counts={fetch_counts}, runs={runs}, warmup_initialise_count={warmup_initialise_count}"
    )
    # Ensure metadata/state exists
    print(f"[FETCH BENCH] Running warmup initialise({int(warmup_initialise_count)})")
    main.initialise(int(warmup_initialise_count))

    results = {}
    for count in fetch_counts:
        print(f"[FETCH BENCH] Evaluating fetch count={count}")
        latencies = []
        records_per_sec = []

        for run_idx in range(1, runs + 1):
            print(f"[FETCH BENCH] Run {run_idx}/{runs} for fetch count={count}")
            elapsed_s = _measure_call(main.fetch, int(count))
            latencies.append(elapsed_s)
            records_per_sec.append((count / elapsed_s) if elapsed_s > 0 else 0.0)

        results[str(count)] = {
            "latency": _summarize(latencies),
            "records_per_sec_avg": round(statistics.mean(records_per_sec), 3),
            "distribution_after_run": get_backend_distribution(),
        }

    print("[FETCH BENCH] Completed fetch benchmark")
    return results


def run_all():
    """Run all ingestion latency experiments and write a JSON report artifact."""
    print("[RUN ALL] Starting data ingestion latency experiment suite")
    report = {
        "experiment": "data_ingestion_latency",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "initialise": benchmark_initialise_latency(),
        "fetch": benchmark_fetch_latency(),
    }

    out_file = REPORT_DIR / f"data_ingestion_latency_{int(time.time())}.json"
    print(f"[RUN ALL] Writing report to {out_file}")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(json.dumps(report, indent=2))
    print(f"Saved report: {out_file}")
    print("[RUN ALL] Data ingestion latency experiment suite finished")


if __name__ == "__main__":
    run_all()