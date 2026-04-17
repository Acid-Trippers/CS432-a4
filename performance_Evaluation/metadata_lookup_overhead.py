import json
import random
import statistics
import time
from datetime import datetime
from pathlib import Path

from src.config import METADATA_FILE
from src.phase_6.CRUD_runner import get_field_locations


REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "performance_reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)


def _summarize_ms(samples_ms):
    """Aggregate millisecond timings into summary statistics and throughput."""
    print(f"[SUMMARY] Building metadata stats for {len(samples_ms)} samples")
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
        "avg_ms": round(statistics.mean(samples_ms), 4),
        "min_ms": round(min(samples_ms), 4),
        "max_ms": round(max(samples_ms), 4),
        "p50_ms": round(p50, 4),
        "p95_ms": round(p95, 4),
        "throughput_ops_per_sec": round(runs / total_s, 3) if total_s > 0 else 0.0,
    }


def benchmark_metadata_file_read(runs=100):
    """Measure latency for opening and reading metadata.json content from disk."""
    print(f"[BENCH FILE READ] Starting metadata file-read benchmark with runs={runs}")

    # FIX: Validate the metadata file exists before benchmarking; return a
    # zero-run summary with an explanatory note instead of crashing with
    # FileNotFoundError / PermissionError which would propagate as a 500.
    metadata_path = Path(METADATA_FILE)
    if not metadata_path.exists():
        print(f"[BENCH FILE READ] METADATA_FILE not found at {METADATA_FILE}, skipping")
        result = _summarize_ms([])
        result["skipped_reason"] = f"METADATA_FILE not found: {METADATA_FILE}"
        return result

    samples_ms = []
    errors = 0
    for run_idx in range(1, runs + 1):
        try:
            t0 = time.perf_counter()
            with open(METADATA_FILE, "r", encoding="utf-8") as f:
                _ = f.read()
            t1 = time.perf_counter()
            samples_ms.append((t1 - t0) * 1000.0)
        except Exception as exc:
            errors += 1
            print(f"[BENCH FILE READ] Run {run_idx} failed: {exc}")

        if run_idx == 1 or run_idx == runs or run_idx % max(1, runs // 5) == 0:
            print(f"[BENCH FILE READ] Progress: {run_idx}/{runs}")

    result = _summarize_ms(samples_ms)
    result["errors"] = errors
    return result


def benchmark_metadata_parse(runs=100):
    """Measure latency for parsing already-read metadata JSON content."""
    print(f"[BENCH PARSE] Starting metadata parse benchmark with runs={runs}")

    # FIX: Guard against missing/unreadable file; return zero summary instead
    # of letting an unhandled exception surface as a 500.
    metadata_path = Path(METADATA_FILE)
    if not metadata_path.exists():
        print(f"[BENCH PARSE] METADATA_FILE not found at {METADATA_FILE}, skipping")
        result = _summarize_ms([])
        result["skipped_reason"] = f"METADATA_FILE not found: {METADATA_FILE}"
        return result

    try:
        with open(METADATA_FILE, "r", encoding="utf-8") as f:
            raw = f.read()
    except Exception as exc:
        print(f"[BENCH PARSE] Could not read METADATA_FILE: {exc}")
        result = _summarize_ms([])
        result["skipped_reason"] = str(exc)
        return result

    samples_ms = []
    errors = 0
    for run_idx in range(1, runs + 1):
        try:
            t0 = time.perf_counter()
            _ = json.loads(raw)
            t1 = time.perf_counter()
            samples_ms.append((t1 - t0) * 1000.0)
        except Exception as exc:
            errors += 1
            print(f"[BENCH PARSE] Run {run_idx} failed: {exc}")

        if run_idx == 1 or run_idx == runs or run_idx % max(1, runs // 5) == 0:
            print(f"[BENCH PARSE] Progress: {run_idx}/{runs}")

    result = _summarize_ms(samples_ms)
    result["errors"] = errors
    return result


def benchmark_field_lookup(runs=1000):
    """Measure in-memory field location lookup latency on the metadata map."""
    print(f"[BENCH LOOKUP] Starting field-lookup benchmark with runs={runs}")

    # FIX: get_field_locations() may return None or raise; guard both cases so
    # the benchmark degrades gracefully instead of crashing the entire suite.
    try:
        locations = get_field_locations() or {}
    except Exception as exc:
        print(f"[BENCH LOOKUP] get_field_locations() raised: {exc}")
        result = _summarize_ms([])
        result["skipped_reason"] = str(exc)
        result["unique_fields"] = 0
        return result

    # FIX: Original code used `locations.keys() or ["record_id"]` which always
    # evaluates truthy on a dict_keys object even when empty.  Use an explicit
    # length check so we fall back to the sentinel key correctly.
    keys = list(locations.keys()) if locations else ["record_id"]
    print(f"[BENCH LOOKUP] Loaded {len(keys)} candidate field keys")

    samples_ms = []
    errors = 0
    for run_idx in range(1, runs + 1):
        field = random.choice(keys)
        try:
            t0 = time.perf_counter()
            _ = locations.get(field, "Unknown")
            t1 = time.perf_counter()
            samples_ms.append((t1 - t0) * 1000.0)
        except Exception as exc:
            errors += 1
            print(f"[BENCH LOOKUP] Run {run_idx} failed: {exc}")

        if run_idx == 1 or run_idx == runs or run_idx % max(1, runs // 5) == 0:
            print(f"[BENCH LOOKUP] Progress: {run_idx}/{runs}")

    result = _summarize_ms(samples_ms)
    result["errors"] = errors
    result["unique_fields"] = len(keys)
    print(f"[BENCH LOOKUP] Completed with result: {result}")
    return result


def benchmark_end_to_end_metadata_path(runs=200):
    """Measure end-to-end latency of calling get_field_locations repeatedly."""
    print(f"[BENCH E2E] Starting end-to-end metadata path benchmark with runs={runs}")

    # FIX: The original version had no error handling; a single exception from
    # get_field_locations() (e.g. missing file, bad JSON, import error) would
    # abort the loop and propagate uncaught all the way to the API layer,
    # producing the observed 500.  Each run is now individually try/caught so
    # partial results are always returned.
    samples_ms = []
    errors = 0
    for run_idx in range(1, runs + 1):
        try:
            t0 = time.perf_counter()
            _ = get_field_locations()
            t1 = time.perf_counter()
            samples_ms.append((t1 - t0) * 1000.0)
        except Exception as exc:
            errors += 1
            print(f"[BENCH E2E] Run {run_idx} failed: {exc}")
            # After 5 consecutive failures abort early to avoid a long hang.
            if errors >= 5 and not samples_ms:
                print("[BENCH E2E] Too many consecutive failures, aborting early")
                result = _summarize_ms([])
                result["errors"] = errors
                result["skipped_reason"] = f"Aborted after {errors} failures: {exc}"
                return result

        if run_idx == 1 or run_idx == runs or run_idx % max(1, runs // 5) == 0:
            print(f"[BENCH E2E] Progress: {run_idx}/{runs}")

    result = _summarize_ms(samples_ms)
    result["errors"] = errors
    return result


def run_all():
    """Run all metadata overhead benchmarks and save one consolidated JSON report."""
    print("[RUN ALL] Starting metadata lookup overhead experiment suite")

    # FIX: Wrap each sub-benchmark individually so one failing benchmark cannot
    # crash run_all() and cause the API to return 500.  Failed benchmarks are
    # represented as {"error": "<message>"} entries in the report instead.
    def _safe_run(name, fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            print(f"[RUN ALL] Benchmark '{name}' raised: {exc}")
            return {"error": str(exc), "runs": 0}

    report = {
        "experiment": "metadata_lookup_overhead",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "file_read": _safe_run("file_read", benchmark_metadata_file_read),
        "json_parse": _safe_run("json_parse", benchmark_metadata_parse),
        "field_lookup": _safe_run("field_lookup", benchmark_field_lookup),
        "end_to_end_get_field_locations": _safe_run(
            "end_to_end_get_field_locations", benchmark_end_to_end_metadata_path
        ),
    }

    out_file = REPORT_DIR / f"metadata_lookup_overhead_{int(time.time())}.json"
    print(f"[RUN ALL] Writing report to {out_file}")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(json.dumps(report, indent=2))
    print(f"Saved report: {out_file}")
    print("[RUN ALL] Metadata lookup overhead experiment suite finished")
    return report


if __name__ == "__main__":
    run_all()