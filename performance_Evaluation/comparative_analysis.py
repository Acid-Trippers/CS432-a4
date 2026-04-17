import time
import statistics
import json
import sys
from pathlib import Path

# Force Python to look at the parent directory (CS432-a4) to find 'src'
root_dir = Path(__file__).resolve().parents[1]
sys.path.append(str(root_dir))

from src.phase_6 import CRUD_operations as crud_ops
from src.phase_6.CRUD_runner import query_runner

report_dir = root_dir / "data" / "performance_reports"
report_dir.mkdir(parents=True, exist_ok=True)

def measure_execution_time(func, *args, runs=10, **kwargs):
    """Executes a function multiple times and returns the average latency in ms."""
    latencies = []
    for _ in range(runs):
        t0 = time.perf_counter()
        func(*args, **kwargs)
        t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1000.0)
    return {
        "avg_ms": round(statistics.mean(latencies), 3),
        "min_ms": round(min(latencies), 3),
        "max_ms": round(max(latencies), 3)
    }

def run_sql_comparison(runs=10):
    """Compares Logical vs Direct SQL for retrieving user records."""
    print("Running SQL Comparison (User Records)...")
    crud_ops.refresh_connections()
    
    # 1. Logical Query
    logical_payload = {
        "operation": "READ",
        "entity": "main_records", # Assuming main_records maps to SQL
        "filters": {"subscription": "free"},
        "columns": ["username", "city"]
    }
    
    def logical_sql_query():
        return query_runner(query_dict=logical_payload)
        
    # 2. Direct SQL Query
    def direct_sql_query():
        model = crud_ops.sql_engine.models.get("main_records")
        if model:
            results = crud_ops.sql_engine.session.query(model.username, model.city).filter(model.subscription == "free").all()
            return results

    logical_stats = measure_execution_time(logical_sql_query, runs=runs)
    direct_stats = measure_execution_time(direct_sql_query, runs=runs)
    
    overhead_ms = logical_stats["avg_ms"] - direct_stats["avg_ms"]
    
    return {
        "logical_latency": logical_stats,
        "direct_latency": direct_stats,
        "framework_overhead_ms": round(overhead_ms, 3)
    }

def run_mongo_comparison(runs=10):
    """Compares Logical vs Direct MongoDB for accessing nested documents."""
    print("Running MongoDB Comparison (Nested Documents)...")
    crud_ops.refresh_connections()
    
    # 1. Logical Query
    logical_payload = {
        "operation": "READ",
        "entity": "user_profiles", # Assuming profiles have nested docs in Mongo
        "filters": {"settings.theme": "dark"},
        "columns": ["username", "settings"]
    }
    
    def logical_mongo_query():
        return query_runner(query_dict=logical_payload)
        
    # 2. Direct Mongo Query
    def direct_mongo_query():
        results = list(crud_ops.mongo_db["user_profiles"].find(
            {"settings.theme": "dark"},
            {"username": 1, "settings": 1, "_id": 0}
        ))
        return results

    logical_stats = measure_execution_time(logical_mongo_query, runs=runs)
    direct_stats = measure_execution_time(direct_mongo_query, runs=runs)
    
    overhead_ms = logical_stats["avg_ms"] - direct_stats["avg_ms"]
    
    return {
        "logical_latency": logical_stats,
        "direct_latency": direct_stats,
        "framework_overhead_ms": round(overhead_ms, 3)
    }

def run_cross_entity_update_comparison(runs=5, custom_payload=None):
    """Compares Logical vs Direct updates across multiple entities."""
    print("Running Cross-Entity Update Comparison...")
    
    # Setup Default vs Custom Payloads
    default_create = {
        "username": "comparative_test_user",
        "city": "test_city",
        "subscription": "free"
    }
    default_update = {"subscription": "premium"}
    
    if custom_payload and isinstance(custom_payload, dict):
        create_data = custom_payload.get("create_payload", default_create)
        update_data = custom_payload.get("update_payload", default_update)
    else:
        create_data = default_create
        update_data = default_update
    
    def setup_temp_record():
        payload = {
            "operation": "CREATE",
            "entity": "main_records",
            "payload": create_data
        }
        crud_ops.refresh_connections()
        result = query_runner(query_dict=payload)
        return result.get("details", {}).get("record_id")

    def cleanup_temp_record(record_id):
        model = crud_ops.sql_engine.models.get("main_records")
        if model:
            sql_del = crud_ops.sql_engine.session.query(model).filter(model.record_id == record_id).delete(synchronize_session=False)
            crud_ops.sql_engine.session.commit()
        mongo_del = crud_ops.mongo_db["main_records"].delete_many({"record_id": record_id}).deleted_count

    def logical_update(target_id):
        payload = {
            "operation": "UPDATE",
            "entity": "main_records",
            "filters": {"record_id": target_id},
            "payload": update_data
        }
        crud_ops.refresh_connections()
        return query_runner(query_dict=payload)
        
    def direct_update(target_id):
        crud_ops.refresh_connections()
        model = crud_ops.sql_engine.models.get("main_records")
        if model:
            crud_ops.sql_engine.session.query(model).filter(model.record_id == target_id).update(update_data)
            crud_ops.sql_engine.session.commit()
            
        crud_ops.mongo_db["main_records"].update_many(
            {"record_id": target_id},
            {"$set": update_data}
        )

    # --- Run Logical Benchmark ---
    logical_latencies = []
    for _ in range(runs):
        target_id = setup_temp_record()
        if not target_id: continue
        t0 = time.perf_counter()
        logical_update(target_id)
        t1 = time.perf_counter()
        logical_latencies.append((t1 - t0) * 1000.0)
        cleanup_temp_record(target_id)
        
    # --- Run Direct Benchmark ---
    direct_latencies = []
    for _ in range(runs):
        target_id = setup_temp_record()
        if not target_id: continue
        t0 = time.perf_counter()
        direct_update(target_id)
        t1 = time.perf_counter()
        direct_latencies.append((t1 - t0) * 1000.0)
        cleanup_temp_record(target_id)

    import statistics
    logical_avg = round(statistics.mean(logical_latencies), 3) if logical_latencies else 0
    direct_avg = round(statistics.mean(direct_latencies), 3) if direct_latencies else 0

    return {
        "logical_latency": {"avg_ms": logical_avg, "min_ms": round(min(logical_latencies), 3) if logical_latencies else 0, "max_ms": round(max(logical_latencies), 3) if logical_latencies else 0},
        "direct_latency": {"avg_ms": direct_avg, "min_ms": round(min(direct_latencies), 3) if direct_latencies else 0, "max_ms": round(max(direct_latencies), 3) if direct_latencies else 0},
        "framework_overhead_ms": round(logical_avg - direct_avg, 3)
    }

def execute_comparative_analysis(custom_payload=None):
    print("Starting Comparative Analysis...")
    report = {
        "sql_comparison": run_sql_comparison(runs=20),
        "mongo_comparison": run_mongo_comparison(runs=20),
        "update_comparison": run_cross_entity_update_comparison(runs=10, custom_payload=custom_payload)
    }
    
    out_file = report_dir / f"comparative_analysis_{int(time.time())}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
        
    return report

if __name__ == "__main__":
    execute_comparative_analysis()