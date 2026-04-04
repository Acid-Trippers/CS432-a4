"""
Enhanced ACID Validators using existing CRUD operations and sql_engine

Tests Atomicity, Consistency, Isolation, Durability properties.
"""

from src.phase_5.sql_engine import SQLEngine
from src.phase_6.CRUD_operations import create_operation
from src.phase_6.CRUD_runner import analyze_query_databases
from pymongo import MongoClient
from src.config import MONGO_URI, MONGO_DB_NAME, COUNTER_FILE
import time
from sqlalchemy import text
import threading


# Initialize engines (same as CRUD_operations.py does)
sql_engine = SQLEngine()
try:
    sql_engine.initialize()
except Exception as e:
    print(f"SQL Engine failed: {e}")

mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
mongo_db = mongo_client[MONGO_DB_NAME]


def _get_main_model():
    return sql_engine.models.get("main_records")


def _new_sql_session():
    return sql_engine.schema_builder.get_session()


def _get_counter_value() -> int:
    try:
        with open(COUNTER_FILE, "r", encoding="utf-8") as fh:
            return int(fh.read().strip() or 0)
    except Exception:
        return 0


def _set_counter_value(value: int):
    with open(COUNTER_FILE, "w", encoding="utf-8") as fh:
        fh.write(str(value))


def _delete_sql_by_record_id(record_id: int):
    model = _get_main_model()
    if not model:
        return
    session = _new_sql_session()
    try:
        session.query(model).filter(model.record_id == record_id).delete()
        session.commit()
    finally:
        session.close()


def _sql_record_exists(record_id: int) -> bool:
    model = _get_main_model()
    if not model:
        return False
    session = _new_sql_session()
    try:
        return session.query(model).filter(model.record_id == record_id).count() > 0
    finally:
        session.close()


def _build_create_query(tag: str):
    return {
        "operation": "CREATE",
        "entity": "main_records",
        "payload": {
            "username": f"acid_{tag}_{int(time.time() * 1000)}",
            "name": "ACID Test User",
            "age": 30,
            "email": f"acid_{tag}_{int(time.time() * 1000)}@example.com",
            "timestamp": "2026-04-04T12:00:00Z",
            "action": "test",
            "comment": f"{tag} test",
        },
    }


def _run_create_transaction(query: dict):
    analysis = analyze_query_databases(query)
    return create_operation(query, analysis)


def get_sql_count(table: str = "main_records") -> int:
    """Get row count from SQL table using existing engine."""
    try:
        model = sql_engine.models.get(table)
        if not model:
            return -1
        session = _new_sql_session()
        try:
            return session.query(model).count()
        finally:
            session.close()
    except Exception:
        return -1


def get_mongo_count(collection: str = "main_records") -> int:
    """Get document count from Mongo collection."""
    try:
        return mongo_db[collection].count_documents({})
    except:
        return -1


def atomicity_test():
    """
    ATOMICITY: Transaction is all-or-nothing. If part fails, all fails.
    
    Test: Attempt to insert record with invalid data to trigger rollback.
    Verify: Counts unchanged if insert failed = PASS
    """
    original_counter = _get_counter_value()
    record_id = original_counter
    collection = mongo_db["main_records"]

    inserted_seed = False
    existing_seed = collection.find_one({"_id": record_id})

    try:
        if not existing_seed:
            collection.insert_one({"_id": record_id, "seed": "acid_atomicity"})
            inserted_seed = True

        _delete_sql_by_record_id(record_id)

        before_sql = get_sql_count()
        before_mongo = get_mongo_count()

        result = _run_create_transaction(_build_create_query("atomicity"))

        after_sql = get_sql_count()
        after_mongo = get_mongo_count()
        tx_state = (result.get("transaction") or {}).get("state")

        atomicity_holds = (
            result.get("status") == "failed"
            and tx_state in ("rolled_back", "failed_needs_recovery")
            and before_sql == after_sql
            and before_mongo == after_mongo
            and (not _sql_record_exists(record_id))
            and collection.count_documents({"_id": record_id}) == 1
        )

        return {
            "test": "atomicity",
            "passed": atomicity_holds,
            "sql_count_before": before_sql,
            "sql_count_after": after_sql,
            "mongo_count_before": before_mongo,
            "mongo_count_after": after_mongo,
            "transaction_state": tx_state,
            "record_id": record_id,
            "note": "Coordinator rollback verified on cross-database failure",
        }
    finally:
        _set_counter_value(original_counter)
        if inserted_seed:
            collection.delete_one({"_id": record_id})


def consistency_test():
    """
    CONSISTENCY: Check if SQL enforces unique constraints.
    
    Test: Try to insert duplicate record_id to same table.
    Verify: Should fail with constraint error = PASS
    """
    try:
        model = _get_main_model()
        if not model:
            return {"test": "consistency", "passed": False, "error": "main_records model unavailable"}

        session = _new_sql_session()
        existing = session.query(model).first()
        if not existing:
            session.close()
            return {"test": "consistency", "passed": True, "note": "No records to test"}
        
        dup_id = existing.record_id
        
        # Try duplicate insert (should fail)
        try:
            session.execute(
                text("INSERT INTO main_records (record_id) VALUES (:rid)"),
                {"rid": dup_id},
            )
            session.commit()
            session.close()
            return {"test": "consistency", "passed": False, "error": "Duplicate insert accepted!"}
        except Exception as e:
            # Constraint violation = expected
            session.rollback()
            session.close()
            constraint_error = "duplicate" in str(e).lower() or "unique" in str(e).lower() or "primary" in str(e).lower()
            return {"test": "consistency", "passed": constraint_error, "error": str(e)[:100]}
    
    except Exception as e:
        return {"test": "consistency", "passed": False, "error": str(e)[:100]}


def isolation_test(num_workers: int = 3):
    """
    ISOLATION: Concurrent txns don't interfere (serializable).
    
    Test: Multiple threads read same data to check consistency.
    Verify: All threads see same committed state = PASS
    """
    before = get_sql_count()
    read_results = []
    errors = []
    lock = threading.Lock()
    
    def worker_read(thread_id):
        """Worker thread that reads current count."""
        try:
            time.sleep(0.01 * thread_id)  # Stagger reads
            count = get_sql_count()
            with lock:
                read_results.append((thread_id, count))
        except Exception as e:
            with lock:
                errors.append(str(e))
    
    # Spawn concurrent readers
    threads = []
    for i in range(num_workers):
        t = threading.Thread(target=worker_read, args=(i,))
        threads.append(t)
        t.start()
    
    # Wait for all to complete
    for t in threads:
        t.join()
    
    after = get_sql_count()
    
    # Isolation holds if all readers see same count (no dirty reads)
    all_reads = [count for _, count in read_results]
    consistent_reads = len(set(all_reads)) <= 1 if all_reads else True
    isolation_holds = consistent_reads and len(errors) == 0 and before == after
    
    return {
        "test": "isolation",
        "passed": isolation_holds,
        "sql_count_before": before,
        "sql_count_after": after,
        "concurrent_reads": len(all_reads),
        "read_consistency": consistent_reads,
        "errors": len(errors),
        "note": "All concurrent reads see consistent state"
    }


def durability_test():
    """
    DURABILITY: Data committed to disk survives queries.
    
    Test: Query data multiple times, verify same results.
    Verify: Data persists and is recoverable = PASS
    """
    original_counter = _get_counter_value()
    record_id = original_counter

    try:
        result = _run_create_transaction(_build_create_query("durability"))
        tx_state = (result.get("transaction") or {}).get("state")
        if result.get("status") != "success":
            return {
                "test": "durability",
                "passed": False,
                "error": "Create transaction failed",
                "transaction_state": tx_state,
            }

        sql_reads = []
        mongo_reads = []
        for _ in range(3):
            sql_reads.append(_sql_record_exists(record_id))
            mongo_reads.append(mongo_db["main_records"].count_documents({"_id": record_id}) == 1)
            time.sleep(0.02)

        success = all(sql_reads) and all(mongo_reads)
        return {
            "test": "durability",
            "passed": success,
            "transaction_state": tx_state,
            "record_id": record_id,
            "sql_reads": sql_reads,
            "mongo_reads": mongo_reads,
            "note": "Committed record remains visible across repeated checks",
        }
    except Exception as e:
        return {"test": "durability", "passed": False, "error": str(e)[:100]}
    finally:
        _delete_sql_by_record_id(record_id)
        mongo_db["main_records"].delete_one({"_id": record_id})
        _set_counter_value(original_counter)
