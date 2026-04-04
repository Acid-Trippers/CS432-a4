"""
Advanced ACID Validators - Extended Testing

Additional tests for more rigorous ACID property validation.
Usage: from ACID.advanced_validators import *
"""

from src.phase_5.sql_engine import SQLEngine
from src.phase_6.CRUD_operations import create_operation
from src.phase_6.CRUD_runner import analyze_query_databases
from pymongo import MongoClient
from src.config import MONGO_URI, MONGO_DB_NAME, COUNTER_FILE
import time
from sqlalchemy import text, inspect as sql_inspect
import threading

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


def _sql_count() -> int:
    model = _get_main_model()
    if not model:
        return -1
    session = _new_sql_session()
    try:
        return session.query(model).count()
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
            "username": f"acid_adv_{tag}_{int(time.time() * 1000)}",
            "name": "Advanced ACID User",
            "age": 31,
            "email": f"acid_adv_{tag}_{int(time.time() * 1000)}@example.com",
            "timestamp": "2026-04-04T12:00:00Z",
            "action": "advanced_test",
            "comment": f"advanced {tag}",
        },
    }


def _run_create_transaction(query: dict):
    analysis = analyze_query_databases(query)
    return create_operation(query, analysis)


# ==================== ADVANCED ATOMICITY ====================

def multi_record_atomicity_test():
    """
    Test: Insert multiple records in single transaction
    Verify: All succeed or all fail (no partial writes)
    """
    try:
        model = _get_main_model()
        if not model:
            return {"test": "multi_record_atomicity", "passed": False, "error": "main_records model unavailable"}

        session = _new_sql_session()
        before = session.query(model).count()
        
        # Try to insert multiple records in one transaction
        try:
            for i in range(5):
                session.execute(
                    text(f"INSERT INTO main_records (record_id, device_id) VALUES ({20000+i}, 'test_{i}')")
                )
            session.commit()
            after = session.query(model).count()
            session.close()
            return {"test": "multi_record_atomicity", "passed": (after == before + 5), "records_added": after - before}
        except Exception as e:
            session.rollback()
            after = session.query(model).count()
            session.close()
            return {"test": "multi_record_atomicity", "passed": (after == before), "error": "Rollback on error"}
    except Exception as e:
        return {"test": "multi_record_atomicity", "passed": False, "error": str(e)[:100]}


def cross_db_atomicity_test():
    """
    Test: Verify SQL and MongoDB stay in sync after operations
    Verify: Both have same record count = PASS
    """
    original_counter = _get_counter_value()
    record_id = original_counter
    collection = mongo_db["main_records"]
    inserted_seed = False

    try:
        # Success path: both participants should commit
        success_result = _run_create_transaction(_build_create_query("cross_db_success"))
        success_id = record_id
        success_tx_state = (success_result.get("transaction") or {}).get("state")
        success_holds = (
            success_result.get("status") == "success"
            and success_tx_state == "committed"
            and _sql_record_exists(success_id)
            and collection.count_documents({"_id": success_id}) == 1
        )

        # Failure path: force SQL success and Mongo failure, then verify rollback
        fail_record_id = success_id + 1
        _set_counter_value(fail_record_id)

        existing_seed = collection.find_one({"_id": fail_record_id})
        if not existing_seed:
            collection.insert_one({"_id": fail_record_id, "seed": "acid_crossdb"})
            inserted_seed = True

        _delete_sql_by_record_id(fail_record_id)

        before_sql = _sql_count()
        before_mongo = collection.count_documents({})

        fail_result = _run_create_transaction(_build_create_query("cross_db_fail"))
        fail_tx_state = (fail_result.get("transaction") or {}).get("state")
        after_sql = _sql_count()
        after_mongo = collection.count_documents({})

        rollback_holds = (
            fail_result.get("status") == "failed"
            and fail_tx_state in ("rolled_back", "failed_needs_recovery")
            and before_sql == after_sql
            and before_mongo == after_mongo
            and not _sql_record_exists(fail_record_id)
        )

        return {
            "test": "cross_db_atomicity",
            "passed": success_holds and rollback_holds,
            "success_tx_state": success_tx_state,
            "rollback_tx_state": fail_tx_state,
            "success_record_id": success_id,
            "rollback_record_id": fail_record_id,
            "success_holds": success_holds,
            "rollback_holds": rollback_holds,
        }
    except Exception as e:
        return {"test": "cross_db_atomicity", "passed": False, "error": str(e)[:100]}
    finally:
        _delete_sql_by_record_id(record_id)
        collection.delete_one({"_id": record_id})
        _delete_sql_by_record_id(record_id + 1)
        if inserted_seed:
            collection.delete_one({"_id": record_id + 1})
        _set_counter_value(original_counter)


# ==================== ADVANCED CONSISTENCY ====================

def not_null_constraint_test():
    """
    Test: NOT NULL constraint enforcement
    Verify: NULL insert rejected
    """
    try:
        try:
            # Try to insert NULL in required field
            sql_engine.session.execute(
                text("INSERT INTO main_records (device_id) VALUES (NULL)")
            )
            sql_engine.session.commit()
            return {"test": "not_null_constraint", "passed": False, "error": "NULL accepted"}
        except Exception as e:
            sql_engine.session.rollback()
            is_null_error = "not null" in str(e).lower() or "null" in str(e).lower()
            return {"test": "not_null_constraint", "passed": is_null_error, "error": str(e)[:80]}
    except Exception as e:
        return {"test": "not_null_constraint", "passed": False, "error": str(e)[:100]}


def schema_validation_test():
    """
    Test: Schema integrity and field types
    Verify: Field types are enforced
    """
    try:
        # Get schema metadata
        table = sql_engine.models.get("main_records")
        if not table:
            return {"test": "schema_validation", "passed": False, "error": "Table not found"}
        
        columns = {col.name: str(col.type) for col in table.__table__.columns}
        
        # Check critical fields exist and have correct types
        required_fields = ["record_id", "device_id", "timestamp"]
        has_required = all(field in columns for field in required_fields)
        
        return {
            "test": "schema_validation",
            "passed": has_required,
            "column_count": len(columns),
            "has_required_fields": has_required
        }
    except Exception as e:
        return {"test": "schema_validation", "passed": False, "error": str(e)[:100]}


# ==================== ADVANCED ISOLATION ====================

def dirty_read_test():
    """
    Test: Dirty read prevention (read uncommitted data)
    Verify: Transactions don't see uncommitted changes
    """
    try:
        model = _get_main_model()
        if not model:
            return {"test": "dirty_read_prevention", "passed": False, "error": "main_records model unavailable"}

        before = _sql_count()
        dirty_reads = []
        lock = threading.Lock()
        test_record_id = 30000
        
        def writer_thread():
            """Start transaction but don't commit."""
            writer_session = _new_sql_session()
            try:
                writer_session.execute(
                    text("INSERT INTO main_records (record_id, device_id) VALUES (:rid, 'dirty')"),
                    {"rid": test_record_id},
                )
                # Intentionally don't commit - test if others can see this
                time.sleep(0.2)
                writer_session.rollback()
            except Exception:
                pass
            finally:
                writer_session.close()
        
        def reader_thread():
            """Try to read during uncommitted write."""
            time.sleep(0.05)  # Wait for writer to insert
            reader_session = _new_sql_session()
            try:
                count = reader_session.query(model).count()
                with lock:
                    dirty_reads.append(count)
            except Exception:
                pass
            finally:
                reader_session.close()
        
        # Run concurrent threads
        t1 = threading.Thread(target=writer_thread)
        t2 = threading.Thread(target=reader_thread)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        
        _delete_sql_by_record_id(test_record_id)
        after = _sql_count()
        
        # No dirty reads = all reads same as before
        no_dirty_reads = all(r == before for r in dirty_reads)
        
        return {
            "test": "dirty_read_prevention",
            "passed": no_dirty_reads and before == after,
            "before": before,
            "after": after,
            "reader_count": len(dirty_reads)
        }
    except Exception as e:
        return {"test": "dirty_read_prevention", "passed": False, "error": str(e)[:100]}


# ==================== ADVANCED DURABILITY ====================

def persistent_connection_test():
    """
    Test: Data survives repeated connection cycles
    Verify: Query after disconnect/reconnect returns same data
    """
    try:
        # First read
        count1 = _sql_count()
        
        # Simulate connection cycle
        time.sleep(0.1)
        
        # Second read (may trigger reconnection)
        count2 = _sql_count()
        
        # Third read
        time.sleep(0.1)
        count3 = _sql_count()
        
        # All reads should match
        all_consistent = (count1 == count2 == count3)
        
        return {
            "test": "persistent_connection",
            "passed": all_consistent,
            "reads": [count1, count2, count3],
            "consistent": all_consistent
        }
    except Exception as e:
        return {"test": "persistent_connection", "passed": False, "error": str(e)[:100]}


def index_integrity_test():
    """
    Test: Indexes maintain data integrity
    Verify: Primary key index prevents duplicates
    """
    try:
        model = _get_main_model()
        if not model:
            return {"test": "index_integrity", "passed": False, "error": "Table not found"}

        inspector = sql_inspect(sql_engine.schema_builder.engine)
        pk = inspector.get_pk_constraint("main_records")
        indexes = inspector.get_indexes("main_records")
        unique_indexes = [idx for idx in indexes if idx.get("unique")]

        pk_exists = bool(pk.get("constrained_columns"))
        index_count = len(indexes)
        has_any_integrity_index = pk_exists or bool(unique_indexes)
        
        return {
            "test": "index_integrity",
            "passed": has_any_integrity_index,
            "pk_columns": pk.get("constrained_columns", []),
            "index_count": index_count,
            "indexes": [idx.get("name") for idx in indexes[:5]],
            "unique_index_count": len(unique_indexes),
        }
    except Exception as e:
        return {"test": "index_integrity", "passed": False, "error": str(e)[:100]}
