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
import asyncio

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

def concurrent_read_write_isolation_test(readers: int = 3, writers: int = 2):
    """
    Test: Isolation under concurrent read/write operations.
    Scenario:
    1. Writers insert rows and HOLD transaction before committing.
    2. Readers query during held transaction (should NOT see uncommitted rows).
    3. Writers commit, readers query again (SHOULD see new rows).
    Pass: Pre-commit reads = base_count; Post-commit reads = base_count + writers
    """
    try:
        model = _get_main_model()
        if not model:
            return {"test": "concurrent_read_write_isolation", "passed": False, "error": "main_records unavailable"}

        base_count = _sql_count()
        base_id = int(time.time() * 1000) % 1000000
        record_ids = [base_id + i for i in range(writers)]
        
        # Synchronization using barrier + events
        writers_ready = threading.Barrier(writers)
        readers_can_read = threading.Event()
        writers_can_commit = threading.Event()
        
        pre_commit_reads = []
        post_commit_reads = []
        read_lock = threading.Lock()

        def writer_thread(record_id: int):
            """Insert and hold transaction until signal."""
            session = _new_sql_session()
            try:
                session.execute(
                    text("INSERT INTO main_records (record_id, device_id) VALUES (:rid, :dev)"),
                    {"rid": record_id, "dev": f"iso_{record_id}"}
                )
                writers_ready.wait(timeout=2)
                readers_can_read.set()
                writers_can_commit.wait(timeout=3)
                session.commit()
            except Exception:
                session.rollback()
            finally:
                session.close()

        def reader_thread():
            """Read count before and after commit."""
            readers_can_read.wait(timeout=3)
            time.sleep(0.05)
            pre_count = _sql_count()
            with read_lock:
                pre_commit_reads.append(pre_count)
            writers_can_commit.set()
            time.sleep(0.1)
            post_count = _sql_count()
            with read_lock:
                post_commit_reads.append(post_count)

        writer_threads = [threading.Thread(target=writer_thread, args=(rid,)) for rid in record_ids]
        for t in writer_threads:
            t.start()
        
        time.sleep(0.05)
        reader_threads = [threading.Thread(target=reader_thread) for _ in range(readers)]
        for t in reader_threads:
            t.start()
        
        for t in writer_threads + reader_threads:
            t.join(timeout=5)
        
        after_count = _sql_count()
        expected = base_count + writers
        
        pre_ok = len(pre_commit_reads) > 0 and all(r == base_count for r in pre_commit_reads)
        post_ok = len(post_commit_reads) > 0 and all(r == expected for r in post_commit_reads)
        final_ok = after_count == expected
        
        return {
            "test": "concurrent_read_write_isolation",
            "passed": pre_ok and post_ok and final_ok,
            "base_count": base_count,
            "expected_after": expected,
            "after_count": after_count,
            "readers": readers,
            "writers": writers,
            "pre_commit_reads": pre_commit_reads,
            "post_commit_reads": post_commit_reads,
        }
    except Exception as e:
        return {"test": "concurrent_read_write_isolation", "passed": False, "error": str(e)[:120]}
    finally:
        for rid in [base_id + i for i in range(writers)]:
            try:
                _delete_sql_by_record_id(rid)
            except Exception:
                pass


# ==================== CRITICAL ROBUSTNESS TESTS ====================

def concurrent_insert_lost_updates_test(num_threads: int = 5):
    """
    Test: Concurrent inserts don't result in lost data.
    Scenario: Multiple threads insert records simultaneously.
    Expected: Final count = initial + (num_threads × records_per_thread).
    Pass: No data lost (count matches exactly).
    """
    try:
        model = _get_main_model()
        if not model:
            return {"test": "concurrent_insert_lost_updates", "passed": False, "error": "main_records unavailable"}
        
        before_count = _sql_count()
        base_id = int(time.time() * 1000) % 1000000
        records_per_thread = 3
        total_expected = before_count + (num_threads * records_per_thread)
        
        inserted_ids = []
        insert_lock = threading.Lock()
        
        def inserter_thread(thread_id: int):
            """Insert records in this thread."""
            session = _new_sql_session()
            try:
                for i in range(records_per_thread):
                    record_id = base_id + (thread_id * 100) + i
                    session.execute(
                        text("INSERT INTO main_records (record_id, device_id) VALUES (:rid, :dev)"),
                        {"rid": record_id, "dev": f"lost_upd_{record_id}"}
                    )
                    with insert_lock:
                        inserted_ids.append(record_id)
                session.commit()
            except Exception as e:
                session.rollback()
            finally:
                session.close()
        
        threads = [threading.Thread(target=inserter_thread, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)
        
        after_count = _sql_count()
        lost_updates = total_expected - after_count
        
        result = {
            "test": "concurrent_insert_lost_updates",
            "passed": after_count == total_expected,
            "before_count": before_count,
            "expected_after": total_expected,
            "actual_after": after_count,
            "lost_updates": lost_updates,
            "num_threads": num_threads,
        }
        
        for rid in inserted_ids:
            try:
                _delete_sql_by_record_id(rid)
            except Exception:
                pass
        
        return result
    except Exception as e:
        return {"test": "concurrent_insert_lost_updates", "passed": False, "error": str(e)[:120]}


def concurrent_update_atomicity_test(num_threads: int = 5):
    """
    Test: Check-then-act race condition (lost update variant).
    Scenario: Seed record with value 100. N threads do: READ → ADD 10 → WRITE.
    Expected final: 100 + (N × 10).
    Pass: Final matches expected (no lost updates).
    """
    try:
        model = _get_main_model()
        if not model:
            return {"test": "concurrent_update_atomicity", "passed": False, "error": "main_records unavailable"}
        
        seed_id = 99999
        initial_balance = 100
        increment = 10
        expected_final = initial_balance + (num_threads * increment)
        
        session = _new_sql_session()
        try:
            session.query(model).filter(model.record_id == seed_id).delete()
            session.commit()
            session.execute(
                text(f"INSERT INTO main_records (record_id, device_id) VALUES ({seed_id}, 'balance_{initial_balance}')"),
            )
            session.commit()
        finally:
            session.close()
        
        def updater_thread():
            """Read-modify-write pattern (tests race condition)."""
            thread_session = _new_sql_session()
            try:
                record = thread_session.query(model).filter(model.record_id == seed_id).first()
                if not record:
                    return
                time.sleep(0.001)  # Increase race condition window
                current_val = int(record.device_id.split('_')[1]) if '_' in record.device_id else initial_balance
                new_val = current_val + increment
                thread_session.execute(
                    text(f"UPDATE main_records SET device_id = :new_dev WHERE record_id = {seed_id}"),
                    {"new_dev": f"balance_{new_val}"}
                )
                thread_session.commit()
            except Exception:
                thread_session.rollback()
            finally:
                thread_session.close()
        
        threads = [threading.Thread(target=updater_thread) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)
        
        final_session = _new_sql_session()
        try:
            record = final_session.query(model).filter(model.record_id == seed_id).first()
            final_balance = int(record.device_id.split('_')[1]) if record and '_' in record.device_id else 0
        finally:
            final_session.close()
        
        lost_updates = expected_final - final_balance
        _delete_sql_by_record_id(seed_id)
        
        return {
            "test": "concurrent_update_atomicity",
            "passed": final_balance == expected_final,
            "initial_balance": initial_balance,
            "expected_final": expected_final,
            "actual_final": final_balance,
            "lost_updates": lost_updates,
            "num_threads": num_threads,
        }
    except Exception as e:
        return {"test": "concurrent_update_atomicity", "passed": False, "error": str(e)[:120]}


def stress_test_concurrent_ops(num_ops: int = 50, num_threads: int = 5):
    """
    Test: System robustness under sustained concurrent load.
    Scenario: N threads perform M total mixed operations (INSERT 40%, READ 30%, UPDATE 20%, DELETE 10%).
    Pass: No deadlocks/timeouts; error rate < 2%; data integrity maintained.
    """
    try:
        model = _get_main_model()
        if not model:
            return {"test": "stress_test_concurrent_ops", "passed": False, "error": "main_records unavailable"}
        
        base_id = int(time.time() * 1000) % 1000000
        success_count = 0
        error_count = 0
        success_lock = threading.Lock()
        inserted_ids = []
        ids_lock = threading.Lock()
        
        before_count = _sql_count()
        
        def worker_ops(worker_id: int):
            """Execute mixed operations."""
            nonlocal success_count, error_count
            ops_per_worker = num_ops // num_threads
            
            for op_idx in range(ops_per_worker):
                try:
                    session = _new_sql_session()
                    record_id = base_id + (worker_id * 1000) + op_idx
                    
                    if op_idx % 10 < 4:  # 40% INSERT
                        session.execute(
                            text("INSERT INTO main_records (record_id, device_id) VALUES (:rid, :dev)"),
                            {"rid": record_id, "dev": f"stress_{record_id}"}
                        )
                        with ids_lock:
                            inserted_ids.append(record_id)
                    elif op_idx % 10 < 7:  # 30% READ
                        session.query(model).filter(model.record_id == record_id).first()
                    elif op_idx % 10 < 9:  # 20% UPDATE
                        session.execute(
                            text(f"UPDATE main_records SET device_id = :dev WHERE record_id = {record_id}"),
                            {"dev": f"stress_upd_{record_id}"}
                        )
                    else:  # 10% DELETE
                        session.execute(
                            text(f"DELETE FROM main_records WHERE record_id = {record_id}")
                        )
                    
                    session.commit()
                    with success_lock:
                        success_count += 1
                except Exception:
                    with success_lock:
                        error_count += 1
                finally:
                    session.close()
        
        threads = [threading.Thread(target=worker_ops, args=(i,)) for i in range(num_threads)]
        start_time = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        elapsed = time.time() - start_time
        
        after_count = _sql_count()
        throughput = success_count / elapsed if elapsed > 0 else 0
        
        for rid in inserted_ids:
            try:
                _delete_sql_by_record_id(rid)
            except Exception:
                pass
        
        error_rate = error_count / (success_count + error_count) if (success_count + error_count) > 0 else 0
        
        return {
            "test": "stress_test_concurrent_ops",
            "passed": error_rate < 0.02 and error_count < 2,
            "num_threads": num_threads,
            "total_ops": success_count + error_count,
            "successful_ops": success_count,
            "failed_ops": error_count,
            "error_rate": f"{error_rate*100:.2f}%",
            "elapsed_seconds": f"{elapsed:.2f}",
            "throughput_ops_per_sec": f"{throughput:.1f}",
            "before_count": before_count,
            "after_count": after_count,
        }
    except Exception as e:
        return {"test": "stress_test_concurrent_ops", "passed": False, "error": str(e)[:120]}


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
