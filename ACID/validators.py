"""Minimal ACID validators focused on core atomicity tests."""

import time
from sqlalchemy import text
from pymongo import MongoClient

from src.config import COUNTER_FILE, MONGO_DB_NAME, MONGO_URI
from src.phase_5.sql_engine import SQLEngine
from src.phase_6.CRUD_operations import create_operation
from src.phase_6.CRUD_runner import analyze_query_databases


sql_engine = SQLEngine()
sql_engine.initialize()

mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
mongo_db = mongo_client[MONGO_DB_NAME]


def _session():
    return sql_engine.schema_builder.get_session()


def _model():
    return sql_engine.models.get("main_records")


def _get_counter() -> int:
    try:
        with open(COUNTER_FILE, "r", encoding="utf-8") as fh:
            return int(fh.read().strip() or 0)
    except Exception:
        return 0


def _set_counter(value: int):
    with open(COUNTER_FILE, "w", encoding="utf-8") as fh:
        fh.write(str(value))


def _sql_count() -> int:
    model = _model()
    if not model:
        return -1
    s = _session()
    try:
        return s.query(model).count()
    finally:
        s.close()


def _mongo_count() -> int:
    return mongo_db["main_records"].count_documents({})


def _sql_exists(record_id: int) -> bool:
    model = _model()
    if not model:
        return False
    s = _session()
    try:
        return s.query(model).filter(model.record_id == record_id).count() > 0
    finally:
        s.close()


def _mongo_exists(record_id: int) -> bool:
    return mongo_db["main_records"].count_documents({"$or": [{"_id": record_id}, {"record_id": record_id}]}) > 0


def _cleanup(record_id: int):
    model = _model()
    if model:
        s = _session()
        try:
            s.query(model).filter(model.record_id == record_id).delete()
            s.commit()
        finally:
            s.close()
    mongo_db["main_records"].delete_many({"$or": [{"_id": record_id}, {"record_id": record_id}]})


def _run_create(tag: str):
    query = {
        "operation": "CREATE",
        "entity": "main_records",
        "payload": {
            "username": f"acid_{tag}_{int(time.time() * 1000)}",
            "name": "ACID Test User",
            "age": 30,
            "email": f"acid_{tag}_{int(time.time() * 1000)}@example.com",
            "timestamp": "2026-04-04T12:00:00Z",
            "action": "test",
            "comment": tag,
        },
    }
    analysis = analyze_query_databases(query)
    return create_operation(query, analysis)


def _seed_mongo_conflict_doc(record_id: int, tag: str):
    mongo_db["main_records"].insert_one(
        {
            "_id": record_id,
            "record_id": record_id,
            "username": f"seed_{tag}_{record_id}",
            "timestamp": "2026-04-04T12:00:00Z",
            "action": "test",
            "comment": f"seed:{tag}",
        }
    )


def atomicity_test():
    """Atomicity test 1: single logical write commits to both stores or rolls back everywhere."""
    original_counter = _get_counter()
    seed_id = original_counter
    success_id = None

    try:
        _cleanup(seed_id)
        _set_counter(original_counter)
        success = _run_create("atomicity_success")
        success_id = (success.get("details") or {}).get("record_id")
        success_sql_exists = _sql_exists(success_id) if success_id is not None else False
        success_mongo_exists = _mongo_exists(success_id) if success_id is not None else False
        success_ok = (
            success.get("status") == "success"
            and success_id is not None
            and success_sql_exists
            and success_mongo_exists
        )

        if success_id is not None:
            _cleanup(success_id)
        _set_counter(original_counter)

        _cleanup(seed_id)
        _seed_mongo_conflict_doc(seed_id, "atomicity")
        before_sql = _sql_count()
        before_mongo = _mongo_count()

        failed = _run_create("atomicity_fail")
        tx_state = (failed.get("transaction") or {}).get("state")

        after_sql = _sql_count()
        after_mongo = _mongo_count()
        failure_sql_exists = _sql_exists(seed_id)
        failure_seed_doc_exists = _mongo_exists(seed_id)
        rollback_ok = (
            failed.get("status") == "failed"
            and tx_state in ("rolled_back", "failed_needs_recovery")
            and before_sql == after_sql
            and before_mongo == after_mongo
            and not failure_sql_exists
            and failure_seed_doc_exists
        )

        passed = success_ok and rollback_ok

        return {
            "test": "atomicity",
            "passed": passed,
            "success_holds": success_ok,
            "rollback_holds": rollback_ok,
            "tests_run": [
                "success_path_commit_sql_and_mongo",
                "failure_path_rollback_after_forced_mongo_conflict",
            ],
            "decision_metrics": {
                "success_status": success.get("status"),
                "success_record_id": success_id,
                "success_sql_exists": success_sql_exists,
                "success_mongo_exists": success_mongo_exists,
                "failure_tx_state": tx_state,
                "sql_count_before_after": f"{before_sql} -> {after_sql}",
                "mongo_count_before_after": f"{before_mongo} -> {after_mongo}",
                "failure_sql_exists": failure_sql_exists,
                "failure_mongo_seed_exists": failure_seed_doc_exists,
                "note": "Mongo count includes one intentionally seeded conflict document during failure phase",
            },
            "decision_rule": "PASS iff success_holds AND rollback_holds",
        }
    finally:
        if success_id is not None:
            _cleanup(success_id)
        _cleanup(seed_id)
        _set_counter(original_counter)


def multi_record_atomicity_test():
    """Atomicity test 2: multi-row SQL transaction fails as a whole with no partial commit."""
    model = _model()
    if not model:
        return {"test": "multi_record_atomicity", "passed": False, "error": "main_records model unavailable"}

    base_id = int(time.time() * 1000) % 100000000
    record_ids = [base_id + i for i in range(5)]
    record_ids[2] = record_ids[0]  # duplicate inside same transaction to force rollback
    unique_record_ids = set(record_ids)

    before = _sql_count()
    s = _session()
    try:
        for record_id in record_ids:
            s.execute(
                text("INSERT INTO main_records (record_id) VALUES (:record_id)"),
                {"record_id": record_id},
            )
        s.commit()
        # If commit succeeds, this test should fail because duplicate did not trigger rollback.
        passed = False
        return {
            "test": "multi_record_atomicity",
            "passed": passed,
            "error": "expected rollback did not happen",
            "tests_run": ["single_sql_transaction_with_intentional_duplicate_record_id"],
            "decision_metrics": {
                "sql_count_before": before,
                "sql_count_after": _sql_count(),
                "record_ids_attempted": list(record_ids),
                "unique_record_id_count": len(unique_record_ids),
            },
            "decision_rule": "PASS iff duplicate triggers rollback and no partial rows remain",
        }
    except Exception:
        s.rollback()
        after = _sql_count()
        no_partial = after == before and all(not _sql_exists(record_id) for record_id in unique_record_ids)
        return {
            "test": "multi_record_atomicity",
            "passed": no_partial,
            "tests_run": ["single_sql_transaction_with_intentional_duplicate_record_id"],
            "decision_metrics": {
                "sql_count_before": before,
                "sql_count_after": after,
                "record_ids_attempted": list(record_ids),
                "unique_record_id_count": len(unique_record_ids),
                "all_unique_ids_absent_after_rollback": all(not _sql_exists(record_id) for record_id in unique_record_ids),
            },
            "decision_rule": "PASS iff duplicate triggers rollback and no partial rows remain",
        }
    finally:
        s.close()
        for record_id in unique_record_ids:
            _cleanup(record_id)

def consistency_test():
    return {"test": "consistency", "passed": False, "error": "not implemented yet"}


def isolation_test(num_workers: int = 3):
    return {"test": "isolation", "passed": False, "error": "not implemented yet"}


def durability_test():
    return {"test": "durability", "passed": False, "error": "not implemented yet"}
