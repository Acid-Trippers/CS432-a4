"""Minimal ACID validators focused on core atomicity and consistency tests."""

import os
import subprocess
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


def _sql_exists_via_engine(engine: SQLEngine, record_id: int) -> bool:
    s = engine.schema_builder.get_session()
    try:
        row = s.execute(
            text("SELECT 1 FROM main_records WHERE record_id = :record_id LIMIT 1"),
            {"record_id": record_id},
        ).first()
        return row is not None
    finally:
        s.close()


def _mongo_exists_in_db(db, record_id: int) -> bool:
    return db["main_records"].count_documents({"$or": [{"_id": record_id}, {"record_id": record_id}]}) > 0


def _fresh_visibility_probe(record_id: int) -> tuple[bool, bool]:
    sql_probe = SQLEngine()
    sql_probe.initialize()
    sql_visible = _sql_exists_via_engine(sql_probe, record_id)

    probe_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        probe_db = probe_client[MONGO_DB_NAME]
        mongo_visible = _mongo_exists_in_db(probe_db, record_id)
    finally:
        probe_client.close()

    return sql_visible, mongo_visible


def _best_effort_cleanup_with_fresh_connections(record_id: int):
    for attempt in range(2):
        sql_probe = None
        session = None
        try:
            sql_probe = SQLEngine()
            sql_probe.initialize()
            if sql_probe.schema_builder.engine is not None:
                sql_probe.schema_builder.engine.dispose()
            session = sql_probe.schema_builder.get_session()
            session.execute(
                text("DELETE FROM main_records WHERE record_id = :record_id"),
                {"record_id": record_id},
            )
            session.commit()
            break
        except Exception:
            if session is not None:
                try:
                    session.rollback()
                except Exception:
                    pass
            if attempt == 0:
                time.sleep(1)
                continue
        finally:
            if session is not None:
                session.close()
            if sql_probe is not None:
                try:
                    sql_probe.close()
                except Exception:
                    pass

    try:
        probe_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        try:
            probe_db = probe_client[MONGO_DB_NAME]
            probe_db["main_records"].delete_many({"$or": [{"_id": record_id}, {"record_id": record_id}]})
        finally:
            probe_client.close()
    except Exception:
        pass


def _run_create(tag: str, payload_overrides: dict | None = None):
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
    if payload_overrides:
        query["payload"].update(payload_overrides)
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
    """Consistency test: reject a type-violating SQL write, keep state valid, and recover with a valid write."""
    original_counter = _get_counter()
    invalid_id = original_counter
    recovery_id = None

    try:
        # Subcheck 1 + 2: force a type violation in SQL and verify state stays consistent.
        _cleanup(invalid_id)
        _set_counter(original_counter)

        invalid_before_sql = _sql_count()
        invalid_before_mongo = _mongo_count()
        invalid_result = _run_create(
            "consistency_invalid",
            payload_overrides={"cpu_usage": "not-an-integer"},
        )

        invalid_after_sql = _sql_count()
        invalid_after_mongo = _mongo_count()
        invalid_tx_state = (invalid_result.get("transaction") or {}).get("state")
        invalid_sql_exists_after_invalid = _sql_exists(invalid_id)
        invalid_mongo_exists_after_invalid = _mongo_exists(invalid_id)

        invalid_rejected = (
            invalid_result.get("status") == "failed"
            and invalid_tx_state in ("rolled_back", "failed_needs_recovery")
        )

        no_invalid_persisted = (
            invalid_before_sql == invalid_after_sql
            and invalid_before_mongo == invalid_after_mongo
            and not invalid_sql_exists_after_invalid
            and not invalid_mongo_exists_after_invalid
        )

        # Subcheck 3: verify a valid write immediately after failure succeeds.
        _cleanup(invalid_id)
        _set_counter(original_counter)
        recovery_result = _run_create("consistency_recovery")
        recovery_id = (recovery_result.get("details") or {}).get("record_id")

        recovery_status = recovery_result.get("status")
        recovery_sql_exists = _sql_exists(recovery_id) if recovery_id is not None else False
        recovery_mongo_exists = _mongo_exists(recovery_id) if recovery_id is not None else False
        recovery_holds = (
            recovery_status == "success"
            and recovery_id is not None
            and recovery_sql_exists
            and recovery_mongo_exists
        )

        passed = invalid_rejected and no_invalid_persisted and recovery_holds

        return {
            "test": "consistency",
            "passed": passed,
            "invalid_write_rejected": invalid_rejected,
            "invalid_state_not_persisted": no_invalid_persisted,
            "recovery_write_holds": recovery_holds,
            "tests_run": [
                "invalid_type_rejection_on_sql_integer_field",
                "state_validation_after_failed_write",
                "valid_write_recovery_after_failure",
            ],
            "decision_metrics": {
                "invalid_status": invalid_result.get("status"),
                "invalid_tx_state": invalid_tx_state,
                "invalid_field": "cpu_usage",
                "invalid_value": "not-an-integer",
                "expected_field_type": "integer",
                "invalid_sql_count_before_after": f"{invalid_before_sql} -> {invalid_after_sql}",
                "invalid_mongo_count_before_after": f"{invalid_before_mongo} -> {invalid_after_mongo}",
                "invalid_sql_exists": invalid_sql_exists_after_invalid,
                "invalid_mongo_exists": invalid_mongo_exists_after_invalid,
                "recovery_status": recovery_status,
                "recovery_record_id": recovery_id,
                "recovery_sql_exists": recovery_sql_exists,
                "recovery_mongo_exists": recovery_mongo_exists,
                "note": "The invalid-write check targets SQL integer field cpu_usage with a string value to verify type enforcement",
            },
            "decision_rule": "PASS iff invalid_write_rejected AND invalid_state_not_persisted AND recovery_write_holds",
        }
    finally:
        if recovery_id is not None:
            _cleanup(recovery_id)
        _cleanup(invalid_id)
        _set_counter(original_counter)


def isolation_test():
    """Isolation test: verify uncommitted SQL writes are not visible to other sessions."""
    model = _model()
    if not model:
        return {"test": "isolation", "passed": False, "error": "main_records model unavailable"}

    test_record_id = _get_counter()
    writer = _session()
    reader_before_commit = _session()
    reader_after_commit = None

    try:
        _cleanup(test_record_id)

        writer.execute(
            text("INSERT INTO main_records (record_id) VALUES (:record_id)"),
            {"record_id": test_record_id},
        )
        writer.flush()

        # Reader session checks visibility while writer transaction is still uncommitted.
        uncommitted_visible = (
            reader_before_commit.execute(
                text("SELECT 1 FROM main_records WHERE record_id = :record_id LIMIT 1"),
                {"record_id": test_record_id},
            ).first()
            is not None
        )

        writer.commit()

        # Use a fresh session to verify visibility after commit.
        reader_after_commit = _session()
        committed_visible = (
            reader_after_commit.execute(
                text("SELECT 1 FROM main_records WHERE record_id = :record_id LIMIT 1"),
                {"record_id": test_record_id},
            ).first()
            is not None
        )

        no_dirty_read = not uncommitted_visible
        committed_read_visible = committed_visible
        passed = no_dirty_read and committed_read_visible

        return {
            "test": "isolation",
            "passed": passed,
            "no_dirty_read_holds": no_dirty_read,
            "committed_read_visible_holds": committed_read_visible,
            "tests_run": [
                "reader_cannot_see_uncommitted_write_from_other_session",
                "reader_sees_write_after_writer_commit",
            ],
            "decision_metrics": {
                "test_record_id": test_record_id,
                "uncommitted_visible": uncommitted_visible,
                "committed_visible": committed_visible,
            },
            "decision_rule": "PASS iff no_dirty_read_holds AND committed_read_visible_holds",
        }
    except Exception as e:
        try:
            writer.rollback()
        except Exception:
            pass
        return {
            "test": "isolation",
            "passed": False,
            "error": str(e),
            "tests_run": [
                "reader_cannot_see_uncommitted_write_from_other_session",
                "reader_sees_write_after_writer_commit",
            ],
        }
    finally:
        try:
            writer.close()
        except Exception:
            pass
        try:
            reader_before_commit.close()
        except Exception:
            pass
        if reader_after_commit is not None:
            try:
                reader_after_commit.close()
            except Exception:
                pass
        _cleanup(test_record_id)


def durability_test(crash_check: bool = False):
    """Durability test: verify committed data survives reconnect, with optional container restart check."""
    original_counter = _get_counter()
    record_id = None

    env_crash_check = os.getenv("ACID_DURABILITY_CRASH_CHECK", "0").strip() in {"1", "true", "yes"}
    crash_check_enabled = bool(crash_check) or env_crash_check
    crash_restart_success = None
    crash_sql_visible = None
    crash_mongo_visible = None
    crash_error = None

    try:
        _set_counter(original_counter)
        _best_effort_cleanup_with_fresh_connections(original_counter)

        create_result = _run_create("durability_commit")
        record_id = (create_result.get("details") or {}).get("record_id")

        commit_status = create_result.get("status")
        commit_holds = commit_status == "success" and record_id is not None
        if not commit_holds:
            return {
                "test": "durability",
                "passed": False,
                "commit_holds": False,
                "reconnect_visibility_holds": False,
                "repeated_read_stability_holds": False,
                "tests_run": [
                    "commit_record_through_normal_create_path",
                    "read_after_fresh_reconnect",
                    "repeated_fresh_reads_confirm_persistence",
                ],
                "decision_metrics": {
                    "commit_status": commit_status,
                    "record_id": record_id,
                    "crash_check_enabled": crash_check_enabled,
                },
                "decision_rule": "PASS iff commit_holds AND reconnect_visibility_holds AND repeated_read_stability_holds AND crash_recovery_holds_if_enabled",
            }

        reconnect_sql_visible, reconnect_mongo_visible = _fresh_visibility_probe(record_id)
        reconnect_visibility_holds = reconnect_sql_visible and reconnect_mongo_visible

        repeated_reads = []
        for _ in range(3):
            sql_visible, mongo_visible = _fresh_visibility_probe(record_id)
            repeated_reads.append({"sql_visible": sql_visible, "mongo_visible": mongo_visible})
        repeated_read_stability_holds = all(item["sql_visible"] and item["mongo_visible"] for item in repeated_reads)

        crash_recovery_holds = True
        if crash_check_enabled:
            try:
                repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
                restart_proc = subprocess.run(
                    ["docker-compose", "restart", "postgres", "mongo"],
                    cwd=repo_root,
                    capture_output=True,
                    text=True,
                    check=True,
                    timeout=240,
                )
                crash_restart_success = restart_proc.returncode == 0

                # Give services a short warm-up window and probe with fresh connections.
                for _ in range(10):
                    try:
                        crash_sql_visible, crash_mongo_visible = _fresh_visibility_probe(record_id)
                        break
                    except Exception:
                        time.sleep(2)

                crash_recovery_holds = (
                    crash_restart_success is True
                    and crash_sql_visible is True
                    and crash_mongo_visible is True
                )
            except Exception as e:
                crash_restart_success = False
                crash_recovery_holds = False
                crash_error = str(e)

        passed = (
            commit_holds
            and reconnect_visibility_holds
            and repeated_read_stability_holds
            and crash_recovery_holds
        )

        tests_run = [
            "commit_record_through_normal_create_path",
            "read_after_fresh_reconnect",
            "repeated_fresh_reads_confirm_persistence",
        ]
        if crash_check_enabled:
            tests_run.append("post_restart_read_after_container_recovery")

        return {
            "test": "durability",
            "passed": passed,
            "commit_holds": commit_holds,
            "reconnect_visibility_holds": reconnect_visibility_holds,
            "repeated_read_stability_holds": repeated_read_stability_holds,
            "crash_recovery_holds": crash_recovery_holds,
            "tests_run": tests_run,
            "decision_metrics": {
                "record_id": record_id,
                "commit_status": commit_status,
                "reconnect_sql_visible": reconnect_sql_visible,
                "reconnect_mongo_visible": reconnect_mongo_visible,
                "repeated_reads": repeated_reads,
                "crash_check_enabled": crash_check_enabled,
                "crash_restart_success": crash_restart_success,
                "crash_sql_visible": crash_sql_visible,
                "crash_mongo_visible": crash_mongo_visible,
                "crash_error": crash_error,
                "note": "Enable crash phase with ACID_DURABILITY_CRASH_CHECK=1",
            },
            "decision_rule": "PASS iff commit_holds AND reconnect_visibility_holds AND repeated_read_stability_holds AND crash_recovery_holds_if_enabled",
        }
    finally:
        if record_id is not None:
            _best_effort_cleanup_with_fresh_connections(record_id)
        _best_effort_cleanup_with_fresh_connections(original_counter)
        _set_counter(original_counter)
