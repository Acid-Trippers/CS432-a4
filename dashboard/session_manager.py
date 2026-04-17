import copy
import json
import os
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any


class SessionManager:
    def __init__(
        self,
        active_file: str,
        archive_file: str,
        idle_archive_seconds: int = 30 * 60,
        max_query_history: int = 200,
        max_transaction_history: int = 100,
    ):
        self.active_file = active_file
        self.archive_file = archive_file
        self.idle_archive_seconds = idle_archive_seconds
        self.max_query_history = max_query_history
        self.max_transaction_history = max_transaction_history

        self._state_lock = threading.RLock()
        self._pipeline_active = False
        self._active_queries = 0
        self._transaction_owner: str | None = None
        self._pending_transactions: dict[str, dict[str, Any]] = {}

        self._ensure_files()

    def _ensure_files(self) -> None:
        for path in [self.active_file, self.archive_file]:
            directory = os.path.dirname(path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            if not os.path.exists(path):
                self._atomic_write_json(path, {})

    @staticmethod
    def _atomic_write_json(path: str, payload: Any) -> None:
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, default=str)
        os.replace(tmp_path, path)

    @staticmethod
    def _read_json_dict(path: str) -> dict[str, Any]:
        try:
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            return data if isinstance(data, dict) else {}
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    @staticmethod
    def _parse_iso(timestamp: str | None) -> datetime | None:
        if not timestamp or not isinstance(timestamp, str):
            return None

        normalized = timestamp.strip()
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"

        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None

        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _safe_uuid4(value: str | None) -> str | None:
        if not value or not isinstance(value, str):
            return None

        candidate = value.strip()
        try:
            parsed = uuid.UUID(candidate)
        except ValueError:
            return None

        if parsed.version != 4:
            return None

        return str(parsed)

    def _default_session(self, session_id: str, now_iso: str | None = None) -> dict[str, Any]:
        stamp = now_iso or self._now_iso()
        return {
            "session_id": session_id,
            "status": "active",
            "started_at": stamp,
            "last_active": stamp,
            "query_history": [],
            "transaction_history": [],
        }

    def _trim_histories(self, session_obj: dict[str, Any]) -> None:
        query_history = session_obj.get("query_history")
        if not isinstance(query_history, list):
            query_history = []
        if len(query_history) > self.max_query_history:
            query_history = query_history[-self.max_query_history :]
        session_obj["query_history"] = query_history

        transaction_history = session_obj.get("transaction_history")
        if not isinstance(transaction_history, list):
            transaction_history = []
        if len(transaction_history) > self.max_transaction_history:
            transaction_history = transaction_history[-self.max_transaction_history :]
        session_obj["transaction_history"] = transaction_history

    def _archive_idle_locked(self) -> int:
        now = datetime.now(timezone.utc)
        active = self._read_json_dict(self.active_file)
        archive = self._read_json_dict(self.archive_file)
        archived_count = 0

        idle_cutoff = now - timedelta(seconds=self.idle_archive_seconds)
        to_archive: list[str] = []

        for session_id, record in active.items():
            if not isinstance(record, dict):
                continue
            last_active = self._parse_iso(record.get("last_active"))
            if last_active is None:
                last_active = self._parse_iso(record.get("started_at"))
            if last_active is None:
                continue
            if last_active < idle_cutoff:
                to_archive.append(session_id)

        if not to_archive:
            return 0

        now_iso = self._now_iso()
        for session_id in to_archive:
            record = active.pop(session_id, None)
            if not isinstance(record, dict):
                continue
            record["status"] = "archived"
            record["archived_at"] = now_iso
            self._trim_histories(record)
            archive[session_id] = record
            archived_count += 1

        self._atomic_write_json(self.active_file, active)
        self._atomic_write_json(self.archive_file, archive)
        return archived_count

    def archive_idle_sessions(self) -> int:
        with self._state_lock:
            return self._archive_idle_locked()

    def resolve_or_create_session(self, candidate_id: str | None) -> str:
        with self._state_lock:
            self._archive_idle_locked()

            active = self._read_json_dict(self.active_file)
            archive = self._read_json_dict(self.archive_file)

            resolved_id = self._safe_uuid4(candidate_id) or str(uuid.uuid4())
            now_iso = self._now_iso()

            if resolved_id in archive and resolved_id not in active:
                restored = archive.pop(resolved_id)
                if not isinstance(restored, dict):
                    restored = self._default_session(resolved_id, now_iso)
                restored["status"] = "active"
                restored["last_active"] = now_iso
                restored.pop("archived_at", None)
                self._trim_histories(restored)
                active[resolved_id] = restored
            elif resolved_id not in active:
                active[resolved_id] = self._default_session(resolved_id, now_iso)
            else:
                current = active[resolved_id]
                if not isinstance(current, dict):
                    current = self._default_session(resolved_id, now_iso)
                current["status"] = "active"
                current["last_active"] = now_iso
                self._trim_histories(current)
                active[resolved_id] = current

            self._atomic_write_json(self.active_file, active)
            self._atomic_write_json(self.archive_file, archive)
            return resolved_id

    def touch_session(self, session_id: str) -> None:
        with self._state_lock:
            active = self._read_json_dict(self.active_file)
            now_iso = self._now_iso()
            session_obj = active.get(session_id)
            if not isinstance(session_obj, dict):
                session_obj = self._default_session(session_id, now_iso)
            session_obj["status"] = "active"
            session_obj["last_active"] = now_iso
            self._trim_histories(session_obj)
            active[session_id] = session_obj
            self._atomic_write_json(self.active_file, active)

    def log_query_start(self, session_id: str, payload: dict[str, Any]) -> str:
        with self._state_lock:
            active = self._read_json_dict(self.active_file)
            now_iso = self._now_iso()
            session_obj = active.get(session_id)
            if not isinstance(session_obj, dict):
                session_obj = self._default_session(session_id, now_iso)

            query_id = str(uuid.uuid4())
            query_entry = {
                "query_id": query_id,
                "at": now_iso,
                "status": "running",
                "payload": copy.deepcopy(payload),
            }

            history = session_obj.get("query_history")
            if not isinstance(history, list):
                history = []
            history.append(query_entry)
            session_obj["query_history"] = history
            session_obj["last_active"] = now_iso
            session_obj["status"] = "active"
            self._trim_histories(session_obj)

            active[session_id] = session_obj
            self._atomic_write_json(self.active_file, active)
            return query_id

    def log_query_end(
        self,
        session_id: str,
        query_id: str,
        outcome: str,
        result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        with self._state_lock:
            active = self._read_json_dict(self.active_file)
            now_iso = self._now_iso()
            session_obj = active.get(session_id)
            if not isinstance(session_obj, dict):
                session_obj = self._default_session(session_id, now_iso)

            history = session_obj.get("query_history")
            if not isinstance(history, list):
                history = []

            summary: dict[str, Any] = {
                "operation": result.get("operation") if isinstance(result, dict) else None,
                "entity": result.get("entity") if isinstance(result, dict) else None,
                "status": result.get("status") if isinstance(result, dict) else None,
                "metrics": result.get("metrics") if isinstance(result, dict) else None,
            }
            if error:
                summary["error"] = error

            matched = False
            for item in reversed(history):
                if isinstance(item, dict) and item.get("query_id") == query_id:
                    item["status"] = outcome
                    item["finished_at"] = now_iso
                    item["result"] = summary
                    if error:
                        item["error"] = error
                    matched = True
                    break

            if not matched:
                fallback_entry = {
                    "query_id": query_id,
                    "at": now_iso,
                    "status": outcome,
                    "finished_at": now_iso,
                    "payload": None,
                    "result": summary,
                }
                if error:
                    fallback_entry["error"] = error
                history.append(fallback_entry)

            session_obj["query_history"] = history
            session_obj["last_active"] = now_iso
            session_obj["status"] = "active"
            self._trim_histories(session_obj)

            active[session_id] = session_obj
            self._atomic_write_json(self.active_file, active)

    def append_transaction(
        self,
        session_id: str,
        tx_id: str,
        status: str,
        query_count: int,
        error: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        with self._state_lock:
            active = self._read_json_dict(self.active_file)
            now_iso = self._now_iso()
            session_obj = active.get(session_id)
            if not isinstance(session_obj, dict):
                session_obj = self._default_session(session_id, now_iso)

            tx_history = session_obj.get("transaction_history")
            if not isinstance(tx_history, list):
                tx_history = []

            tx_entry: dict[str, Any] = {
                "tx_id": tx_id,
                "status": status,
                "query_count": int(query_count),
                "completed_at": now_iso,
            }
            if error:
                tx_entry["error"] = error
            if details:
                tx_entry["details"] = details

            tx_history.append(tx_entry)
            session_obj["transaction_history"] = tx_history
            session_obj["last_active"] = now_iso
            session_obj["status"] = "active"
            self._trim_histories(session_obj)

            active[session_id] = session_obj
            self._atomic_write_json(self.active_file, active)

    def begin_manual_transaction(self, session_id: str) -> dict[str, Any] | None:
        with self._state_lock:
            if self._pipeline_active:
                return None

            if self._transaction_owner and self._transaction_owner != session_id:
                return None

            if session_id in self._pending_transactions:
                return copy.deepcopy(self._pending_transactions[session_id])

            tx_payload = {
                "tx_id": str(uuid.uuid4()),
                "session_id": session_id,
                "status": "IN_PROGRESS",
                "started_at": self._now_iso(),
                "operations": [],
            }
            self._transaction_owner = session_id
            self._pending_transactions[session_id] = tx_payload
            self.touch_session(session_id)
            return copy.deepcopy(tx_payload)

    def get_manual_transaction(self, session_id: str) -> dict[str, Any] | None:
        with self._state_lock:
            tx_payload = self._pending_transactions.get(session_id)
            if not isinstance(tx_payload, dict):
                return None
            return copy.deepcopy(tx_payload)

    def stage_manual_operation(self, session_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        with self._state_lock:
            tx_payload = self._pending_transactions.get(session_id)
            if not isinstance(tx_payload, dict):
                raise RuntimeError("no active transaction for session")

            operation_entry = {
                "op_id": str(uuid.uuid4()),
                "staged_at": self._now_iso(),
                "payload": copy.deepcopy(payload),
            }

            operations = tx_payload.get("operations")
            if not isinstance(operations, list):
                operations = []
            operations.append(operation_entry)
            tx_payload["operations"] = operations
            tx_payload["last_active"] = self._now_iso()
            self._pending_transactions[session_id] = tx_payload
            self.touch_session(session_id)
            return copy.deepcopy(operation_entry)

    def complete_manual_transaction(
        self,
        session_id: str,
        status: str,
        error: str | None = None,
        results: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None:
        with self._state_lock:
            tx_payload = self._pending_transactions.pop(session_id, None)
            if not isinstance(tx_payload, dict):
                return None

            tx_payload["status"] = status
            tx_payload["ended_at"] = self._now_iso()
            tx_payload["result_count"] = len(results or [])
            if error:
                tx_payload["error"] = error
            if results:
                tx_payload["results"] = copy.deepcopy(results)

            if self._transaction_owner == session_id:
                self._transaction_owner = None

            self.append_transaction(
                session_id=session_id,
                tx_id=str(tx_payload.get("tx_id")),
                status=status,
                query_count=len(tx_payload.get("operations") or []),
                error=error,
                details={
                    "started_at": tx_payload.get("started_at"),
                    "ended_at": tx_payload.get("ended_at"),
                    "result_count": tx_payload.get("result_count"),
                },
            )
            return tx_payload

    def rollback_manual_transaction(self, session_id: str, reason: str | None = None) -> dict[str, Any] | None:
        return self.complete_manual_transaction(
            session_id=session_id,
            status="ROLLED_BACK",
            error=reason,
            results=None,
        )

    def has_active_transaction(self) -> bool:
        with self._state_lock:
            return self._transaction_owner is not None

    def is_transaction_owner(self, session_id: str) -> bool:
        with self._state_lock:
            return self._transaction_owner == session_id

    def begin_query_execution(self) -> bool:
        with self._state_lock:
            if self._pipeline_active:
                return False
            self._active_queries += 1
            return True

    def end_query_execution(self) -> None:
        with self._state_lock:
            if self._active_queries > 0:
                self._active_queries -= 1

    def try_enter_pipeline(self) -> tuple[bool, str | None]:
        with self._state_lock:
            if self._pipeline_active:
                return False, "pipeline is already running"
            if self._active_queries > 0:
                return False, "system busy with active queries"
            if self._transaction_owner is not None:
                return False, "system busy with active transaction"

            self._pipeline_active = True
            return True, None

    def exit_pipeline(self) -> None:
        with self._state_lock:
            self._pipeline_active = False

    def list_sessions(self, include_archived: bool = True) -> dict[str, Any]:
        with self._state_lock:
            self._archive_idle_locked()

            active = self._read_json_dict(self.active_file)
            archive = self._read_json_dict(self.archive_file)

            def _build_view(session_obj: dict[str, Any], location: str) -> dict[str, Any]:
                record = copy.deepcopy(session_obj)
                queries = record.get("query_history")
                txs = record.get("transaction_history")
                if not isinstance(queries, list):
                    queries = []
                if not isinstance(txs, list):
                    txs = []

                record["query_count"] = len(queries)
                record["transaction_count"] = len(txs)
                record["location"] = location
                record["query_history"] = queries
                record["transaction_history"] = txs
                return record

            sessions: list[dict[str, Any]] = []
            for value in active.values():
                if isinstance(value, dict):
                    sessions.append(_build_view(value, "active"))

            if include_archived:
                for value in archive.values():
                    if isinstance(value, dict):
                        sessions.append(_build_view(value, "archive"))

            def _sort_key(item: dict[str, Any]):
                parsed = self._parse_iso(item.get("last_active"))
                return parsed or datetime.min.replace(tzinfo=timezone.utc)

            sessions.sort(key=_sort_key, reverse=True)
            return {
                "total": len(sessions),
                "active": sum(1 for item in sessions if item.get("location") == "active"),
                "archived": sum(1 for item in sessions if item.get("location") == "archive"),
                "sessions": sessions,
            }

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        with self._state_lock:
            self._archive_idle_locked()
            active = self._read_json_dict(self.active_file)
            archive = self._read_json_dict(self.archive_file)

            raw = active.get(session_id)
            location = "active"
            if not isinstance(raw, dict):
                raw = archive.get(session_id)
                location = "archive"

            if not isinstance(raw, dict):
                return None

            result = copy.deepcopy(raw)
            query_history = result.get("query_history")
            tx_history = result.get("transaction_history")
            if not isinstance(query_history, list):
                query_history = []
            if not isinstance(tx_history, list):
                tx_history = []

            result["query_history"] = query_history
            result["transaction_history"] = tx_history
            result["query_count"] = len(query_history)
            result["transaction_count"] = len(tx_history)
            result["location"] = location
            return result

    def clear_all_sessions(self) -> None:
        with self._state_lock:
            self._pending_transactions.clear()
            self._transaction_owner = None
            self._active_queries = 0
            self._pipeline_active = False
            self._atomic_write_json(self.active_file, {})
            self._atomic_write_json(self.archive_file, {})
