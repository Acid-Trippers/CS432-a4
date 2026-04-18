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

    @staticmethod
    def _is_admin_actor(session_id: str | None) -> bool:
        return isinstance(session_id, str) and (
            session_id == "admin" or session_id.startswith("admin:")
        )

    def _default_session(self, session_id: str, now_iso: str | None = None) -> dict[str, Any]:
        stamp = now_iso or self._now_iso()
        return {
            "session_id": session_id,
            "title": "Untitled Session",
            "status": "ACTIVE",
            "started_at": stamp,
            "last_active": stamp,
            "query_history": [],
            "transaction_history": [],
        }

    @staticmethod
    def _normalize_title(title: str | None) -> str:
        if not isinstance(title, str):
            return "Untitled Session"
        normalized = title.strip()
        return normalized[:120] if normalized else "Untitled Session"

    @staticmethod
    def _normalize_status(status_value: Any, default: str = "ACTIVE") -> str:
        normalized = str(status_value or "").strip().upper()
        if normalized in {"ACTIVE", "IN_TRANSACTION", "ARCHIVED"}:
            return normalized
        if normalized in {"IN-TRANSACTION", "INTRANSACTION"}:
            return "IN_TRANSACTION"
        if normalized in {"ARCHIVE"}:
            return "ARCHIVED"
        return default

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
            record["status"] = "ARCHIVED"
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

            candidate_uuid = self._safe_uuid4(candidate_id)
            resolved_id = candidate_uuid or str(uuid.uuid4())
            now_iso = self._now_iso()

            if candidate_uuid and candidate_uuid in archive and candidate_uuid not in active:
                raise ValueError("session is archived and closed")

            if resolved_id in archive and resolved_id not in active and not candidate_uuid:
                while resolved_id in archive or resolved_id in active:
                    resolved_id = str(uuid.uuid4())

            if resolved_id not in active:
                active[resolved_id] = self._default_session(resolved_id, now_iso)
            else:
                current = active[resolved_id]
                if not isinstance(current, dict):
                    current = self._default_session(resolved_id, now_iso)
                current["title"] = self._normalize_title(current.get("title"))
                current_status = self._normalize_status(current.get("status"))
                current["status"] = (
                    "IN_TRANSACTION" if current_status == "IN_TRANSACTION" else "ACTIVE"
                )
                current["last_active"] = now_iso
                self._trim_histories(current)
                active[resolved_id] = current

            self._atomic_write_json(self.active_file, active)
            self._atomic_write_json(self.archive_file, archive)
            return resolved_id

    def touch_session(self, session_id: str) -> None:
        if self._is_admin_actor(session_id):
            return
        with self._state_lock:
            active = self._read_json_dict(self.active_file)
            now_iso = self._now_iso()
            session_obj = active.get(session_id)
            if not isinstance(session_obj, dict):
                session_obj = self._default_session(session_id, now_iso)
            session_obj["title"] = self._normalize_title(session_obj.get("title"))
            current_status = self._normalize_status(session_obj.get("status"))
            session_obj["status"] = (
                "IN_TRANSACTION" if current_status == "IN_TRANSACTION" else "ACTIVE"
            )
            session_obj["last_active"] = now_iso
            self._trim_histories(session_obj)
            active[session_id] = session_obj
            self._atomic_write_json(self.active_file, active)

    def create_session(self, title: str | None = None) -> dict[str, Any]:
        with self._state_lock:
            new_id = str(uuid.uuid4())
            now_iso = self._now_iso()

            active = self._read_json_dict(self.active_file)
            while new_id in active:
                new_id = str(uuid.uuid4())

            archive = self._read_json_dict(self.archive_file)
            while new_id in archive:
                new_id = str(uuid.uuid4())

            session_obj = self._default_session(new_id, now_iso)
            session_obj["title"] = self._normalize_title(title)
            active[new_id] = session_obj
            self._atomic_write_json(self.active_file, active)

            return copy.deepcopy(session_obj)

    def set_session_title(self, session_id: str, title: str | None) -> dict[str, Any] | None:
        with self._state_lock:
            active = self._read_json_dict(self.active_file)
            session_obj = active.get(session_id)
            if not isinstance(session_obj, dict):
                return None

            session_obj["title"] = self._normalize_title(title)
            active[session_id] = session_obj
            self._atomic_write_json(self.active_file, active)
            return copy.deepcopy(session_obj)

    def log_query_start(self, session_id: str, payload: dict[str, Any]) -> str:
        if self._is_admin_actor(session_id):
            return str(uuid.uuid4())
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
            current_status = self._normalize_status(session_obj.get("status"))
            session_obj["status"] = (
                "IN_TRANSACTION" if current_status == "IN_TRANSACTION" else "ACTIVE"
            )
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
        if self._is_admin_actor(session_id):
            return
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
            
            # For READ operations, include actual data and row count
            if isinstance(result, dict):
                operation = result.get("operation")
                if operation == "READ" and "data" in result:
                    read_data = result.get("data")
                    if isinstance(read_data, dict):
                        row_count = len(read_data)
                    elif isinstance(read_data, list):
                        row_count = len(read_data)
                    else:
                        row_count = 0
                    summary["row_count"] = row_count
                    summary["data"] = read_data
                elif operation in {"CREATE", "UPDATE", "DELETE"}:
                    if "affected_records" in result:
                        summary["affected_records"] = result.get("affected_records")
                    if "changes_summary" in result:
                        summary["changes_summary"] = result.get("changes_summary")
            
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
            current_status = self._normalize_status(session_obj.get("status"))
            session_obj["status"] = (
                "IN_TRANSACTION" if current_status == "IN_TRANSACTION" else "ACTIVE"
            )
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
        if self._is_admin_actor(session_id):
            return
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
            session_obj["status"] = "ACTIVE"
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

            if self._is_admin_actor(session_id):
                return copy.deepcopy(tx_payload)

            active = self._read_json_dict(self.active_file)
            now_iso = self._now_iso()
            session_obj = active.get(session_id)
            if not isinstance(session_obj, dict):
                session_obj = self._default_session(session_id, now_iso)
            session_obj["title"] = self._normalize_title(session_obj.get("title"))
            session_obj["status"] = "IN_TRANSACTION"
            session_obj["last_active"] = now_iso
            self._trim_histories(session_obj)
            active[session_id] = session_obj
            self._atomic_write_json(self.active_file, active)
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

            if self._is_admin_actor(session_id):
                return copy.deepcopy(operation_entry)

            active = self._read_json_dict(self.active_file)
            now_iso = self._now_iso()
            session_obj = active.get(session_id)
            if not isinstance(session_obj, dict):
                session_obj = self._default_session(session_id, now_iso)
            session_obj["status"] = "IN_TRANSACTION"
            session_obj["last_active"] = now_iso
            self._trim_histories(session_obj)
            active[session_id] = session_obj
            self._atomic_write_json(self.active_file, active)

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

            if self._is_admin_actor(session_id):
                return tx_payload

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

            active = self._read_json_dict(self.active_file)
            now_iso = self._now_iso()
            session_obj = active.get(session_id)
            if isinstance(session_obj, dict):
                session_obj["status"] = "ACTIVE"
                session_obj["last_active"] = now_iso
                self._trim_histories(session_obj)
                active[session_id] = session_obj
                self._atomic_write_json(self.active_file, active)

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
                record["title"] = self._normalize_title(record.get("title"))
                record["status"] = self._normalize_status(
                    record.get("status"),
                    default="ACTIVE" if location == "active" else "ARCHIVED",
                )
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
            result["title"] = self._normalize_title(result.get("title"))
            result["status"] = self._normalize_status(
                result.get("status"),
                default="ACTIVE" if location == "active" else "ARCHIVED",
            )
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

    def get_entities_in_session(self, session_id: str) -> list[dict[str, Any]]:
        """
        Extract unique entities from a session's query history.
        Returns list of entity objects with query stats.
        """
        session = self.get_session(session_id)
        if not session:
            return []

        query_history = session.get("query_history", [])
        if not isinstance(query_history, list):
            return []

        entities: dict[str, dict[str, Any]] = {}
        for query_entry in query_history:
            if not isinstance(query_entry, dict):
                continue

            payload = query_entry.get("payload")
            if not isinstance(payload, dict):
                continue

            entity_name = payload.get("entity")
            if not entity_name:
                continue

            if entity_name not in entities:
                entities[entity_name] = {
                    "entity_name": entity_name,
                    "query_count": 0,
                    "operations": [],
                    "last_queried": None,
                }

            entities[entity_name]["query_count"] += 1
            operation = payload.get("operation")
            if operation and operation not in entities[entity_name]["operations"]:
                entities[entity_name]["operations"].append(operation)

            query_at = query_entry.get("at")
            if query_at:
                parsed_time = self._parse_iso(query_at)
                if parsed_time:
                    current_last = self._parse_iso(entities[entity_name]["last_queried"]) if entities[entity_name]["last_queried"] else None
                    if current_last is None or parsed_time > current_last:
                        entities[entity_name]["last_queried"] = query_at

        result = list(entities.values())
        result.sort(key=lambda x: self._parse_iso(x.get("last_queried")) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        return result

    def get_query_history_paginated(
        self,
        session_id: str,
        limit: int = 50,
        offset: int = 0,
        operation_filter: str | None = None,
        entity_filter: str | None = None,
        status_filter: str | None = None,
    ) -> dict[str, Any]:
        """
        Get paginated query history from a session with optional filters.
        """
        session = self.get_session(session_id)
        if not session:
            return {"total": 0, "queries": []}

        query_history = session.get("query_history", [])
        if not isinstance(query_history, list):
            return {"total": 0, "queries": []}

        # Filter queries
        filtered_queries = []
        for query_entry in query_history:
            if not isinstance(query_entry, dict):
                continue

            payload = query_entry.get("payload", {})
            result = query_entry.get("result", {})

            if operation_filter:
                if isinstance(payload, dict):
                    if payload.get("operation", "").upper() != operation_filter.upper():
                        continue

            if entity_filter:
                if isinstance(payload, dict):
                    if payload.get("entity", "") != entity_filter:
                        continue

            if status_filter:
                if result.get("status", "").lower() != status_filter.lower():
                    continue

            filtered_queries.append(query_entry)

        # Sort by most recent first
        filtered_queries.sort(
            key=lambda x: self._parse_iso(x.get("at")) or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True
        )

        # Paginate
        total = len(filtered_queries)
        paginated = filtered_queries[offset : offset + limit]

        result_list = []
        for query_entry in paginated:
            payload = query_entry.get("payload", {})
            result = query_entry.get("result", {})
            metrics = result.get("metrics", {})

            result_list.append({
                "query_id": query_entry.get("query_id"),
                "timestamp": query_entry.get("at"),
                "operation": payload.get("operation") if isinstance(payload, dict) else None,
                "entity": payload.get("entity") if isinstance(payload, dict) else None,
                "status": query_entry.get("status") or result.get("status"),
                "row_count": result.get("row_count") if result.get("operation") == "READ" else result.get("affected_records"),
                "execution_time_ms": metrics.get("execution_time_ms") if isinstance(metrics, dict) else None,
                "filters": payload.get("filters") if isinstance(payload, dict) else None,
                "columns": payload.get("columns") if isinstance(payload, dict) else None,
                "error": query_entry.get("error"),
            })

        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "queries": result_list,
        }

    def clear_all_sessions(self) -> None:
        with self._state_lock:
            self._pending_transactions.clear()
            self._transaction_owner = None
            self._active_queries = 0
            self._pipeline_active = False
            self._atomic_write_json(self.active_file, {})
            self._atomic_write_json(self.archive_file, {})

    def delete_sessions(self, session_ids: list[str]) -> dict[str, Any]:
        with self._state_lock:
            active = self._read_json_dict(self.active_file)
            archive = self._read_json_dict(self.archive_file)

            deleted: list[str] = []
            for raw_id in session_ids:
                resolved_id = self._safe_uuid4(raw_id)
                if not resolved_id:
                    continue

                removed = False
                if resolved_id in active:
                    active.pop(resolved_id, None)
                    removed = True
                if resolved_id in archive:
                    archive.pop(resolved_id, None)
                    removed = True

                if not removed:
                    continue

                self._pending_transactions.pop(resolved_id, None)
                if self._transaction_owner == resolved_id:
                    self._transaction_owner = None
                deleted.append(resolved_id)

            self._atomic_write_json(self.active_file, active)
            self._atomic_write_json(self.archive_file, archive)

            return {
                "deleted": deleted,
                "deleted_count": len(deleted),
            }
