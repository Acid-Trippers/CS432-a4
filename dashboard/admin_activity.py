import copy
import json
import os
import threading
import uuid
from datetime import datetime, timezone
from typing import Any


class AdminActivityManager:
    def __init__(
        self,
        file_path: str,
        max_query_history: int = 500,
        max_transaction_history: int = 200,
    ):
        self.file_path = file_path
        self.max_query_history = max_query_history
        self.max_transaction_history = max_transaction_history
        self._lock = threading.RLock()
        self._ensure_file()

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def _ensure_file(self) -> None:
        directory = os.path.dirname(self.file_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        if not os.path.exists(self.file_path):
            self._atomic_write_json({})

    def _atomic_write_json(self, payload: dict[str, Any]) -> None:
        tmp_path = f"{self.file_path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, default=str)
        os.replace(tmp_path, self.file_path)

    def _read_payload(self) -> dict[str, Any]:
        try:
            with open(self.file_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            return data if isinstance(data, dict) else {}
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _default_actor(self, actor_id: str) -> dict[str, Any]:
        stamp = self._now_iso()
        return {
            "actor_id": actor_id,
            "role": "admin",
            "started_at": stamp,
            "last_active": stamp,
            "query_history": [],
            "transaction_history": [],
        }

    def _trim_histories(self, actor_obj: dict[str, Any]) -> None:
        query_history = actor_obj.get("query_history")
        if not isinstance(query_history, list):
            query_history = []
        if len(query_history) > self.max_query_history:
            query_history = query_history[-self.max_query_history :]
        actor_obj["query_history"] = query_history

        transaction_history = actor_obj.get("transaction_history")
        if not isinstance(transaction_history, list):
            transaction_history = []
        if len(transaction_history) > self.max_transaction_history:
            transaction_history = transaction_history[-self.max_transaction_history :]
        actor_obj["transaction_history"] = transaction_history

    def touch_actor(self, actor_id: str) -> None:
        with self._lock:
            payload = self._read_payload()
            actor = payload.get(actor_id)
            if not isinstance(actor, dict):
                actor = self._default_actor(actor_id)
            actor["last_active"] = self._now_iso()
            self._trim_histories(actor)
            payload[actor_id] = actor
            self._atomic_write_json(payload)

    def log_query_start(self, actor_id: str, query_payload: dict[str, Any]) -> str:
        with self._lock:
            payload = self._read_payload()
            actor = payload.get(actor_id)
            if not isinstance(actor, dict):
                actor = self._default_actor(actor_id)

            query_id = str(uuid.uuid4())
            now_iso = self._now_iso()
            query_entry = {
                "query_id": query_id,
                "at": now_iso,
                "status": "running",
                "payload": copy.deepcopy(query_payload),
            }

            history = actor.get("query_history")
            if not isinstance(history, list):
                history = []
            history.append(query_entry)
            actor["query_history"] = history
            actor["last_active"] = now_iso
            self._trim_histories(actor)
            payload[actor_id] = actor
            self._atomic_write_json(payload)
            return query_id

    def log_query_end(
        self,
        actor_id: str,
        query_id: str,
        outcome: str,
        result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        with self._lock:
            payload = self._read_payload()
            actor = payload.get(actor_id)
            if not isinstance(actor, dict):
                actor = self._default_actor(actor_id)

            history = actor.get("query_history")
            if not isinstance(history, list):
                history = []

            summary = {
                "operation": result.get("operation") if isinstance(result, dict) else None,
                "entity": result.get("entity") if isinstance(result, dict) else None,
                "status": result.get("status") if isinstance(result, dict) else None,
                "metrics": result.get("metrics") if isinstance(result, dict) else None,
            }
            if error:
                summary["error"] = error

            finished_at = self._now_iso()
            matched = False
            for item in reversed(history):
                if isinstance(item, dict) and item.get("query_id") == query_id:
                    item["status"] = outcome
                    item["finished_at"] = finished_at
                    item["result"] = summary
                    if error:
                        item["error"] = error
                    matched = True
                    break

            if not matched:
                fallback_entry = {
                    "query_id": query_id,
                    "at": finished_at,
                    "status": outcome,
                    "finished_at": finished_at,
                    "payload": None,
                    "result": summary,
                }
                if error:
                    fallback_entry["error"] = error
                history.append(fallback_entry)

            actor["query_history"] = history
            actor["last_active"] = finished_at
            self._trim_histories(actor)
            payload[actor_id] = actor
            self._atomic_write_json(payload)

    def append_transaction(
        self,
        actor_id: str,
        tx_id: str,
        status: str,
        query_count: int,
        error: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            payload = self._read_payload()
            actor = payload.get(actor_id)
            if not isinstance(actor, dict):
                actor = self._default_actor(actor_id)

            history = actor.get("transaction_history")
            if not isinstance(history, list):
                history = []

            tx_entry: dict[str, Any] = {
                "tx_id": tx_id,
                "status": status,
                "query_count": int(query_count),
                "completed_at": self._now_iso(),
            }
            if error:
                tx_entry["error"] = error
            if details:
                tx_entry["details"] = copy.deepcopy(details)

            history.append(tx_entry)
            actor["transaction_history"] = history
            actor["last_active"] = self._now_iso()
            self._trim_histories(actor)
            payload[actor_id] = actor
            self._atomic_write_json(payload)

    def get_actor(self, actor_id: str) -> dict[str, Any] | None:
        with self._lock:
            payload = self._read_payload()
            actor = payload.get(actor_id)
            if not isinstance(actor, dict):
                return None

            result = copy.deepcopy(actor)
            query_history = result.get("query_history")
            if not isinstance(query_history, list):
                query_history = []
            tx_history = result.get("transaction_history")
            if not isinstance(tx_history, list):
                tx_history = []

            result["query_history"] = query_history
            result["transaction_history"] = tx_history
            result["query_count"] = len(query_history)
            result["transaction_count"] = len(tx_history)
            return result
