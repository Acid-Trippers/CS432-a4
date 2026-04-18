from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, Field, model_validator

from dashboard.dependencies import get_execution_context


router = APIRouter()


class QueryPayload(BaseModel):
    operation: str = Field(min_length=1)
    entity: str = Field(min_length=1)
    filters: Dict[str, Any] | None = None
    payload: Dict[str, Any] | None = None
    columns: list | None = None  # Column selection support

    @model_validator(mode="after")
    def validate_operation_requirements(self):
        valid_operations = {"CREATE", "READ", "UPDATE", "DELETE"}
        if self.operation not in valid_operations:
            raise ValueError(
                f"Invalid operation '{self.operation}'. Must be one of: {sorted(valid_operations)}"
            )

        if self.operation == "CREATE":
            if self.payload is None:
                raise ValueError("CREATE operation requires 'payload' field.")
            if not isinstance(self.payload, dict) or not self.payload:
                raise ValueError("Field 'payload' must be a non-empty object.")

        elif self.operation == "READ":
            if self.filters is None:
                raise ValueError("READ operation requires 'filters' field.")
            if not isinstance(self.filters, dict):
                raise ValueError("Field 'filters' must be a dict.")

        elif self.operation == "UPDATE":
            if self.filters is None:
                raise ValueError("UPDATE operation requires 'filters' field.")
            if not isinstance(self.filters, dict) or not self.filters:
                raise ValueError("Field 'filters' must be a non-empty object.")

            if self.payload is None:
                raise ValueError("UPDATE operation requires 'payload' field.")
            if not isinstance(self.payload, dict) or not self.payload:
                raise ValueError("Field 'payload' must be a non-empty object.")

        elif self.operation == "DELETE":
            if self.filters is None:
                raise ValueError("DELETE operation requires 'filters' field.")
            if not isinstance(self.filters, dict):
                raise ValueError("Field 'filters' must be a dict.")

        return self


def _attach_session_header(response: Response, session_id: str | None) -> None:
    if isinstance(session_id, str) and session_id:
        response.headers["X-Session-ID"] = session_id


def _is_failed_result(result: dict[str, Any] | None) -> bool:
    if not isinstance(result, dict):
        return True
    status_value = str(result.get("status", "")).lower()
    return status_value in {"failed", "error"} or bool(result.get("error"))


def _extract_transaction_payload(result: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(result, dict):
        return None

    tx = result.get("transaction")
    if not isinstance(tx, dict):
        return None

    tx_id = tx.get("transaction_id")
    if not isinstance(tx_id, str) or not tx_id.strip():
        return None

    tx_state = str(tx.get("state") or "COMMITTED").strip().upper()
    tx_error = tx.get("error")
    tx_error_str = str(tx_error) if tx_error else None

    return {
        "tx_id": tx_id.strip(),
        "status": tx_state,
        "error": tx_error_str,
        "details": {
            "operation": result.get("operation"),
            "entity": result.get("entity"),
            "compensation_errors": tx.get("compensation_errors", []),
        },
    }


@router.post("/api/query")
async def execute_query(
    body: QueryPayload,
    request: Request,
    response: Response,
    execution_context: dict[str, str | None] = Depends(get_execution_context),
):
    session_manager = request.app.state.session_manager
    admin_activity_manager = request.app.state.admin_activity_manager
    actor_type = execution_context.get("actor_type")
    actor_id = execution_context.get("actor_id")
    session_id = execution_context.get("session_id")

    if not isinstance(actor_id, str) or not actor_id:
        raise HTTPException(status_code=500, detail="invalid execution actor")

    _attach_session_header(response, session_id)

    if request.app.state.pipeline_busy:
        raise HTTPException(status_code=409, detail="pipeline is running; queries are temporarily blocked")

    if session_manager.has_active_transaction():
        if session_manager.is_transaction_owner(actor_id):
            raise HTTPException(
                status_code=409,
                detail="manual transaction active for this session; use /api/query/tx/exec and /api/query/tx/commit",
            )
        raise HTTPException(status_code=423, detail="system busy with an active transaction")

    if not session_manager.begin_query_execution():
        raise HTTPException(status_code=409, detail="pipeline is running; queries are temporarily blocked")

    admin_query_id: str | None = None
    if actor_type == "admin":
        admin_query_id = admin_activity_manager.log_query_start(actor_id, body.model_dump())

    try:
        from src.phase_6.CRUD_runner import query_runner

        result = query_runner(
            query_dict=body.model_dump(),
            session_id=actor_id if actor_type == "user" else None,
            session_manager=session_manager if actor_type == "user" else None,
        )
        if result is None:
            raise HTTPException(status_code=400, detail="query could not be executed")

        if actor_type == "admin" and admin_query_id:
            result_status = "success"
            result_error = None
            if _is_failed_result(result if isinstance(result, dict) else None):
                result_status = "failed"
                if isinstance(result, dict):
                    result_error = str(result.get("error") or "query execution failed")
                else:
                    result_error = "query execution failed"

            admin_activity_manager.log_query_end(
                actor_id=actor_id,
                query_id=admin_query_id,
                outcome=result_status,
                result=result if isinstance(result, dict) else None,
                error=result_error,
            )

        tx_payload = _extract_transaction_payload(result if isinstance(result, dict) else None)
        if tx_payload:
            if actor_type == "user":
                session_manager.append_transaction(
                    session_id=actor_id,
                    tx_id=tx_payload["tx_id"],
                    status=tx_payload["status"],
                    query_count=1,
                    error=tx_payload["error"],
                    details=tx_payload["details"],
                )
            elif actor_type == "admin":
                admin_activity_manager.append_transaction(
                    actor_id=actor_id,
                    tx_id=tx_payload["tx_id"],
                    status=tx_payload["status"],
                    query_count=1,
                    error=tx_payload["error"],
                    details=tx_payload["details"],
                )

        return result
    except HTTPException:
        if actor_type == "admin" and admin_query_id:
            admin_activity_manager.log_query_end(
                actor_id=actor_id,
                query_id=admin_query_id,
                outcome="failed",
                result=None,
                error="query execution failed",
            )
        raise
    except Exception as exc:
        if actor_type == "admin" and admin_query_id:
            admin_activity_manager.log_query_end(
                actor_id=actor_id,
                query_id=admin_query_id,
                outcome="failed",
                result=None,
                error=str(exc),
            )
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        session_manager.end_query_execution()


@router.post("/api/query/tx/begin")
async def begin_manual_transaction(
    request: Request,
    response: Response,
    execution_context: dict[str, str | None] = Depends(get_execution_context),
):
    session_manager = request.app.state.session_manager
    admin_activity_manager = request.app.state.admin_activity_manager
    actor_id = execution_context.get("actor_id")
    actor_type = execution_context.get("actor_type")
    session_id = execution_context.get("session_id")

    if not isinstance(actor_id, str) or not actor_id:
        raise HTTPException(status_code=500, detail="invalid execution actor")

    _attach_session_header(response, session_id)

    if request.app.state.pipeline_busy:
        raise HTTPException(status_code=409, detail="pipeline is running; transactions are temporarily blocked")

    existing = session_manager.get_manual_transaction(actor_id)
    if existing is not None:
        raise HTTPException(status_code=409, detail="manual transaction already active for this session")

    tx_state = session_manager.begin_manual_transaction(actor_id)
    if tx_state is None:
        raise HTTPException(status_code=423, detail="system busy with another active transaction")

    if actor_type == "admin":
        admin_activity_manager.touch_actor(actor_id)

    return {
        "status": "IN_PROGRESS",
        "tx_id": tx_state.get("tx_id"),
        "started_at": tx_state.get("started_at"),
        "queued_operations": len(tx_state.get("operations") or []),
    }


@router.post("/api/query/tx/exec")
async def stage_manual_transaction_operation(
    body: QueryPayload,
    request: Request,
    response: Response,
    execution_context: dict[str, str | None] = Depends(get_execution_context),
):
    session_manager = request.app.state.session_manager
    admin_activity_manager = request.app.state.admin_activity_manager
    actor_id = execution_context.get("actor_id")
    actor_type = execution_context.get("actor_type")
    session_id = execution_context.get("session_id")

    if not isinstance(actor_id, str) or not actor_id:
        raise HTTPException(status_code=500, detail="invalid execution actor")

    _attach_session_header(response, session_id)

    if request.app.state.pipeline_busy:
        raise HTTPException(status_code=409, detail="pipeline is running; transactions are temporarily blocked")

    tx_state = session_manager.get_manual_transaction(actor_id)
    if tx_state is None:
        raise HTTPException(status_code=409, detail="no active transaction for this session")

    try:
        staged = session_manager.stage_manual_operation(actor_id, body.model_dump())
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    if actor_type == "admin":
        admin_activity_manager.touch_actor(actor_id)

    tx_state = session_manager.get_manual_transaction(actor_id) or {}
    return {
        "status": "STAGED",
        "tx_id": tx_state.get("tx_id"),
        "op_id": staged.get("op_id"),
        "queued_operations": len(tx_state.get("operations") or []),
    }


@router.post("/api/query/tx/commit")
async def commit_manual_transaction(
    request: Request,
    response: Response,
    execution_context: dict[str, str | None] = Depends(get_execution_context),
):
    session_manager = request.app.state.session_manager
    admin_activity_manager = request.app.state.admin_activity_manager
    actor_id = execution_context.get("actor_id")
    actor_type = execution_context.get("actor_type")
    session_id = execution_context.get("session_id")

    if not isinstance(actor_id, str) or not actor_id:
        raise HTTPException(status_code=500, detail="invalid execution actor")

    _attach_session_header(response, session_id)

    if request.app.state.pipeline_busy:
        raise HTTPException(status_code=409, detail="pipeline is running; transactions are temporarily blocked")

    tx_state = session_manager.get_manual_transaction(actor_id)
    if tx_state is None:
        raise HTTPException(status_code=409, detail="no active transaction for this session")

    operations = tx_state.get("operations") or []
    staged_payloads: list[dict[str, Any]] = []
    for item in operations:
        if isinstance(item, dict) and isinstance(item.get("payload"), dict):
            staged_payloads.append(item["payload"])

    from src.phase_6.CRUD_runner import query_runner

    results: list[dict[str, Any]] = []
    try:
        for payload in staged_payloads:
            result = query_runner(
                query_dict=payload,
                persist_output=False,
                session_id=actor_id if actor_type == "user" else None,
                session_manager=session_manager if actor_type == "user" else None,
            )
            if _is_failed_result(result if isinstance(result, dict) else None):
                raise RuntimeError(
                    f"staged operation failed: {(result or {}).get('error', 'unknown error')}"
                )

            results.append(
                {
                    "operation": result.get("operation") if isinstance(result, dict) else None,
                    "entity": result.get("entity") if isinstance(result, dict) else None,
                    "status": result.get("status") if isinstance(result, dict) else None,
                }
            )

        completed = session_manager.complete_manual_transaction(
            session_id=actor_id,
            status="COMMITTED",
            error=None,
            results=results,
        )

        if actor_type == "admin":
            tx_id = str((completed or tx_state).get("tx_id"))
            admin_activity_manager.append_transaction(
                actor_id=actor_id,
                tx_id=tx_id,
                status="COMMITTED",
                query_count=len(staged_payloads),
                error=None,
                details={
                    "started_at": (completed or tx_state).get("started_at"),
                    "ended_at": (completed or tx_state).get("ended_at"),
                    "result_count": len(results),
                },
            )

        return {
            "status": "COMMITTED",
            "tx_id": (completed or {}).get("tx_id"),
            "executed_operations": len(staged_payloads),
            "results": results,
        }
    except Exception as exc:
        completed = session_manager.complete_manual_transaction(
            session_id=actor_id,
            status="FAILED",
            error=str(exc),
            results=results,
        )

        if actor_type == "admin":
            tx_id = str((completed or tx_state).get("tx_id"))
            admin_activity_manager.append_transaction(
                actor_id=actor_id,
                tx_id=tx_id,
                status="FAILED",
                query_count=len(staged_payloads),
                error=str(exc),
                details={
                    "started_at": (completed or tx_state).get("started_at"),
                    "ended_at": (completed or tx_state).get("ended_at"),
                    "result_count": len(results),
                },
            )

        tx_id = (completed or tx_state).get("tx_id")
        raise HTTPException(
            status_code=500,
            detail=(
                f"manual transaction commit failed (tx_id={tx_id}, "
                f"executed_operations={len(results)}): {exc}"
            ),
        )


@router.post("/api/query/tx/rollback")
async def rollback_manual_transaction(
    request: Request,
    response: Response,
    execution_context: dict[str, str | None] = Depends(get_execution_context),
):
    session_manager = request.app.state.session_manager
    admin_activity_manager = request.app.state.admin_activity_manager
    actor_id = execution_context.get("actor_id")
    actor_type = execution_context.get("actor_type")
    session_id = execution_context.get("session_id")

    if not isinstance(actor_id, str) or not actor_id:
        raise HTTPException(status_code=500, detail="invalid execution actor")

    _attach_session_header(response, session_id)

    tx_state = session_manager.get_manual_transaction(actor_id)
    if tx_state is None:
        raise HTTPException(status_code=409, detail="no active transaction for this session")

    rolled_back = session_manager.rollback_manual_transaction(
        session_id=actor_id,
        reason="rolled back by client request",
    )

    if actor_type == "admin":
        tx_id = str((rolled_back or tx_state).get("tx_id"))
        admin_activity_manager.append_transaction(
            actor_id=actor_id,
            tx_id=tx_id,
            status="ROLLED_BACK",
            query_count=len((tx_state or {}).get("operations") or []),
            error="rolled back by client request",
            details={
                "started_at": (rolled_back or tx_state).get("started_at"),
                "ended_at": (rolled_back or tx_state).get("ended_at"),
                "result_count": 0,
            },
        )

    return {
        "status": "ROLLED_BACK",
        "tx_id": (rolled_back or tx_state).get("tx_id"),
        "discarded_operations": len((tx_state or {}).get("operations") or []),
    }
