from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, Field, model_validator

from dashboard.dependencies import get_session_id


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


def _attach_session_header(response: Response, session_id: str) -> None:
    response.headers["X-Session-ID"] = session_id


def _is_failed_result(result: dict[str, Any] | None) -> bool:
    if not isinstance(result, dict):
        return True
    status_value = str(result.get("status", "")).lower()
    return status_value in {"failed", "error"} or bool(result.get("error"))


@router.post("/api/query")
async def execute_query(
    body: QueryPayload,
    request: Request,
    response: Response,
    session_id: str = Depends(get_session_id),
):
    session_manager = request.app.state.session_manager
    _attach_session_header(response, session_id)

    if request.app.state.pipeline_busy:
        raise HTTPException(status_code=409, detail="pipeline is running; queries are temporarily blocked")

    if session_manager.has_active_transaction():
        if session_manager.is_transaction_owner(session_id):
            raise HTTPException(
                status_code=409,
                detail="manual transaction active for this session; use /api/query/tx/exec and /api/query/tx/commit",
            )
        raise HTTPException(status_code=423, detail="system busy with an active transaction")

    if not session_manager.begin_query_execution():
        raise HTTPException(status_code=409, detail="pipeline is running; queries are temporarily blocked")

    try:
        from src.phase_6.CRUD_runner import query_runner

        result = query_runner(
            query_dict=body.model_dump(),
            session_id=session_id,
            session_manager=session_manager,
        )
        if result is None:
            raise HTTPException(status_code=400, detail="query could not be executed")
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        session_manager.end_query_execution()


@router.post("/api/query/tx/begin")
async def begin_manual_transaction(
    request: Request,
    response: Response,
    session_id: str = Depends(get_session_id),
):
    session_manager = request.app.state.session_manager
    _attach_session_header(response, session_id)

    if request.app.state.pipeline_busy:
        raise HTTPException(status_code=409, detail="pipeline is running; transactions are temporarily blocked")

    existing = session_manager.get_manual_transaction(session_id)
    if existing is not None:
        raise HTTPException(status_code=409, detail="manual transaction already active for this session")

    tx_state = session_manager.begin_manual_transaction(session_id)
    if tx_state is None:
        raise HTTPException(status_code=423, detail="system busy with another active transaction")

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
    session_id: str = Depends(get_session_id),
):
    session_manager = request.app.state.session_manager
    _attach_session_header(response, session_id)

    if request.app.state.pipeline_busy:
        raise HTTPException(status_code=409, detail="pipeline is running; transactions are temporarily blocked")

    tx_state = session_manager.get_manual_transaction(session_id)
    if tx_state is None:
        raise HTTPException(status_code=409, detail="no active transaction for this session")

    try:
        staged = session_manager.stage_manual_operation(session_id, body.model_dump())
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    tx_state = session_manager.get_manual_transaction(session_id) or {}
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
    session_id: str = Depends(get_session_id),
):
    session_manager = request.app.state.session_manager
    _attach_session_header(response, session_id)

    if request.app.state.pipeline_busy:
        raise HTTPException(status_code=409, detail="pipeline is running; transactions are temporarily blocked")

    tx_state = session_manager.get_manual_transaction(session_id)
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
                session_id=session_id,
                session_manager=session_manager,
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
            session_id=session_id,
            status="COMMITTED",
            error=None,
            results=results,
        )

        return {
            "status": "COMMITTED",
            "tx_id": (completed or {}).get("tx_id"),
            "executed_operations": len(staged_payloads),
            "results": results,
        }
    except Exception as exc:
        completed = session_manager.complete_manual_transaction(
            session_id=session_id,
            status="FAILED",
            error=str(exc),
            results=results,
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
    session_id: str = Depends(get_session_id),
):
    session_manager = request.app.state.session_manager
    _attach_session_header(response, session_id)

    tx_state = session_manager.get_manual_transaction(session_id)
    if tx_state is None:
        raise HTTPException(status_code=409, detail="no active transaction for this session")

    rolled_back = session_manager.rollback_manual_transaction(
        session_id=session_id,
        reason="rolled back by client request",
    )
    return {
        "status": "ROLLED_BACK",
        "tx_id": (rolled_back or tx_state).get("tx_id"),
        "discarded_operations": len((tx_state or {}).get("operations") or []),
    }
