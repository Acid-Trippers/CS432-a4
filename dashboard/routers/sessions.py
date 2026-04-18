from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field

from dashboard.dependencies import require_admin


router = APIRouter(prefix="/api/sessions")


class SessionCreatePayload(BaseModel):
    title: str = Field(min_length=1, max_length=120)


class SessionDeletePayload(BaseModel):
    session_ids: list[str] = Field(min_length=1)


@router.get("")
async def list_sessions(
    request: Request,
    include_archived: bool = Query(default=True),
):
    session_manager = request.app.state.session_manager
    snapshot = session_manager.list_sessions(include_archived=include_archived)

    sessions = snapshot.get("sessions", [])
    shaped = []
    for item in sessions:
        if not isinstance(item, dict):
            continue
        shaped.append(
            {
                "session_id": item.get("session_id"),
                "title": item.get("title") or "Untitled Session",
                "status": item.get("status") or item.get("location") or "unknown",
                "last_active": item.get("last_active"),
                "started_at": item.get("started_at"),
                "query_count": item.get("query_count", 0),
                "transaction_count": item.get("transaction_count", 0),
                "location": item.get("location", "active"),
            }
        )

    return {
        "total": snapshot.get("total", 0),
        "active": snapshot.get("active", 0),
        "archived": snapshot.get("archived", 0),
        "sessions": shaped,
    }


@router.post("")
async def create_session(
    payload: SessionCreatePayload,
    request: Request,
    response: Response,
):
    session_manager = request.app.state.session_manager
    session_obj = session_manager.create_session(title=payload.title)
    session_id = session_obj.get("session_id")
    if not isinstance(session_id, str) or not session_id:
        raise HTTPException(status_code=500, detail="failed to create session")

    response.headers["X-Session-ID"] = session_id
    return {
        "session_id": session_id,
        "title": session_obj.get("title") or "Untitled Session",
        "status": session_obj.get("status", "ACTIVE"),
        "started_at": session_obj.get("started_at"),
    }


@router.post("/delete")
async def delete_sessions(
    payload: SessionDeletePayload,
    request: Request,
    _: str = Depends(require_admin),
):
    session_manager = request.app.state.session_manager
    result = session_manager.delete_sessions(payload.session_ids)

    return {
        "deleted_count": result.get("deleted_count", 0),
        "deleted": result.get("deleted", []),
    }
