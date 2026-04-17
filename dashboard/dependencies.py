from fastapi import Header, Request

from src.config import MONGO_DB_NAME


def get_sql_engine(request: Request):
    return request.app.state.sql_engine


def get_mongo_db(request: Request):
    return request.app.state.mongo_client[MONGO_DB_NAME]


def get_coordinator(request: Request):
    return request.app.state.coordinator


def get_session_manager(request: Request):
    return request.app.state.session_manager


def get_session_id(
    request: Request,
    x_session_id: str | None = Header(default=None, alias="X-Session-ID"),
) -> str:
    session_manager = request.app.state.session_manager
    session_id = session_manager.resolve_or_create_session(x_session_id)
    return session_id
