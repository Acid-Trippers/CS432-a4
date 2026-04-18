from fastapi import Header, HTTPException, Request, status

from src.config import MONGO_DB_NAME


ADMIN_TOKEN_COOKIE = "admin_token"


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


def get_admin_tokens_store(request: Request) -> set[str]:
    token_store = getattr(request.app.state, "admin_tokens", None)
    if not isinstance(token_store, set):
        token_store = set()
        request.app.state.admin_tokens = token_store
    return token_store


def extract_admin_token(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
) -> str | None:
    if isinstance(x_admin_token, str) and x_admin_token.strip():
        return x_admin_token.strip()

    cookie_token = request.cookies.get(ADMIN_TOKEN_COOKIE)
    if isinstance(cookie_token, str) and cookie_token.strip():
        return cookie_token.strip()

    return None


def is_admin_token_valid(request: Request, token: str | None = None) -> bool:
    candidate = token.strip() if isinstance(token, str) else None
    if not candidate:
        cookie_value = request.cookies.get(ADMIN_TOKEN_COOKIE)
        if isinstance(cookie_value, str) and cookie_value.strip():
            candidate = cookie_value.strip()

    if not candidate:
        return False

    return candidate in get_admin_tokens_store(request)


def require_admin(
    request: Request,
    admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
) -> str:
    resolved_token = extract_admin_token(request, admin_token)
    if not is_admin_token_valid(request, resolved_token):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="admin authentication required",
        )
    return resolved_token or ""
