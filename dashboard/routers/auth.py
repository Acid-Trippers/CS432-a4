import secrets

from fastapi import APIRouter, HTTPException, Request, Response, status
from pydantic import BaseModel, Field

from dashboard.dependencies import ADMIN_TOKEN_COOKIE, get_admin_tokens_store
from src.config import ADMIN_PASS


router = APIRouter(prefix="/api/auth/admin")


class AdminLoginPayload(BaseModel):
    password: str = Field(min_length=1)


def _set_admin_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=ADMIN_TOKEN_COOKIE,
        value=token,
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=60 * 60 * 8,
        path="/",
    )


@router.post("/login")
async def admin_login(payload: AdminLoginPayload, request: Request, response: Response):
    if payload.password != ADMIN_PASS:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid admin password",
        )

    token = secrets.token_urlsafe(32)
    token_store = get_admin_tokens_store(request)
    token_store.add(token)

    _set_admin_cookie(response, token)
    return {"authenticated": True}


@router.post("/logout")
async def admin_logout(request: Request, response: Response):
    cookie_token = request.cookies.get(ADMIN_TOKEN_COOKIE)
    if cookie_token:
        token_store = get_admin_tokens_store(request)
        token_store.discard(cookie_token)

    response.delete_cookie(key=ADMIN_TOKEN_COOKIE, path="/")
    return {"authenticated": False}


@router.get("/status")
async def admin_status(request: Request):
    cookie_token = request.cookies.get(ADMIN_TOKEN_COOKIE)
    token_store = get_admin_tokens_store(request)
    authenticated = bool(cookie_token and cookie_token in token_store)
    return {"authenticated": authenticated}
