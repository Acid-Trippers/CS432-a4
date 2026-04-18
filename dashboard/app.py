from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pymongo import MongoClient

from dashboard.dependencies import is_admin_token_valid
from dashboard.session_manager import SessionManager
from dashboard.routers import acid, auth, pipeline, query, sessions, stats
from src.config import (
    INITIAL_SCHEMA_FILE,
    METADATA_FILE,
    MONGO_URI,
    SESSIONS_ARCHIVE_FILE,
    SESSIONS_FILE,
    TRANSACTION_LOG_FILE,
)
from src.phase_5.sql_engine import SQLEngine
from src.phase_6.transaction_coordinator import TransactionCoordinator


BASE_DIR = Path(__file__).resolve().parent


@asynccontextmanager
async def lifespan(app: FastAPI):
    sql_engine = SQLEngine()
    app.state.sql_initialized = False
    app.state.pipeline_state = "fresh"

    if os.path.exists(METADATA_FILE):
        try:
            initialized = sql_engine.initialize()
        except Exception:
            initialized = False

        if initialized:
            app.state.sql_initialized = True
            app.state.pipeline_state = "initialized"
        elif os.path.exists(INITIAL_SCHEMA_FILE):
            app.state.pipeline_state = "schema_ready"
    elif os.path.exists(INITIAL_SCHEMA_FILE):
        app.state.pipeline_state = "schema_ready"

    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)

    coordinator = TransactionCoordinator(log_file=TRANSACTION_LOG_FILE)
    session_manager = SessionManager(
        active_file=SESSIONS_FILE,
        archive_file=SESSIONS_ARCHIVE_FILE,
    )

    app.state.sql_engine = sql_engine
    app.state.mongo_client = mongo_client
    app.state.coordinator = coordinator
    app.state.session_manager = session_manager
    app.state.pipeline_busy = False
    app.state.admin_tokens = set()

    try:
        yield
    finally:
        try:
            app.state.sql_engine.close()
        except Exception:
            pass

        try:
            app.state.mongo_client.close()
        except Exception:
            pass


app = FastAPI(lifespan=lifespan)

app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.state.templates = templates

app.include_router(stats.router)
app.include_router(pipeline.router)
app.include_router(query.router)
app.include_router(acid.router)
app.include_router(auth.router)
app.include_router(sessions.router)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/setup", response_class=HTMLResponse)
async def setup(request: Request):
    if not is_admin_token_valid(request):
        return RedirectResponse(url="/", status_code=307)

    schema_payload = {
        "entities": {
            "example_entity": {
                "fields": {
                    "id": {"type": "string", "required": True},
                    "name": {"type": "string", "required": True},
                }
            }
        }
    }

    if os.path.exists(INITIAL_SCHEMA_FILE):
        try:
            with open(INITIAL_SCHEMA_FILE, "r", encoding="utf-8") as schema_file:
                loaded_schema = json.load(schema_file)
                if isinstance(loaded_schema, dict):
                    schema_payload = loaded_schema
        except Exception:
            pass

    return templates.TemplateResponse(
        "setup.html",
        {
            "request": request,
            "schema_json": json.dumps(schema_payload, indent=2),
        },
    )


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_home(request: Request):
    return RedirectResponse(url="/", status_code=307)


@app.get("/dashboard/admin", response_class=HTMLResponse)
async def dashboard_admin(request: Request):
    if not is_admin_token_valid(request):
        return RedirectResponse(url="/", status_code=307)
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/dashboard/developer", response_class=HTMLResponse)
async def developer_dashboard(request: Request):
    if not is_admin_token_valid(request):
        return RedirectResponse(url="/", status_code=307)
    return templates.TemplateResponse("dashboard_developer.html", {"request": request})


@app.get("/dashboard/{session_id}", response_class=HTMLResponse)
async def dashboard_user(request: Request, session_id: str):
    try:
        normalized_session_id = str(uuid.UUID(session_id))
    except ValueError:
        return RedirectResponse(url="/", status_code=307)

    session_view = request.app.state.session_manager.get_session(normalized_session_id)
    if session_view is None:
        return RedirectResponse(url="/", status_code=307)

    return templates.TemplateResponse(
        "dashboard_user.html",
        {
            "request": request,
            "session_id": normalized_session_id,
            "session_title": session_view.get("title") or "Untitled Session",
        },
    )
