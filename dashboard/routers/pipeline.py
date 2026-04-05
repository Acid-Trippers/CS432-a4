import asyncio
import importlib
import json
import os
import shutil

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from src.config import DATA_DIR, INITIAL_SCHEMA_FILE
from src.phase_5.sql_engine import SQLEngine


router = APIRouter()


class SchemaPayload(BaseModel):
    schema: dict


def _dispose_shared_sql_engine(request: Request) -> None:
    sql_engine = request.app.state.sql_engine

    try:
        sql_engine.close()
    except Exception:
        pass

    try:
        raw_engine = sql_engine.schema_builder.engine
        if raw_engine is not None:
            raw_engine.dispose()
    except Exception:
        pass


def _reinitialize_shared_sql_engine(request: Request) -> None:
    new_engine = SQLEngine()
    if not new_engine.initialize():
        raise RuntimeError("Failed to reinitialize shared SQL engine")
    request.app.state.sql_engine = new_engine
    request.app.state.sql_initialized = True


def _reset_shared_sql_engine(request: Request) -> None:
    request.app.state.sql_engine = SQLEngine()
    request.app.state.sql_initialized = False


def _wipe_runtime_data_files(preserve_schema: bool = True) -> list[str]:
    removed: list[str] = []
    schema_path = os.path.abspath(INITIAL_SCHEMA_FILE)

    for name in os.listdir(DATA_DIR):
        path = os.path.abspath(os.path.join(DATA_DIR, name))
        if preserve_schema and path == schema_path:
            continue

        if os.path.isdir(path):
            shutil.rmtree(path, ignore_errors=True)
            removed.append(path)
        elif os.path.isfile(path):
            try:
                os.remove(path)
                removed.append(path)
            except FileNotFoundError:
                continue

    return removed


@router.post("/api/pipeline/schema")
async def save_schema(payload: SchemaPayload, request: Request):
    try:
        schema_module = importlib.import_module("src.phase_1_to_4.00_schema_definition")
        schema_module.validate_structure(payload.schema)

        with open(INITIAL_SCHEMA_FILE, "w", encoding="utf-8") as schema_file:
            json.dump(payload.schema, schema_file, indent=4)

        _dispose_shared_sql_engine(request)
        _reset_shared_sql_engine(request)
        request.app.state.pipeline_state = "schema_ready"

        return {
            "valid": True,
            "saved_to": INITIAL_SCHEMA_FILE,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/pipeline/initialise")
async def run_initialise(request: Request, count: int = Query(default=1000, ge=0)):
    if request.app.state.pipeline_busy:
        raise HTTPException(status_code=409, detail="pipeline is already running")

    request.app.state.pipeline_busy = True

    try:
        _dispose_shared_sql_engine(request)

        main_module = importlib.import_module("main")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: main_module.initialise(count))

        _reinitialize_shared_sql_engine(request)
        request.app.state.pipeline_state = "initialized"

        return {
            "status": "completed",
            "operation": "initialise",
            "count": count,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        request.app.state.pipeline_busy = False


@router.post("/api/pipeline/fetch")
async def run_fetch(request: Request, count: int = Query(default=100, ge=0)):
    if request.app.state.pipeline_busy:
        raise HTTPException(status_code=409, detail="pipeline is already running")

    request.app.state.pipeline_busy = True

    try:
        _dispose_shared_sql_engine(request)

        main_module = importlib.import_module("main")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: main_module.fetch(count))

        _reinitialize_shared_sql_engine(request)
        request.app.state.pipeline_state = "initialized"

        return {
            "status": "completed",
            "operation": "fetch",
            "count": count,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        request.app.state.pipeline_busy = False


@router.post("/api/pipeline/reset")
async def reset_everything(request: Request, wipe_schema: bool = Query(default=False)):
    if request.app.state.pipeline_busy:
        raise HTTPException(status_code=409, detail="pipeline is already running")

    request.app.state.pipeline_busy = True

    try:
        print("[RESET] Disposing shared SQL engine...", flush=True)
        _dispose_shared_sql_engine(request)

        print("[RESET] Starting clean_databases in executor...", flush=True)
        main_module = importlib.import_module("main")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, main_module.clean_databases)
        print("[RESET] clean_databases finished.", flush=True)

        removed_paths = _wipe_runtime_data_files(preserve_schema=not wipe_schema)
        print(f"[RESET] Wiped {len(removed_paths)} runtime data files (wipe_schema={wipe_schema}).", flush=True)
        
        _reset_shared_sql_engine(request)
        print("[RESET] Re-initialized shared SQL engine.", flush=True)

        request.app.state.pipeline_state = (
            "schema_ready" if os.path.exists(INITIAL_SCHEMA_FILE) else "fresh"
        )

        return {
            "status": "completed",
            "operation": "reset",
            "pipeline_state": request.app.state.pipeline_state,
            "removed_count": len(removed_paths),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        request.app.state.pipeline_busy = False
