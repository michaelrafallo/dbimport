from __future__ import annotations

import os
import time
import uuid
from pathlib import Path
from typing import Any

import mysql.connector
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from redis.exceptions import RedisError

from app.config import get_settings
from app.queue import get_queue
from app.services.file_service import cleanup_temp_path, resolve_sql_source, save_upload_stream
from app.services.import_executor import run_import_job
from app.services.job_state import (
    append_log,
    get_log_tail,
    init_job,
    load_state,
    update_state,
)
from app.services.local_jobs import is_local_job_alive, run_in_background

router = APIRouter(prefix="/api/imports", tags=["imports"])


class MySQLConnectionRequest(BaseModel):
    host: str = Field(min_length=1)
    username: str = Field(min_length=1)
    password: str = ""


class StartImportRequest(BaseModel):
    job_id: str
    host: str
    username: str
    password: str
    database: str
    stop_on_error: bool = True
    max_errors: int = 100
    show_query_logs: bool = False
    resume: bool = False


class DropTablesRequest(BaseModel):
    host: str
    username: str
    password: str
    database: str


def _clear_dir_contents(path: Path) -> int:
    if not path.exists():
        return 0
    deleted = 0
    for item in path.iterdir():
        cleanup_temp_path(item)
        deleted += 1
    return deleted


def _running_jobs_exist() -> bool:
    settings = get_settings()
    if not settings.state_dir.exists():
        return False
    for file in settings.state_dir.glob("*.json"):
        # Skip tracker artifacts; they are not JobProgress payloads.
        if file.name.endswith(".progress.json"):
            continue
        job_id = file.stem
        if is_local_job_alive(job_id):
            return True
    return False


def _parse_host_port(host_raw: str) -> tuple[str, int | None]:
    host = host_raw.strip()
    if not host:
        return host, None

    # On Windows/XAMPP, forcing TCP avoids connector socket path issues for localhost.
    if host.lower() == "localhost":
        return "127.0.0.1", None

    # Accept host:port syntax in the Host input.
    if ":" in host and not host.startswith("["):
        maybe_host, maybe_port = host.rsplit(":", 1)
        if maybe_port.isdigit():
            return maybe_host.strip() or "127.0.0.1", int(maybe_port)
    return host, None


def _format_mysql_exception(exc: Exception) -> str:
    errno = getattr(exc, "errno", None)
    msg = getattr(exc, "msg", None) or (exc.args[0] if getattr(exc, "args", None) else str(exc))
    msg = str(msg)
    if "not all arguments converted during string formatting" in msg.lower():
        msg = (
            "MySQL connector formatting error while resolving socket/host. "
            "Try using 127.0.0.1:3306 instead of localhost, and ensure MySQL is running."
        )
    if errno is not None:
        return f"{errno}: {msg}"
    return msg


def _mysql_config(host: str, username: str, password: str, database: str | None = None) -> dict[str, Any]:
    host_value, parsed_port = _parse_host_port(host)
    cfg: dict[str, Any] = {
        "host": host_value,
        "user": username.strip(),
        "password": password,
        "autocommit": False,
        "connection_timeout": 15,
    }
    if parsed_port is not None:
        cfg["port"] = parsed_port
    if database:
        cfg["database"] = database.strip()
    return cfg


def _connection_candidates(base_cfg: dict[str, Any], host_input: str) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = [base_cfg]
    host_value = str(base_cfg.get("host", "")).strip().lower()
    port_specified = "port" in base_cfg
    local_hosts = {"127.0.0.1", "localhost"}
    if host_value in local_hosts and not port_specified:
        fallback_ports = os.getenv("MYSQL_FALLBACK_PORTS", "3306,3307,3308")
        for p in [x.strip() for x in fallback_ports.split(",") if x.strip().isdigit()]:
            candidate = dict(base_cfg)
            candidate["port"] = int(p)
            if candidate not in candidates:
                candidates.append(candidate)
    return candidates


def _connect_mysql_with_fallback(
    host: str, username: str, password: str, database: str | None = None
) -> tuple[Any, dict[str, Any]]:
    base_cfg = _mysql_config(host, username, password, database)
    last_exc: Exception | None = None
    for cfg in _connection_candidates(base_cfg, host):
        try:
            conn = mysql.connector.connect(**cfg)
            return conn, cfg
        except Exception as exc:  # pragma: no cover - depends on runtime env
            last_exc = exc
            continue
    if last_exc is None:
        raise RuntimeError("No MySQL connection candidates could be generated.")
    raise last_exc


@router.post("/connect")
def connect_mysql(payload: MySQLConnectionRequest) -> dict[str, Any]:
    conn = None
    try:
        conn, resolved_cfg = _connect_mysql_with_fallback(payload.host, payload.username, payload.password)
        return {
            "ok": True,
            "resolved_host": resolved_cfg.get("host"),
            "resolved_port": resolved_cfg.get("port"),
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Connection failed: {_format_mysql_exception(exc)}") from exc
    finally:
        if conn:
            conn.close()


@router.post("/databases")
def list_databases(payload: MySQLConnectionRequest) -> dict[str, Any]:
    conn = None
    cursor = None
    try:
        conn, resolved_cfg = _connect_mysql_with_fallback(payload.host, payload.username, payload.password)
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        dbs = [row[0] for row in cursor.fetchall()]
        return {
            "databases": dbs,
            "resolved_host": resolved_cfg.get("host"),
            "resolved_port": resolved_cfg.get("port"),
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not list databases: {_format_mysql_exception(exc)}") from exc
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@router.post("/upload")
def upload_dump(file: UploadFile = File(...)) -> dict[str, str]:
    saved_path = save_upload_stream(file)
    job_id = uuid.uuid4().hex
    init_job(job_id, str(saved_path))
    append_log(job_id, "INFO", f"Upload stored: {saved_path.name}")
    return {"job_id": job_id, "file_path": str(saved_path)}


@router.post("/start")
def start_import(payload: StartImportRequest) -> dict[str, Any]:
    state = load_state(payload.job_id)
    if state.status == "unknown":
        raise HTTPException(status_code=404, detail="Job does not exist.")
    if state.status == "running":
        raise HTTPException(status_code=409, detail="Job is already running.")

    source_file = Path(state.source_file or "")
    if not source_file.exists():
        raise HTTPException(status_code=404, detail="Uploaded file no longer exists.")

    sql_file = resolve_sql_source(source_file)
    update_state(payload.job_id, extracted_sql_file=str(sql_file))
    append_log(payload.job_id, "INFO", f"Using SQL source: {sql_file.name}")

    mysql_config = _mysql_config(payload.host, payload.username, payload.password, payload.database)
    options = {
        "stop_on_error": payload.stop_on_error,
        "max_errors": payload.max_errors,
        "show_query_logs": payload.show_query_logs,
        "resume": payload.resume,
    }
    try:
        q = get_queue()
        # Fast fail with explicit API error if queue backend is unavailable.
        q.connection.ping()
        rq_job = q.enqueue(
            run_import_job,
            payload.job_id,
            mysql_config,
            str(sql_file),
            options,
            job_id=payload.job_id,
        )
        update_state(payload.job_id, status="queued", message="Queued for worker")
        return {
            "ok": True,
            "job_id": payload.job_id,
            "queue_job_id": rq_job.id,
            "execution_mode": "rq",
        }
    except RedisError as exc:
        append_log(payload.job_id, "INFO", f"Queue unavailable; switching to local background worker: {exc}")
        # Mark queued before the local thread starts to avoid racing with worker state updates.
        update_state(payload.job_id, status="queued", message="Queued for local background worker")
        thread_name = run_in_background(
            payload.job_id,
            run_import_job,
            payload.job_id,
            mysql_config,
            str(sql_file),
            options,
        )
        append_log(payload.job_id, "INFO", f"Local worker thread started: {thread_name}")
        return {
            "ok": True,
            "job_id": payload.job_id,
            "queue_job_id": thread_name,
            "execution_mode": "local_thread",
        }


@router.post("/clear")
def clear_import_artifacts() -> dict[str, Any]:
    if _running_jobs_exist():
        raise HTTPException(status_code=409, detail="An import is still running or queued. Cancel it first.")

    settings = get_settings()
    deleted_count = 0
    deleted_count += _clear_dir_contents(settings.uploads_dir)
    deleted_count += _clear_dir_contents(settings.temp_dir)
    deleted_count += _clear_dir_contents(settings.logs_dir)
    deleted_count += _clear_dir_contents(settings.state_dir)
    return {"ok": True, "deleted_items": deleted_count}


@router.post("/drop-tables")
def drop_tables(payload: DropTablesRequest) -> dict[str, Any]:
    conn = None
    cursor = None
    try:
        conn, _ = _connect_mysql_with_fallback(payload.host, payload.username, payload.password, payload.database)
        cursor = conn.cursor()
        cursor.execute("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'")
        rows = cursor.fetchall()
        table_names = [str(row[0]) for row in rows]
        if not table_names:
            return {"ok": True, "database": payload.database, "dropped_count": 0, "dropped_tables": []}

        cursor.execute("SET FOREIGN_KEY_CHECKS=0")
        for table in table_names:
            safe = table.replace("`", "``")
            cursor.execute(f"DROP TABLE IF EXISTS `{safe}`")
        cursor.execute("SET FOREIGN_KEY_CHECKS=1")
        conn.commit()
        return {
            "ok": True,
            "database": payload.database,
            "dropped_count": len(table_names),
            "dropped_tables": table_names,
        }
    except Exception as exc:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise HTTPException(status_code=400, detail=f"Drop tables failed: {_format_mysql_exception(exc)}") from exc
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@router.post("/{job_id}/cancel")
def cancel_job(job_id: str) -> dict[str, Any]:
    state = load_state(job_id)
    if state.status == "unknown":
        raise HTTPException(status_code=404, detail="Job does not exist.")
    update_state(job_id, stop_requested=True, status="cancelled", message="Cancellation requested")
    append_log(job_id, "WARN", "Cancellation requested by user.")
    return {"ok": True}


@router.get("/{job_id}/status")
def get_status(job_id: str, logs_tail: int = 200) -> dict[str, Any]:
    state = load_state(job_id)
    if state.status == "unknown":
        raise HTTPException(status_code=404, detail="Job does not exist.")
    return {"state": state.__dict__, "logs": get_log_tail(job_id, max_lines=max(10, min(logs_tail, 1000)))}


@router.get("/{job_id}/logs/download")
def download_logs(job_id: str) -> FileResponse:
    settings = get_settings()
    path = settings.logs_dir / f"{job_id}.log"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Log file does not exist.")
    return FileResponse(path, media_type="text/plain", filename=f"{job_id}.log")


@router.post("/{job_id}/cleanup")
def cleanup_job(job_id: str) -> dict[str, Any]:
    state = load_state(job_id)
    if state.status == "unknown":
        raise HTTPException(status_code=404, detail="Job does not exist.")

    cleanup_temp_path(Path(state.extracted_sql_file)) if state.extracted_sql_file else None
    cleanup_temp_path(Path(state.source_file)) if state.source_file else None
    update_state(job_id, message="Files cleaned up")
    return {"ok": True}
