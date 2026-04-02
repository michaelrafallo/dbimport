from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.config import get_settings


_STATE_LOCK = threading.Lock()


@dataclass
class JobProgress:
    status: str = "queued"
    message: str = "Job queued"
    percent: float = 0.0
    bytes_total: int = 0
    bytes_processed: int = 0
    parsed_queries: int = 0
    executed_queries: int = 0
    failed_queries: int = 0
    skipped_queries: int = 0
    eta_seconds: int | None = None
    started_at: float | None = None
    finished_at: float | None = None
    heartbeat_at: float | None = None
    stop_requested: bool = False
    log_file: str | None = None
    source_file: str | None = None
    extracted_sql_file: str | None = None
    error: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)


def _state_file(job_id: str) -> Path:
    return get_settings().state_dir / f"{job_id}.json"


def _log_file(job_id: str) -> Path:
    return get_settings().logs_dir / f"{job_id}.log"


def init_job(job_id: str, source_file: str) -> JobProgress:
    state = JobProgress(
        status="queued",
        message="Upload received and waiting for worker",
        source_file=source_file,
        log_file=str(_log_file(job_id)),
        heartbeat_at=time.time(),
    )
    save_state(job_id, state)
    append_log(job_id, "INFO", "Job initialized")
    return state


def load_state(job_id: str) -> JobProgress:
    path = _state_file(job_id)
    if not path.exists():
        return JobProgress(status="unknown", message="Job not found")
    data = json.loads(path.read_text(encoding="utf-8"))
    return JobProgress(**data)


def save_state(job_id: str, state: JobProgress) -> None:
    with _STATE_LOCK:
        path = _state_file(job_id)
        path.write_text(json.dumps(state.__dict__, ensure_ascii=True, indent=2), encoding="utf-8")


def update_state(job_id: str, **changes: Any) -> JobProgress:
    state = load_state(job_id)
    for key, value in changes.items():
        setattr(state, key, value)
    state.heartbeat_at = time.time()
    save_state(job_id, state)
    return state


def append_log(job_id: str, level: str, message: str) -> None:
    normalized = str(level or "INFO").strip().upper()
    level_map = {
        "WARN": "WARNING",
        "WARNING": "WARNING",
        "ERR": "ERROR",
        "ERROR": "ERROR",
        "SUCCESS": "SUCCESS",
        "DEBUG": "INFO",
        "INFO": "INFO",
    }
    normalized = level_map.get(normalized, "INFO")
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    line = f"[{ts}] [{normalized}] {message}\n"
    with _STATE_LOCK:
        _log_file(job_id).open("a", encoding="utf-8").write(line)


def get_log_tail(job_id: str, max_lines: int = 200) -> list[str]:
    path = _log_file(job_id)
    if not path.exists():
        return []
    max_lines = max(1, min(max_lines, 5000))
    # Read from end-of-file in chunks to avoid loading huge logs entirely.
    chunk_size = 8192
    newline_count = 0
    chunks: list[bytes] = []
    with path.open("rb") as f:
        f.seek(0, 2)
        file_size = f.tell()
        pos = file_size
        while pos > 0 and newline_count <= max_lines:
            read_size = chunk_size if pos >= chunk_size else pos
            pos -= read_size
            f.seek(pos)
            data = f.read(read_size)
            chunks.append(data)
            newline_count += data.count(b"\n")
    raw = b"".join(reversed(chunks))
    lines = raw.decode("utf-8", errors="replace").splitlines()
    return lines[-max_lines:]
