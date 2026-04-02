from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from app.config import get_settings


def _tracker_path(job_id: str) -> Path:
    return get_settings().state_dir / f"{job_id}.progress.json"


def init_tracker(job_id: str, table_names: list[str]) -> dict[str, Any]:
    state = {
        "job_id": job_id,
        "created_at": time.time(),
        "updated_at": time.time(),
        "current_table": None,
        "completed_tables": [],
        "failed_tables": [],
        "table_order": table_names,
        "tables": {
            t: {
                "status": "pending",
                "attempts": 0,
                "last_error": None,
                "row_count_before": None,
                "started_at": None,
                "finished_at": None,
            }
            for t in table_names
        },
    }
    save_tracker(job_id, state)
    return state


def load_tracker(job_id: str) -> dict[str, Any] | None:
    path = _tracker_path(job_id)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def save_tracker(job_id: str, state: dict[str, Any]) -> None:
    state["updated_at"] = time.time()
    _tracker_path(job_id).write_text(json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8")


def mark_table_started(job_id: str, table: str, row_count_before: int | None) -> dict[str, Any]:
    state = load_tracker(job_id) or init_tracker(job_id, [])
    table_state = state["tables"].setdefault(
        table,
        {"status": "pending", "attempts": 0, "last_error": None, "row_count_before": None, "started_at": None, "finished_at": None},
    )
    table_state["status"] = "in_progress"
    table_state["attempts"] = int(table_state.get("attempts", 0)) + 1
    table_state["row_count_before"] = row_count_before
    table_state["started_at"] = time.time()
    table_state["finished_at"] = None
    table_state["last_error"] = None
    state["current_table"] = table
    save_tracker(job_id, state)
    return state


def mark_table_completed(job_id: str, table: str) -> dict[str, Any]:
    state = load_tracker(job_id) or init_tracker(job_id, [])
    table_state = state["tables"].setdefault(table, {})
    table_state["status"] = "completed"
    table_state["finished_at"] = time.time()
    if table not in state["completed_tables"]:
        state["completed_tables"].append(table)
    if table in state["failed_tables"]:
        state["failed_tables"].remove(table)
    state["current_table"] = None
    save_tracker(job_id, state)
    return state


def mark_table_failed(job_id: str, table: str, error: str) -> dict[str, Any]:
    state = load_tracker(job_id) or init_tracker(job_id, [])
    table_state = state["tables"].setdefault(table, {})
    table_state["status"] = "failed"
    table_state["last_error"] = error
    table_state["finished_at"] = time.time()
    if table not in state["failed_tables"]:
        state["failed_tables"].append(table)
    state["current_table"] = table
    save_tracker(job_id, state)
    return state
