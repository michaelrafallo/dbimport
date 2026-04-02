from __future__ import annotations

import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import mysql.connector
from mysql.connector import Error as MySQLError

from app.config import get_settings
from app.services.importer import ImportCancelled, import_split_tables
from app.services.job_state import append_log, load_state, update_state
from app.services.parser import split_sql_by_table
from app.services.sql_stream_parser import iter_sql_statements

DDL_PREFIXES = ("CREATE ", "ALTER ", "DROP ", "TRUNCATE ", "RENAME ", "USE ", "LOCK ", "UNLOCK ")
SQL_MODE_SET_PATTERN = re.compile(r"^\s*SET\s+(?:SESSION\s+)?sql_mode", re.IGNORECASE)
TABLE_EXISTS_MSG_PATTERN = re.compile(r"table\s+'([^']+)'\s+already exists", re.IGNORECASE)
VALUES_KEYWORD_PATTERN = re.compile(r"\bVALUES\b", re.IGNORECASE)
_TABLE_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"^\s*INSERT\s+INTO\s+`?([A-Za-z0-9_.$-]+)`?", re.IGNORECASE), "INSERT"),
    (re.compile(r"^\s*UPDATE\s+`?([A-Za-z0-9_.$-]+)`?", re.IGNORECASE), "UPDATE"),
    (re.compile(r"^\s*DELETE\s+FROM\s+`?([A-Za-z0-9_.$-]+)`?", re.IGNORECASE), "DELETE"),
    (
        re.compile(
            r"^\s*CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+`?([A-Za-z0-9_.$-]+)`?",
            re.IGNORECASE,
        ),
        "CREATE",
    ),
    (re.compile(r"^\s*ALTER\s+TABLE\s+`?([A-Za-z0-9_.$-]+)`?", re.IGNORECASE), "ALTER"),
    (re.compile(r"^\s*DROP\s+TABLE(?:\s+IF\s+EXISTS)?\s+`?([A-Za-z0-9_.$-]+)`?", re.IGNORECASE), "DROP"),
    (re.compile(r"^\s*TRUNCATE\s+TABLE\s+`?([A-Za-z0-9_.$-]+)`?", re.IGNORECASE), "TRUNCATE"),
    (re.compile(r"^\s*LOCK\s+TABLES?\s+`?([A-Za-z0-9_.$-]+)`?", re.IGNORECASE), "LOCK"),
]


@dataclass
class ImportOptions:
    stop_on_error: bool = True
    max_errors: int = 100
    show_query_logs: bool = False
    resume: bool = False


def _is_transient(error: MySQLError) -> bool:
    transient_codes = {1205, 1213, 2006, 2013, 2055}
    return getattr(error, "errno", None) in transient_codes


def _is_packet_too_large(error: MySQLError) -> bool:
    msg = str(error).lower()
    return getattr(error, "errno", None) in {1153, 2006, 2013} and "max_allowed_packet" in msg


def _is_connection_lost(error: MySQLError) -> bool:
    msg = str(error).lower()
    return getattr(error, "errno", None) in {2006, 2013, 2055} or "lost connection" in msg


def _compute_reconnect_wait(
    attempt_index: int, base_seconds: float, max_seconds: float, rest_every: int, rest_seconds: float
) -> float:
    # Exponential backoff capped by max_seconds, plus periodic cool-down rest.
    wait = min(base_seconds * (2 ** max(attempt_index - 1, 0)), max_seconds)
    if rest_every > 0 and attempt_index > 0 and attempt_index % rest_every == 0:
        wait += max(rest_seconds, 0.0)
    return max(wait, 0.1)


def _is_table_exists_error(error: MySQLError) -> bool:
    msg = str(error).lower()
    return getattr(error, "errno", None) == 1050 or "already exists" in msg


def _extract_table_from_exists_error(error: MySQLError) -> str | None:
    match = TABLE_EXISTS_MSG_PATTERN.search(str(error))
    if not match:
        return None
    raw = match.group(1).strip()
    # MySQL usually returns just table name, but handle db.table too.
    if "." in raw:
        return raw.split(".")[-1].strip("`")
    return raw.strip("`")


def _base_table_name(table_name: str) -> str:
    raw = str(table_name).strip("`")
    if "." in raw:
        raw = raw.split(".")[-1]
    return raw.strip("`")


def _load_existing_tables(cursor: Any, database: str) -> set[str]:
    cursor.execute(
        """
        SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
        """,
        (database,),
    )
    return {str(row[0]) for row in cursor.fetchall()}


def _estimate_eta(started_at: float, bytes_done: int, bytes_total: int) -> int | None:
    elapsed = max(time.time() - started_at, 0.001)
    if bytes_done <= 0:
        return None
    rate = bytes_done / elapsed
    remaining = max(bytes_total - bytes_done, 0)
    return int(remaining / rate) if rate > 0 else None


def _execute_with_retry(cursor: Any, query: str, attempts: int, base_delay: float) -> None:
    for attempt in range(1, attempts + 1):
        try:
            cursor.execute(query)
            return
        except MySQLError as exc:
            if not _is_transient(exc) or attempt == attempts:
                raise
            time.sleep(base_delay * (2 ** (attempt - 1)))


def _normalize_session_sql_mode_for_legacy_dates(cursor: Any) -> None:
    # Many legacy dumps use zero-date defaults. Strip strict zero-date flags
    # so schema can be imported without mutating source SQL.
    cursor.execute(
        """
        SET SESSION sql_mode = (
          SELECT TRIM(BOTH ',' FROM
            REPLACE(
              REPLACE(
                REPLACE(@@SESSION.sql_mode, 'NO_ZERO_DATE', ''),
              'NO_ZERO_IN_DATE', ''),
            ',,', ',')
          )
        )
        """
    )


def _split_insert_statement(statement: str) -> tuple[str, list[str], str] | None:
    values_match = VALUES_KEYWORD_PATTERN.search(statement)
    if not values_match:
        return None

    values_end = values_match.end()
    body_start = values_end
    while body_start < len(statement) and statement[body_start].isspace():
        body_start += 1
    prefix = statement[:body_start]
    body = statement[body_start:]
    tuples: list[str] = []

    i = 0
    n = len(body)
    in_single = False
    in_double = False
    in_backtick = False
    escape_next = False
    depth = 0
    tuple_start = -1
    end_of_tuples = -1

    while i < n:
        ch = body[i]
        if escape_next:
            escape_next = False
            i += 1
            continue
        if in_single:
            if ch == "\\":
                escape_next = True
            elif ch == "'":
                in_single = False
            i += 1
            continue
        if in_double:
            if ch == "\\":
                escape_next = True
            elif ch == '"':
                in_double = False
            i += 1
            continue
        if in_backtick:
            if ch == "`":
                in_backtick = False
            i += 1
            continue

        if ch == "'":
            in_single = True
        elif ch == '"':
            in_double = True
        elif ch == "`":
            in_backtick = True
        elif ch == "(":
            if depth == 0:
                tuple_start = i
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0 and tuple_start >= 0:
                tuples.append(body[tuple_start : i + 1])
                tuple_start = -1
                j = i + 1
                while j < n and body[j].isspace():
                    j += 1
                if j < n and body[j] == ",":
                    i = j
                else:
                    end_of_tuples = i + 1
                    break
        i += 1

    if not tuples:
        return None
    tail = body[end_of_tuples:] if end_of_tuples >= 0 else ""
    return prefix, tuples, tail


def _execute_insert_with_split_retry(
    cursor: Any,
    statement: str,
    attempts: int,
    base_delay: float,
    min_chunk_size: int = 1,
) -> int:
    parsed = _split_insert_statement(statement)
    if not parsed:
        raise RuntimeError("Could not split INSERT for packet-size fallback.")
    prefix, tuples, tail = parsed
    if len(tuples) <= min_chunk_size:
        # Cannot split further.
        _execute_with_retry(cursor, statement, attempts=attempts, base_delay=base_delay)
        return 1

    mid = max(len(tuples) // 2, 1)
    groups = [tuples[:mid], tuples[mid:]]
    executed_parts = 0
    for group in groups:
        if not group:
            continue
        chunk_stmt = f"{prefix}{','.join(group)}{tail}"
        try:
            _execute_with_retry(cursor, chunk_stmt, attempts=attempts, base_delay=base_delay)
            executed_parts += 1
        except MySQLError as exc:
            if _is_packet_too_large(exc) and len(group) > 1:
                executed_parts += _execute_insert_with_split_retry(
                    cursor,
                    chunk_stmt,
                    attempts=attempts,
                    base_delay=base_delay,
                    min_chunk_size=min_chunk_size,
                )
            else:
                raise
    return executed_parts


def _extract_table_context(statement: str) -> tuple[str | None, str | None]:
    for pattern, operation in _TABLE_PATTERNS:
        match = pattern.search(statement)
        if match:
            return match.group(1), operation
    return None, None


def _ensure_table_meta(table_meta: dict[str, dict[str, Any]], table_order: list[str], table_name: str) -> dict[str, Any]:
    entry = table_meta.get(table_name)
    if entry is None:
        entry = {
            "name": table_name,
            "status": "pending",
            "parsed": 0,
            "executed": 0,
            "failed": 0,
            "last_operation": None,
            "started_at": None,
            "finished_at": None,
            "duration_seconds": None,
        }
        table_meta[table_name] = entry
        table_order.append(table_name)
    return entry


def _serialize_tables(table_meta: dict[str, dict[str, Any]], table_order: list[str]) -> list[dict[str, Any]]:
    return [table_meta[name] for name in table_order if name in table_meta]


def _plan_resume_tables(table_meta_raw: list[dict[str, Any]]) -> tuple[set[str], str | None]:
    done_tables = [str(t.get("name")) for t in table_meta_raw if t.get("status") == "done" and t.get("name")]
    if not done_tables:
        return set(), None
    last_completed = done_tables[-1]
    skip = set(done_tables[:-1])
    return skip, last_completed


def _quote_table_ref(table_name: str) -> str:
    parts = [p for p in table_name.split(".") if p]
    if not parts:
        safe = table_name.replace("`", "``")
        return f"`{safe}`"
    return ".".join(f"`{p.replace('`', '``')}`" for p in parts)


def _mark_table_running(entry: dict[str, Any]) -> None:
    if not entry.get("started_at"):
        entry["started_at"] = time.time()
    entry["status"] = "running"
    entry["finished_at"] = None
    entry["duration_seconds"] = None


def _mark_table_done(entry: dict[str, Any]) -> float:
    started = entry.get("started_at") or time.time()
    finished = time.time()
    duration = max(finished - started, 0.0)
    entry["status"] = "done"
    entry["finished_at"] = finished
    entry["duration_seconds"] = duration
    return duration


def _mark_table_error(entry: dict[str, Any]) -> float:
    started = entry.get("started_at") or time.time()
    finished = time.time()
    duration = max(finished - started, 0.0)
    entry["status"] = "error"
    entry["finished_at"] = finished
    entry["duration_seconds"] = duration
    return duration


def run_import_job(
    job_id: str,
    mysql_config: dict[str, Any],
    sql_file_path: str,
    options_raw: dict[str, Any] | None = None,
) -> None:
    options = ImportOptions(**(options_raw or {}))
    sql_file = Path(sql_file_path)
    total_bytes = sql_file.stat().st_size
    prev_state = load_state(job_id)

    update_state(
        job_id,
        status="running",
        message="Preparing table-based import",
        started_at=prev_state.started_at or time.time(),
        bytes_total=total_bytes,
        bytes_processed=0,
        error=None,
        meta={**prev_state.meta, "last_offset": 0},
    )
    append_log(job_id, "INFO", f"Import started for {sql_file.name}")
    try:
        append_log(job_id, "INFO", "Parsing SQL into per-table files...")
        manifest = split_sql_by_table(job_id, sql_file)
        table_count = len(manifest.get("tables", []))
        append_log(job_id, "INFO", f"Prepared {table_count} table SQL files.")
        if table_count:
            preview = ", ".join(Path(t["file"]).name for t in manifest["tables"][:5])
            append_log(job_id, "INFO", f"Split file preview: {preview}")
        pending_tables = [
            {
                "name": str(t.get("name", "")),
                "status": "pending",
                "parsed": 0,
                "executed": 0,
                "failed": 0,
                "last_operation": None,
                "started_at": None,
                "finished_at": None,
                "duration_seconds": None,
            }
            for t in manifest.get("tables", [])
            if t.get("name")
        ]
        update_state(
            job_id,
            status="running",
            message="Per-table SQL files prepared",
            percent=5.0 if table_count > 0 else 100.0,
            bytes_total=total_bytes,
            bytes_processed=0,
            meta={
                **load_state(job_id).meta,
                "manifest": manifest,
                "tables": pending_tables,
                "table_total": table_count,
                "completed_tables": 0,
            },
        )

        import_split_tables(
            job_id=job_id,
            mysql_config=mysql_config,
            manifest=manifest,
            stop_on_error=options.stop_on_error,
            max_errors=options.max_errors,
            show_query_logs=options.show_query_logs,
            resume=options.resume,
        )

        final_state = load_state(job_id)
        if final_state.status != "cancelled":
            update_state(
                job_id,
                status="completed",
                message="Import completed",
                percent=100.0,
                bytes_total=total_bytes,
                bytes_processed=total_bytes,
                eta_seconds=0,
                finished_at=time.time(),
            )
            append_log(job_id, "SUCCESS", "Import completed successfully.")
    except ImportCancelled:
        return
    except Exception as exc:
        current = load_state(job_id)
        append_log(job_id, "ERROR", str(exc))
        update_state(
            job_id,
            status="failed",
            message="Import failed",
            finished_at=time.time(),
            error=str(exc),
            parsed_queries=current.parsed_queries,
            executed_queries=current.executed_queries,
            failed_queries=(current.failed_queries or 0) + 1,
            skipped_queries=current.skipped_queries,
            meta=current.meta,
        )
        raise
