from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Any

import mysql.connector
from mysql.connector import Error as MySQLError

from app.config import get_settings
from app.services.job_state import append_log, load_state, update_state
from app.services.sql_stream_parser import iter_sql_statements
from app.services.tracker import (
    init_tracker,
    load_tracker,
    mark_table_completed,
    mark_table_failed,
    mark_table_started,
)


class ImportCancelled(Exception):
    pass


SQL_MODE_SET_PATTERN = re.compile(r"^\s*SET\s+(?:SESSION\s+)?sql_mode", re.IGNORECASE)


def _table_exists(cursor: Any, db: str, table: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
        LIMIT 1
        """,
        (db, table),
    )
    return cursor.fetchone() is not None


def _row_count(cursor: Any, table: str) -> int | None:
    try:
        cursor.execute(f"SELECT COUNT(*) FROM `{table.replace('`', '``')}`")
        row = cursor.fetchone()
        return int(row[0]) if row else 0
    except Exception:
        return None


def _drop_table(cursor: Any, conn: Any, table: str) -> None:
    cursor.execute(f"DROP TABLE IF EXISTS `{table.replace('`', '``')}`")
    conn.commit()


def _load_existing_tables(cursor: Any, db: str) -> set[str]:
    cursor.execute(
        """
        SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA=%s AND TABLE_TYPE='BASE TABLE'
        """,
        (db,),
    )
    return {str(row[0]) for row in cursor.fetchall()}


def _existing_fk_children(cursor: Any, db: str, parent_table: str) -> set[str]:
    cursor.execute(
        """
        SELECT DISTINCT TABLE_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA=%s
          AND REFERENCED_TABLE_NAME=%s
          AND REFERENCED_TABLE_SCHEMA=%s
        """,
        (db, parent_table, db),
    )
    return {str(row[0]) for row in cursor.fetchall()}


def _is_fk_create_error(exc: Exception | None) -> bool:
    if exc is None:
        return False
    errno = getattr(exc, "errno", None)
    msg = str(exc).lower()
    return errno == 1005 and ("incorrectly formed" in msg or "foreign key constraint fails" in msg)


def _is_connection_lost(exc: Exception | None) -> bool:
    if exc is None:
        return False
    errno = getattr(exc, "errno", None)
    msg = str(exc).lower()
    return errno in {2006, 2013, 2055} or "lost connection" in msg


def _compute_reconnect_wait(
    attempt_index: int, base_seconds: float, max_seconds: float, rest_every: int, rest_seconds: float
) -> float:
    wait = min(base_seconds * (2 ** max(attempt_index - 1, 0)), max_seconds)
    if rest_every > 0 and attempt_index > 0 and attempt_index % rest_every == 0:
        wait += max(rest_seconds, 0.0)
    return max(wait, 0.1)


def _normalize_session_sql_mode_for_legacy_dates(cursor: Any) -> None:
    # Keep source SQL unchanged while allowing legacy zero-date defaults.
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


def import_split_tables(
    job_id: str,
    mysql_config: dict[str, Any],
    manifest: dict[str, Any],
    stop_on_error: bool,
    max_errors: int,
    show_query_logs: bool,
    resume: bool,
) -> None:
    settings = get_settings()
    tables = manifest.get("tables", [])
    table_names = [t["name"] for t in tables]
    tracker = load_tracker(job_id) if resume else None
    if not tracker:
        tracker = init_tracker(job_id, table_names)

    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()
    try:
        def _apply_session_settings() -> None:
            # Large dump restore can require out-of-order drops/creates across related tables.
            # Disable FK/unique checks in this import session and keep legacy-date compatibility.
            cursor.execute("SET FOREIGN_KEY_CHECKS=0")
            cursor.execute("SET UNIQUE_CHECKS=0")
            _normalize_session_sql_mode_for_legacy_dates(cursor)
            conn.commit()

        _apply_session_settings()
        append_log(
            job_id,
            "INFO",
            "Session constraints relaxed for import (FOREIGN_KEY_CHECKS=0, UNIQUE_CHECKS=0, legacy-date SQL mode).",
        )

        table_meta: dict[str, dict[str, Any]] = {
            name: {
                "name": name,
                "status": "pending",
                "parsed": 0,
                "executed": 0,
                "failed": 0,
                "last_operation": None,
                "started_at": None,
                "finished_at": None,
                "duration_seconds": None,
            }
            for name in table_names
        }

        def _tables_payload() -> list[dict[str, Any]]:
            return [table_meta[name] for name in table_names if name in table_meta]

        def _check_cancel() -> None:
            state = load_state(job_id)
            if state.stop_requested:
                append_log(job_id, "WARN", "Cancel requested by user.")
                update_state(
                    job_id,
                    status="cancelled",
                    message="Import cancelled by user",
                    finished_at=time.time(),
                    meta={**state.meta, "tables": _tables_payload()},
                )
                raise ImportCancelled("Import cancelled by user")

        def _execute_with_reconnect(statement: str, context: str) -> None:
            nonlocal conn, cursor
            reconnect_count = 0
            reconnect_limit = max(int(settings.reconnect_attempts), 0)
            while True:
                try:
                    cursor.execute(statement)
                    return
                except MySQLError as exc:
                    if not _is_connection_lost(exc):
                        raise
                    reconnect_count += 1
                    if reconnect_limit > 0 and reconnect_count > reconnect_limit:
                        raise
                    wait_seconds = _compute_reconnect_wait(
                        reconnect_count,
                        base_seconds=max(float(settings.reconnect_base_seconds), 0.1),
                        max_seconds=max(float(settings.reconnect_max_seconds), 1.0),
                        rest_every=max(int(settings.reconnect_rest_every), 0),
                        rest_seconds=max(float(settings.reconnect_rest_seconds), 0.0),
                    )
                    append_log(
                        job_id,
                        "WARNING",
                        f"MySQL connection lost during {context}; reconnecting "
                        f"(attempt={reconnect_count}"
                        + (f"/{reconnect_limit}" if reconnect_limit > 0 else "/unlimited")
                        + f", wait={wait_seconds:.1f}s).",
                    )
                    time.sleep(wait_seconds)
                    _check_cancel()
                    try:
                        cursor.close()
                    except Exception:
                        pass
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = mysql.connector.connect(**mysql_config)
                    cursor = conn.cursor()
                    _apply_session_settings()

        parsed = int(load_state(job_id).parsed_queries or 0)
        executed = int(load_state(job_id).executed_queries or 0)
        failed = int(load_state(job_id).failed_queries or 0)
        skipped = int(load_state(job_id).skipped_queries or 0)

        global_file = manifest.get("global_file")

        completed_in_tracker = list(tracker.get("completed_tables", []))
        replay_table = completed_in_tracker[-1] if (resume and completed_in_tracker) else None
        skip_tables = set(completed_in_tracker[:-1]) if (resume and completed_in_tracker) else set()
        existing_tables = _load_existing_tables(cursor, str(mysql_config["database"]))

        if manifest.get("missing_create_tables"):
            append_log(
                job_id,
                "WARNING",
                "Some table files have no CREATE TABLE statement and may be incomplete: "
                + ", ".join(manifest["missing_create_tables"]),
            )

        if resume:
            if replay_table:
                append_log(job_id, "INFO", f"Resume replay table: {replay_table}")
            if skip_tables:
                append_log(job_id, "INFO", f"Resume skip tables: {len(skip_tables)} already completed.")

        failed_count = 0
        total = len(tables)
        completed_tables_count = len(completed_in_tracker)
        for idx, entry in enumerate(tables, start=1):
            _check_cancel()
            table = entry["name"]
            table_file = Path(entry["file"])
            if table in skip_tables:
                append_log(job_id, "INFO", f"Skip completed table: {table}")
                skipped += 1
                completed_tables_count += 1
                table_meta[table]["status"] = "done"
                table_meta[table]["finished_at"] = time.time()
                table_meta[table]["duration_seconds"] = 0.0
                progress = round((completed_tables_count / max(total, 1)) * 100, 2)
                update_state(
                    job_id,
                    status="running",
                    message=f"Skipping table: {table}",
                    percent=progress,
                    parsed_queries=parsed,
                    executed_queries=executed,
                    failed_queries=failed,
                    skipped_queries=skipped,
                    meta={
                        **load_state(job_id).meta,
                        "current_table": table,
                        "table_index": idx,
                        "table_total": total,
                        "completed_tables": completed_tables_count,
                        "tables": _tables_payload(),
                    },
                )
                continue

            # Requested behavior:
            # - If table exists, skip it.
            # - In resume mode, replay only the last completed table.
            if table in existing_tables and not (resume and table == replay_table):
                append_log(job_id, "WARNING", f"Table already exists; skipped table {table}.")
                skipped += 1
                completed_tables_count += 1
                table_meta[table]["status"] = "done"
                table_meta[table]["finished_at"] = time.time()
                table_meta[table]["duration_seconds"] = 0.0
                progress = round((completed_tables_count / max(total, 1)) * 100, 2)
                update_state(
                    job_id,
                    status="running",
                    message=f"Skipping existing table: {table}",
                    percent=progress,
                    parsed_queries=parsed,
                    executed_queries=executed,
                    failed_queries=failed,
                    skipped_queries=skipped,
                    meta={
                        **load_state(job_id).meta,
                        "current_table": table,
                        "table_index": idx,
                        "table_total": total,
                        "completed_tables": completed_tables_count,
                        "tables": _tables_payload(),
                    },
                )
                continue

            # If importing into a partially existing schema, parent-table CREATE can fail
            # (errno 150) when existing child FKs already reference it before keys are added.
            # In that case, skip this table and continue instead of hard-failing the whole job.
            fk_children = _existing_fk_children(cursor, str(mysql_config["database"]), table)
            if not resume and table not in existing_tables and fk_children:
                append_log(
                    job_id,
                    "WARNING",
                    "Skipped table "
                    f"{table}: existing FK dependencies from {', '.join(sorted(fk_children))}. "
                    "Use Drop Tables first for a full clean import.",
                )
                skipped += 1
                completed_tables_count += 1
                table_meta[table]["status"] = "done"
                table_meta[table]["finished_at"] = time.time()
                table_meta[table]["duration_seconds"] = 0.0
                progress = round((completed_tables_count / max(total, 1)) * 100, 2)
                update_state(
                    job_id,
                    status="running",
                    message=f"Skipping dependent table: {table}",
                    percent=progress,
                    parsed_queries=parsed,
                    executed_queries=executed,
                    failed_queries=failed,
                    skipped_queries=skipped,
                    meta={
                        **load_state(job_id).meta,
                        "current_table": table,
                        "table_index": idx,
                        "table_total": total,
                        "completed_tables": completed_tables_count,
                        "tables": _tables_payload(),
                    },
                )
                continue

            tstate = tracker.get("tables", {}).get(table, {})
            attempts = int(tstate.get("attempts", 0))

            # Resume behavior:
            # - Skip all previously completed tables except the last one.
            # - Last completed table is replayed from scratch (drop and import again).
            if resume and table == replay_table and _table_exists(cursor, mysql_config["database"], table):
                append_log(job_id, "WARN", f"Resume replay: dropping last completed table {table}")
                _drop_table(cursor, conn, table)
                existing_tables.discard(table)
            elif resume and attempts > 0 and _table_exists(cursor, mysql_config["database"], table):
                append_log(job_id, "WARN", f"Recovery: dropping partially imported table {table}")
                _drop_table(cursor, conn, table)
                existing_tables.discard(table)

            row_before = _row_count(cursor, table) if _table_exists(cursor, mysql_config["database"], table) else 0
            mark_table_started(job_id, table, row_before)
            table_started = time.time()
            append_log(job_id, "INFO", f"Importing table {idx}/{total}: {table}")
            table_meta[table]["status"] = "running"
            table_meta[table]["last_operation"] = "IMPORT"
            table_meta[table]["started_at"] = table_started
            table_meta[table]["finished_at"] = None
            table_meta[table]["duration_seconds"] = None
            update_state(
                job_id,
                status="running",
                message=f"Importing table: {table}",
                percent=round((completed_tables_count / max(total, 1)) * 100, 2),
                parsed_queries=parsed,
                executed_queries=executed,
                failed_queries=failed,
                skipped_queries=skipped,
                meta={
                    **load_state(job_id).meta,
                    "current_table": table,
                    "table_index": idx,
                    "table_total": total,
                    "completed_tables": completed_tables_count,
                    "tables": _tables_payload(),
                },
            )

            table_retry_limit = max(int(settings.transient_retry_attempts), 1)
            imported_ok = False
            last_exc: Exception | None = None
            table_statement_count = 0
            for attempt in range(1, table_retry_limit + 1):
                _check_cancel()
                try:
                    for stmt, _ in iter_sql_statements(table_file, chunk_size=settings.parser_chunk_size):
                        _check_cancel()
                        if show_query_logs:
                            append_log(job_id, "DEBUG", stmt[:800])
                        _execute_with_reconnect(stmt, f"table {table}")
                        if SQL_MODE_SET_PATTERN.search(stmt):
                            _normalize_session_sql_mode_for_legacy_dates(cursor)
                        parsed += 1
                        executed += 1
                        table_statement_count += 1
                        table_meta[table]["parsed"] += 1
                        table_meta[table]["executed"] += 1
                        table_meta[table]["last_operation"] = "QUERY"
                        if table_statement_count % max(settings.progress_emit_interval_queries, 25) == 0:
                            append_log(
                                job_id,
                                "INFO",
                                f"Working on table {table}: executed {table_meta[table]['executed']} statements.",
                            )
                            update_state(
                                job_id,
                                status="running",
                                message=f"Importing table: {table}",
                                percent=round((completed_tables_count / max(total, 1)) * 100, 2),
                                parsed_queries=parsed,
                                executed_queries=executed,
                                failed_queries=failed,
                                skipped_queries=skipped,
                                meta={
                                    **load_state(job_id).meta,
                                    "current_table": table,
                                    "table_index": idx,
                                    "table_total": total,
                                    "completed_tables": completed_tables_count,
                                    "tables": _tables_payload(),
                                },
                            )
                    conn.commit()
                    imported_ok = True
                    break
                except MySQLError as exc:
                    conn.rollback()
                    last_exc = exc
                    failed += 1
                    append_log(job_id, "WARN", f"Table {table} attempt {attempt} failed: {exc}")
                    table_meta[table]["failed"] += 1
                    table_meta[table]["status"] = "error"
                    table_meta[table]["finished_at"] = time.time()
                    table_meta[table]["duration_seconds"] = max(table_meta[table]["finished_at"] - table_started, 0.0)
                    if attempt < table_retry_limit:
                        _drop_table(cursor, conn, table)
                        time.sleep(max(settings.transient_retry_base_seconds, 0.1) * attempt)
                        table_meta[table]["status"] = "running"
                        table_meta[table]["finished_at"] = None
                        table_meta[table]["duration_seconds"] = None

            if imported_ok:
                mark_table_completed(job_id, table)
                dur = time.time() - table_started
                append_log(job_id, "SUCCESS", f"Table {table} imported successfully in {dur:.2f}s")
                completed_tables_count += 1
                table_meta[table]["status"] = "done"
                table_meta[table]["finished_at"] = time.time()
                table_meta[table]["duration_seconds"] = dur
                progress = round((idx / max(total, 1)) * 100, 2)
                update_state(
                    job_id,
                    status="running",
                    message=f"Imported table: {table}",
                    percent=progress,
                    parsed_queries=parsed,
                    executed_queries=executed,
                    failed_queries=failed,
                    skipped_queries=skipped,
                    meta={
                        **load_state(job_id).meta,
                        "current_table": table,
                        "table_index": idx,
                        "table_total": total,
                        "completed_tables": completed_tables_count,
                        "tables": _tables_payload(),
                    },
                )
            else:
                failed_count += 1
                err_text = str(last_exc) if last_exc else "Unknown table import error"
                mark_table_failed(job_id, table, err_text)
                append_log(job_id, "ERROR", f"Table {table} failed: {err_text}")
                table_meta[table]["status"] = "error"
                table_meta[table]["finished_at"] = time.time()
                table_meta[table]["duration_seconds"] = max(table_meta[table]["finished_at"] - table_started, 0.0)

                # Keep long imports moving on dirty/partially-restored schemas where
                # CREATE TABLE can fail with FK metadata conflicts (errno 150).
                if _is_fk_create_error(last_exc):
                    skipped += 1
                    append_log(
                        job_id,
                        "WARNING",
                        f"Skipped table {table} after FK create error. "
                        "For a full clean restore, run Drop Tables first then import again.",
                    )
                    update_state(
                        job_id,
                        parsed_queries=parsed,
                        executed_queries=executed,
                        failed_queries=failed,
                        skipped_queries=skipped,
                        meta={**load_state(job_id).meta, "tables": _tables_payload()},
                    )
                    continue

                update_state(
                    job_id,
                    parsed_queries=parsed,
                    executed_queries=executed,
                    failed_queries=failed,
                    skipped_queries=skipped,
                    meta={**load_state(job_id).meta, "tables": _tables_payload()},
                )
                if stop_on_error or failed_count >= max_errors:
                    raise RuntimeError(f"Stopped on table failure: {table} - {err_text}")

        # Apply cross-table/global statements at the end so FK/constraint statements
        # run after all related tables are created.
        if global_file and Path(global_file).exists():
            append_log(job_id, "INFO", "Applying global SQL statements...")
            for stmt, _ in iter_sql_statements(Path(global_file), chunk_size=settings.parser_chunk_size):
                _check_cancel()
                parsed += 1
                try:
                    _execute_with_reconnect(stmt, "global SQL")
                    if SQL_MODE_SET_PATTERN.search(stmt):
                        _normalize_session_sql_mode_for_legacy_dates(cursor)
                    executed += 1
                except MySQLError as exc:
                    skipped += 1
                    append_log(job_id, "WARNING", f"Skipped global statement after table import: {exc}")
            conn.commit()

    finally:
        try:
            cursor.execute("SET UNIQUE_CHECKS=1")
            cursor.execute("SET FOREIGN_KEY_CHECKS=1")
            conn.commit()
            append_log(job_id, "INFO", "Session constraints restored after import.")
        except Exception:
            pass
        cursor.close()
        conn.close()
