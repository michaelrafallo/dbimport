from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from app.config import get_settings
from app.services.sql_stream_parser import iter_sql_statements

_IDENT = r"(?:`[^`]+`|[A-Za-z0-9_.$-]+)"
_TABLE_REF = rf"({_IDENT}(?:\s*\.\s*{_IDENT})?)"

CREATE_TABLE_RE = re.compile(
    rf"^\s*CREATE\s+(?:TEMPORARY\s+)?TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+{_TABLE_REF}",
    re.IGNORECASE,
)
INSERT_INTO_RE = re.compile(
    rf"^\s*INSERT(?:\s+(?:LOW_PRIORITY|DELAYED|HIGH_PRIORITY|IGNORE))*\s+INTO\s+{_TABLE_REF}",
    re.IGNORECASE,
)
REPLACE_INTO_RE = re.compile(rf"^\s*REPLACE(?:\s+(?:LOW_PRIORITY|DELAYED))*\s+INTO\s+{_TABLE_REF}", re.IGNORECASE)
ALTER_TABLE_RE = re.compile(rf"^\s*ALTER\s+TABLE\s+{_TABLE_REF}", re.IGNORECASE)
DROP_TABLE_RE = re.compile(rf"^\s*DROP\s+TABLE(?:\s+IF\s+EXISTS)?\s+{_TABLE_REF}", re.IGNORECASE)


def _table_from_statement(statement: str) -> tuple[str | None, str | None]:
    # Many dumps prepend comments before SQL statements (phpMyAdmin style).
    # Strip leading comments so table detection can still match CREATE/INSERT.
    text = statement.lstrip("\ufeff")
    while True:
        stripped = text.lstrip()
        if not stripped:
            break
        if stripped.startswith("--") or stripped.startswith("#"):
            nl = stripped.find("\n")
            text = stripped[nl + 1 :] if nl >= 0 else ""
            continue
        if stripped.startswith("/*"):
            end = stripped.find("*/")
            if end >= 0:
                text = stripped[end + 2 :]
                continue
        text = stripped
        break

    for regex, kind in (
        (CREATE_TABLE_RE, "CREATE"),
        (INSERT_INTO_RE, "INSERT"),
        (REPLACE_INTO_RE, "INSERT"),
        (ALTER_TABLE_RE, "ALTER"),
        (DROP_TABLE_RE, "DROP"),
    ):
        m = regex.search(text)
        if m:
            raw = m.group(1).replace(" ", "")
            table = raw.split(".")[-1].strip("`")
            return table, kind
    return None, None


def _quote(table: str) -> str:
    return f"`{table.replace('`', '``')}`"


def split_sql_by_table(job_id: str, sql_file: Path) -> dict[str, Any]:
    settings = get_settings()
    out_dir = settings.temp_dir / job_id / "tables"
    out_dir.mkdir(parents=True, exist_ok=True)

    global_file = out_dir / "__global__.sql"
    tables: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    handles: dict[Path, Any] = {}

    def append(path: Path, stmt: str) -> None:
        handle = handles.get(path)
        if handle is None:
            handle = path.open("a", encoding="utf-8")
            handles[path] = handle
        handle.write(f"{stmt.strip()};\n\n")

    try:
        for statement, _ in iter_sql_statements(sql_file, chunk_size=settings.parser_chunk_size):
            table, kind = _table_from_statement(statement)
            if not table:
                append(global_file, statement)
                continue

            if table not in tables:
                file_path = out_dir / f"{table}.sql"
                tables[table] = {
                    "name": table,
                    "file": str(file_path),
                    "statement_count": 0,
                    "has_create": False,
                    "insert_count": 0,
                }
                order.append(table)

            entry = tables[table]
            path = Path(entry["file"])
            if kind == "CREATE" and not entry["has_create"]:
                append(path, f"DROP TABLE IF EXISTS {_quote(table)}")
                append(path, statement)
                entry["has_create"] = True
                entry["statement_count"] += 2
            elif kind == "DROP":
                # Skip original drops since we control table lifecycle.
                continue
            else:
                append(path, statement)
                entry["statement_count"] += 1
                if kind == "INSERT":
                    entry["insert_count"] += 1
    finally:
        for handle in handles.values():
            handle.close()

    missing_create = [t for t in order if not tables[t].get("has_create")]

    manifest = {
        "job_id": job_id,
        "source_file": str(sql_file),
        "split_dir": str(out_dir),
        "global_file": str(global_file) if global_file.exists() else None,
        "tables": [tables[t] for t in order],
        "table_count": len(order),
        "missing_create_tables": missing_create,
    }
    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=True, indent=2), encoding="utf-8")
    return manifest
