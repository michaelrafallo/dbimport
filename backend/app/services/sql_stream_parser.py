from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Generator


@dataclass
class ParserState:
    delimiter: str = ";"
    in_single_quote: bool = False
    in_double_quote: bool = False
    in_backtick: bool = False
    in_block_comment: bool = False
    in_line_comment: bool = False
    escape_next: bool = False


def _is_delimiter_command(line: str) -> tuple[bool, str]:
    stripped = line.strip()
    if not stripped:
        return False, ""
    if not stripped.upper().startswith("DELIMITER "):
        return False, ""
    parts = stripped.split(maxsplit=1)
    if len(parts) != 2 or not parts[1]:
        return False, ""
    return True, parts[1].strip()


def _can_switch_delimiter(statement_chars: list[str]) -> bool:
    if not statement_chars:
        return True
    return "".join(statement_chars).strip() == ""


def _update_state(state: ParserState, prev_char: str, cur: str, nxt: str) -> None:
    if state.in_line_comment:
        if cur == "\n":
            state.in_line_comment = False
        return

    if state.in_block_comment:
        if prev_char == "*" and cur == "/":
            state.in_block_comment = False
        return

    if state.escape_next:
        state.escape_next = False
        return

    if state.in_single_quote:
        if cur == "\\":
            state.escape_next = True
        elif cur == "'":
            state.in_single_quote = False
        return

    if state.in_double_quote:
        if cur == "\\":
            state.escape_next = True
        elif cur == '"':
            state.in_double_quote = False
        return

    if state.in_backtick:
        if cur == "`":
            state.in_backtick = False
        return

    # Outside strings/comments.
    if cur == "#" and not state.in_single_quote:
        state.in_line_comment = True
    elif cur == "-" and nxt == "-":
        state.in_line_comment = True
    elif cur == "/" and nxt == "*":
        state.in_block_comment = True
    elif cur == "'":
        state.in_single_quote = True
    elif cur == '"':
        state.in_double_quote = True
    elif cur == "`":
        state.in_backtick = True


def iter_sql_statements(
    sql_file: Path, chunk_size: int = 1024 * 1024, start_offset: int = 0
) -> Generator[tuple[str, int], None, None]:
    state = ParserState()
    statement_chars: list[str] = []
    rolling_tail = ""
    delimiter_len = len(state.delimiter)
    bytes_processed = start_offset

    with sql_file.open("r", encoding="utf-8", errors="replace", newline="") as f:
        if start_offset > 0:
            f.seek(start_offset)
            _ = f.readline()  # resume from next complete line
            bytes_processed = f.tell()

        pending_line = ""
        prev_char = ""
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break

            bytes_processed = f.tell()
            data = pending_line + chunk
            lines = data.splitlines(keepends=True)
            if lines and not lines[-1].endswith(("\n", "\r")):
                pending_line = lines.pop()
            else:
                pending_line = ""

            for line in lines:
                is_cmd, new_delim = _is_delimiter_command(line)
                if is_cmd and _can_switch_delimiter(statement_chars):
                    statement_chars = []
                    state.delimiter = new_delim
                    delimiter_len = len(state.delimiter)
                    rolling_tail = ""
                    prev_char = ""
                    continue

                i = 0
                while i < len(line):
                    cur = line[i]
                    nxt = line[i + 1] if i + 1 < len(line) else ""
                    # Fast handling for comment openings when not in strings/comments.
                    if (
                        not state.in_single_quote
                        and not state.in_double_quote
                        and not state.in_backtick
                        and not state.in_block_comment
                        and not state.in_line_comment
                    ):
                        if cur == "#" or (cur == "-" and nxt == "-"):
                            state.in_line_comment = True
                        elif cur == "/" and nxt == "*":
                            state.in_block_comment = True

                    statement_chars.append(cur)
                    _update_state(state, prev_char, cur, nxt)

                    if (
                        not state.in_single_quote
                        and not state.in_double_quote
                        and not state.in_backtick
                        and not state.in_block_comment
                        and not state.in_line_comment
                    ):
                        rolling_tail = (rolling_tail + cur)[-delimiter_len:]
                        if delimiter_len and rolling_tail == state.delimiter:
                            stmt_chars = statement_chars[:-delimiter_len]
                            stmt = "".join(stmt_chars).strip()
                            statement_chars = []
                            rolling_tail = ""
                            if stmt:
                                yield stmt, bytes_processed
                    else:
                        rolling_tail = ""

                    prev_char = cur
                    i += 1

        if pending_line:
            statement_chars.append(pending_line)

        tail = "".join(statement_chars).strip()
        if tail:
            yield tail, bytes_processed
