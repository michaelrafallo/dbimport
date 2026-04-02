from pathlib import Path

from app.services.sql_stream_parser import iter_sql_statements


def _write(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "dump.sql"
    p.write_text(content, encoding="utf-8")
    return p


def test_parses_basic_statements(tmp_path: Path) -> None:
    sql = "CREATE TABLE t(id INT);\nINSERT INTO t VALUES (1);\n"
    p = _write(tmp_path, sql)
    statements = [s for s, _ in iter_sql_statements(p)]
    assert statements == ["CREATE TABLE t(id INT)", "INSERT INTO t VALUES (1)"]


def test_handles_delimiter_blocks(tmp_path: Path) -> None:
    sql = """
DELIMITER $$
CREATE PROCEDURE hello()
BEGIN
  SELECT 1;
END$$
DELIMITER ;
INSERT INTO t VALUES (2);
"""
    p = _write(tmp_path, sql)
    statements = [s for s, _ in iter_sql_statements(p)]
    assert len(statements) == 2
    assert "CREATE PROCEDURE hello()" in statements[0]
    assert statements[1] == "INSERT INTO t VALUES (2)"


def test_ignores_semicolon_inside_strings(tmp_path: Path) -> None:
    sql = "INSERT INTO t VALUES ('a;b;c');\nINSERT INTO t VALUES (3);\n"
    p = _write(tmp_path, sql)
    statements = [s for s, _ in iter_sql_statements(p)]
    assert statements == ["INSERT INTO t VALUES ('a;b;c')", "INSERT INTO t VALUES (3)"]
