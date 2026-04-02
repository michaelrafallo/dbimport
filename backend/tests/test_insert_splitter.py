from app.services.import_executor import _split_insert_statement


def test_split_insert_handles_newline_after_values() -> None:
    stmt = "INSERT INTO `t` (`a`) VALUES\n(1),(2),(3);"
    parsed = _split_insert_statement(stmt)
    assert parsed is not None
    prefix, tuples, tail = parsed
    assert "VALUES" in prefix.upper()
    assert len(tuples) == 3
    assert tail.strip() == ";"


def test_split_insert_handles_on_duplicate_tail() -> None:
    stmt = (
        "INSERT INTO `t` (`id`,`v`) VALUES (1,'a'),(2,'b') "
        "ON DUPLICATE KEY UPDATE `v`=VALUES(`v`);"
    )
    parsed = _split_insert_statement(stmt)
    assert parsed is not None
    prefix, tuples, tail = parsed
    assert len(tuples) == 2
    assert "ON DUPLICATE KEY UPDATE" in tail.upper()
    assert prefix.upper().endswith("VALUES ")
