from app.services.import_executor import _extract_table_from_exists_error, _is_table_exists_error


class _Err:
    def __init__(self, errno: int | None, msg: str) -> None:
        self.errno = errno
        self._msg = msg

    def __str__(self) -> str:
        return self._msg


def test_detects_errno_1050_as_table_exists() -> None:
    err = _Err(1050, "Table 'x' already exists")
    assert _is_table_exists_error(err) is True


def test_detects_message_as_table_exists() -> None:
    err = _Err(None, "Table 'wp_actionscheduler_actions' already exists")
    assert _is_table_exists_error(err) is True


def test_extracts_table_name_from_exists_error_message() -> None:
    err = _Err(1050, "1050 (42S01): Table 'wp_actionscheduler_actions' already exists")
    assert _extract_table_from_exists_error(err) == "wp_actionscheduler_actions"
