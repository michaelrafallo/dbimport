from app.services.import_executor import _plan_resume_tables, _quote_table_ref


def test_plan_resume_tables_replays_last_completed_and_skips_earlier() -> None:
    table_meta = [
        {"name": "wp_users", "status": "done"},
        {"name": "wp_posts", "status": "done"},
        {"name": "wp_comments", "status": "running"},
    ]
    skip, replay = _plan_resume_tables(table_meta)
    assert replay == "wp_posts"
    assert skip == {"wp_users"}


def test_quote_table_ref_handles_schema_and_table() -> None:
    assert _quote_table_ref("mydb.wp_posts") == "`mydb`.`wp_posts`"
    assert _quote_table_ref("wp_users") == "`wp_users`"
