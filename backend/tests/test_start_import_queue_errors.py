from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError as RedisConnectionError

from app.main import app


class BrokenQueue:
    class _Conn:
        @staticmethod
        def ping() -> None:
            raise RedisConnectionError("refused")

    connection = _Conn()

    @staticmethod
    def enqueue(*args, **kwargs):  # pragma: no cover
        raise AssertionError("enqueue should not be called when ping fails")


def test_start_import_falls_back_to_local_thread_if_queue_is_down(monkeypatch) -> None:
    monkeypatch.setattr("app.api.imports.get_queue", lambda: BrokenQueue())
    client = TestClient(app)

    upload = client.post("/api/imports/upload", files={"file": ("small.sql", b"SELECT 1;")})
    assert upload.status_code == 200
    job_id = upload.json()["job_id"]

    payload = {
        "job_id": job_id,
        "host": "127.0.0.1",
        "username": "root",
        "password": "",
        "database": "test",
        "stop_on_error": True,
        "max_errors": 100,
        "show_query_logs": False,
        "resume": False,
    }
    resp = client.post("/api/imports/start", json=payload)
    assert resp.status_code == 200
    assert resp.json()["ok"] is True
    assert resp.json()["execution_mode"] == "local_thread"

    status = client.get(f"/api/imports/{job_id}/status")
    assert status.status_code == 200
    # Local fallback should not regress the job into unknown status.
    assert status.json()["state"]["status"] in {"queued", "running", "failed", "completed", "cancelled"}
