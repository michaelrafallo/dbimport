from __future__ import annotations

import threading
from typing import Any, Callable


_LOCAL_THREADS: dict[str, threading.Thread] = {}
_LOCAL_LOCK = threading.Lock()


def run_in_background(job_id: str, target: Callable[..., None], *args: Any, **kwargs: Any) -> str:
    def _runner() -> None:
        try:
            target(*args, **kwargs)
        except Exception:
            # Worker function already persists/logs failures in job state.
            pass
        finally:
            with _LOCAL_LOCK:
                _LOCAL_THREADS.pop(job_id, None)

    thread = threading.Thread(target=_runner, name=f"import-job-{job_id[:8]}", daemon=True)
    with _LOCAL_LOCK:
        _LOCAL_THREADS[job_id] = thread
    thread.start()
    return thread.name


def is_local_job_alive(job_id: str) -> bool:
    with _LOCAL_LOCK:
        t = _LOCAL_THREADS.get(job_id)
    return bool(t and t.is_alive())
