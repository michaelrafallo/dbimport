from __future__ import annotations

from rq import Connection, Worker

from app.queue import get_redis


def main() -> None:
    redis_conn = get_redis()
    with Connection(redis_conn):
        worker = Worker(["imports"])
        worker.work(with_scheduler=True)


if __name__ == "__main__":
    main()
