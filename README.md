# Production-Ready MySQL Importer (Python)

Web-based MySQL dump importer optimized for very large SQL files (tested design for 5GB+), with background workers, streaming parser/execution, and real-time progress/logging.

## Features

- Supports `.sql` and `.zip` uploads (zip extracted server-side with safety checks).
- Streams file processing and parses statements incrementally (no full-file memory load).
- Handles multiline SQL and custom `DELIMITER` blocks.
- Background import jobs via RQ worker + Redis.
- Live progress over WebSocket with polling fallback.
- Error policy: stop on first error or continue/skip with max error cap.
- Cancel running imports and download per-job log files.
- Checkpoint metadata for resume-from-offset behavior.

## Architecture

- Frontend: `frontend/index.html`, `frontend/app.js`, `frontend/styles.css`
- API: `backend/app/main.py`, `backend/app/api/imports.py`
- Services:
  - `backend/app/services/file_service.py`
  - `backend/app/services/sql_stream_parser.py`
  - `backend/app/services/import_executor.py`
  - `backend/app/services/job_state.py`
- Worker: `backend/app/workers/rq_worker.py`

## Local Setup (Windows / XAMPP compatible)

1. Install Python 3.11+ and Redis.
2. Create virtual environment and install deps:
   - `cd backend`
   - `python -m venv .venv`
   - `.venv\\Scripts\\activate`
   - `pip install -r requirements.txt`
3. Start API from repo root:
   - `uvicorn app.main:app --app-dir backend --reload`
4. Start worker in another terminal:
   - `cd backend`
   - `.venv\\Scripts\\activate`
   - `python -m app.workers.rq_worker`
5. Open `http://127.0.0.1:8000`.

## Docker Setup

- `docker compose up --build`
- API: `http://127.0.0.1:8000`

## Usage Flow

1. Enter MySQL host/user/password.
2. Click **Connect** to validate and load databases.
3. Pick DB from dropdown or manually type DB name.
4. Upload `.sql` or `.zip`.
5. Click **Import Database**, confirm modal, and monitor progress/logs.

## 5GB+ Tuning Recommendations

- Keep `PARSER_CHUNK_SIZE` between `4MB` and `16MB` for faster streaming on SSD.
- Keep `IMPORT_BATCH_SIZE` between `500` and `2000` depending on MySQL capacity.
- Increase `PROGRESS_EMIT_INTERVAL_QUERIES` to reduce state I/O overhead.
- Increase MySQL:
  - `max_allowed_packet`
  - `innodb_log_file_size`
  - `net_read_timeout`
- Use local SSD for `backend/storage`.
- Keep query log toggle disabled unless troubleshooting.

## Security Notes

- Credentials are only used at runtime and not written to logs/state files.
- Strict file extension checks and zip inspection limits.
- Safe zip extraction strategy prevents directory traversal.

## Testing

- `cd backend`
- `pytest -q`

## Important Limitations / Next Hardening

- Resume currently continues from byte checkpoint and skips to next newline; statements crossing checkpoint boundaries may replay or skip in edge cases.
- For strict exactly-once resume semantics with huge dumps, add transactional checkpoint markers at statement boundaries plus an idempotency strategy.
- Add optional auth layer before exposing publicly (current scope is single-operator local/admin).
# dbimport
