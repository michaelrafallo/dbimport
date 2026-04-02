from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _resolve_dir(env_name: str, default_relative: str) -> Path:
    configured = os.getenv(env_name, default_relative)
    p = Path(configured)
    if not p.is_absolute():
        p = PROJECT_ROOT / p
    return p.resolve()


@dataclass(frozen=True)
class Settings:
    app_name: str = "MySQL Importer"
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    uploads_dir: Path = _resolve_dir("UPLOADS_DIR", "backend/storage/uploads")
    temp_dir: Path = _resolve_dir("TEMP_DIR", "backend/storage/temp")
    logs_dir: Path = _resolve_dir("LOGS_DIR", "backend/storage/logs")
    state_dir: Path = _resolve_dir("STATE_DIR", "backend/storage/state")
    max_upload_size_bytes: int = int(os.getenv("MAX_UPLOAD_SIZE_BYTES", str(20 * 1024 * 1024 * 1024)))
    max_zip_members: int = int(os.getenv("MAX_ZIP_MEMBERS", "8"))
    max_extracted_zip_size_bytes: int = int(
        os.getenv("MAX_EXTRACTED_ZIP_SIZE_BYTES", str(30 * 1024 * 1024 * 1024))
    )
    import_batch_size: int = int(os.getenv("IMPORT_BATCH_SIZE", "1000"))
    progress_emit_interval_queries: int = int(os.getenv("PROGRESS_EMIT_INTERVAL_QUERIES", "100"))
    parser_chunk_size: int = int(os.getenv("PARSER_CHUNK_SIZE", str(4 * 1024 * 1024)))
    cancel_check_interval_queries: int = int(os.getenv("CANCEL_CHECK_INTERVAL_QUERIES", "50"))
    state_flush_interval_seconds: float = float(os.getenv("STATE_FLUSH_INTERVAL_SECONDS", "2.0"))
    transient_retry_attempts: int = int(os.getenv("TRANSIENT_RETRY_ATTEMPTS", "3"))
    transient_retry_base_seconds: float = float(os.getenv("TRANSIENT_RETRY_BASE_SECONDS", "0.4"))
    reconnect_attempts: int = int(os.getenv("RECONNECT_ATTEMPTS", "0"))
    reconnect_base_seconds: float = float(os.getenv("RECONNECT_BASE_SECONDS", "1.0"))
    reconnect_max_seconds: float = float(os.getenv("RECONNECT_MAX_SECONDS", "30.0"))
    reconnect_rest_every: int = int(os.getenv("RECONNECT_REST_EVERY", "10"))
    reconnect_rest_seconds: float = float(os.getenv("RECONNECT_REST_SECONDS", "20.0"))


def get_settings() -> Settings:
    settings = Settings()
    settings.uploads_dir.mkdir(parents=True, exist_ok=True)
    settings.temp_dir.mkdir(parents=True, exist_ok=True)
    settings.logs_dir.mkdir(parents=True, exist_ok=True)
    settings.state_dir.mkdir(parents=True, exist_ok=True)
    return settings
