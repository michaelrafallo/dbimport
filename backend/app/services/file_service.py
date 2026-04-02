from __future__ import annotations

import os
import shutil
import uuid
import zipfile
from pathlib import Path
from typing import BinaryIO

from fastapi import HTTPException, UploadFile

from app.config import get_settings

ALLOWED_EXTENSIONS = {".sql", ".zip"}


def _safe_name(name: str) -> str:
    # Keep only basename and prefix with random token to avoid collisions.
    return f"{uuid.uuid4().hex}_{Path(name).name}"


def validate_filename(filename: str) -> None:
    ext = Path(filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Only .sql and .zip files are allowed.")


def save_upload_stream(upload: UploadFile) -> Path:
    settings = get_settings()
    validate_filename(upload.filename or "")

    safe_name = _safe_name(upload.filename or "upload.sql")
    dest = settings.uploads_dir / safe_name
    bytes_written = 0
    chunk_size = 1024 * 1024

    with dest.open("wb") as out:
        while True:
            chunk = upload.file.read(chunk_size)
            if not chunk:
                break
            bytes_written += len(chunk)
            if bytes_written > settings.max_upload_size_bytes:
                out.close()
                dest.unlink(missing_ok=True)
                raise HTTPException(status_code=413, detail="Uploaded file exceeds allowed size.")
            out.write(chunk)

    if bytes_written == 0:
        dest.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")
    return dest


def _is_within(directory: Path, candidate: Path) -> bool:
    try:
        candidate.resolve().relative_to(directory.resolve())
        return True
    except ValueError:
        return False


def _copy_stream(src: BinaryIO, dst: BinaryIO, max_size: int) -> int:
    copied = 0
    chunk_size = 1024 * 1024
    while True:
        chunk = src.read(chunk_size)
        if not chunk:
            break
        copied += len(chunk)
        if copied > max_size:
            raise HTTPException(status_code=400, detail="Extracted SQL file exceeds allowed size.")
        dst.write(chunk)
    return copied


def resolve_sql_source(file_path: Path) -> Path:
    if file_path.suffix.lower() == ".sql":
        return file_path
    if file_path.suffix.lower() != ".zip":
        raise HTTPException(status_code=400, detail="Unsupported uploaded file type.")
    return extract_sql_from_zip(file_path)


def extract_sql_from_zip(zip_path: Path) -> Path:
    settings = get_settings()
    sql_output_dir = settings.temp_dir / zip_path.stem
    sql_output_dir.mkdir(parents=True, exist_ok=True)

    sql_candidates: list[zipfile.ZipInfo] = []
    total_uncompressed = 0
    with zipfile.ZipFile(zip_path) as zf:
        infos = zf.infolist()
        if len(infos) > settings.max_zip_members:
            raise HTTPException(status_code=400, detail="Zip has too many entries.")
        for info in infos:
            if info.is_dir():
                continue
            if Path(info.filename).suffix.lower() == ".sql":
                sql_candidates.append(info)
                total_uncompressed += info.file_size

        if not sql_candidates:
            raise HTTPException(status_code=400, detail="Zip does not contain a .sql file.")
        if total_uncompressed > settings.max_extracted_zip_size_bytes:
            raise HTTPException(status_code=400, detail="Uncompressed SQL exceeds configured limit.")

        selected = max(sql_candidates, key=lambda i: i.file_size)
        dest = sql_output_dir / _safe_name(selected.filename)
        if not _is_within(sql_output_dir, dest):
            raise HTTPException(status_code=400, detail="Unsafe zip entry path.")

        with zf.open(selected, "r") as src, dest.open("wb") as dst:
            _copy_stream(src, dst, settings.max_extracted_zip_size_bytes)
    return dest


def cleanup_temp_path(path: Path | None) -> None:
    if not path:
        return
    try:
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        elif path.exists():
            path.unlink(missing_ok=True)
    except OSError:
        # Cleanup should not break user flow.
        pass
