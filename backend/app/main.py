from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.api.imports import router as imports_router
from app.config import get_settings
from app.services.job_state import get_log_tail, load_state

settings = get_settings()
app = FastAPI(title=settings.app_name)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(imports_router)

frontend_dir = Path("frontend").resolve()
if frontend_dir.exists():
    app.mount("/static", StaticFiles(directory=frontend_dir), name="static")


@app.get("/")
def index() -> FileResponse:
    return FileResponse(frontend_dir / "index.html")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.websocket("/ws/imports/{job_id}")
async def import_updates(websocket: WebSocket, job_id: str) -> None:
    await websocket.accept()
    try:
        while True:
            state = load_state(job_id)
            await websocket.send_json({"state": state.__dict__, "logs": get_log_tail(job_id, 200)})
            if state.status in {"completed", "failed", "cancelled"}:
                await asyncio.sleep(0.2)
                break
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        return
    finally:
        await websocket.close()
