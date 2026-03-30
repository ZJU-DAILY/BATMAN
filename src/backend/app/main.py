from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers.presets import router as presets_router
from app.routers.sessions import router as sessions_router


app = FastAPI(title="ADP Demo Backend", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(sessions_router)
app.include_router(presets_router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
