from __future__ import annotations

from datetime import timedelta

import pandas as pd
from fastapi import APIRouter, HTTPException

from app.config import settings
from app.engine.utils import infer_dtype, json_safe_value, new_id, now_utc, stem_name
from app.models import SessionStatus
from app.models import Session, SessionSummary, SourceTableSpec
from app.presets import PRESETS, list_presets
from app.storage import storage
from app.target_catalog import apply_target_table, build_target_database, get_target_table


router = APIRouter(prefix="/api/presets", tags=["presets"])


@router.get("")
async def get_presets() -> list[dict[str, str]]:
    return list_presets()


@router.post("/{preset_id}/load", response_model=SessionSummary)
async def load_preset(preset_id: str) -> SessionSummary:
    if preset_id not in PRESETS:
        raise HTTPException(status_code=404, detail="Preset not found")
    preset = PRESETS[preset_id]
    now = now_utc()
    target_database = build_target_database()
    selected_table = get_target_table(target_database, preset.target_table_id)
    if selected_table is None:
        raise HTTPException(status_code=500, detail="Preset target table is missing.")
    target_database.tables = [selected_table]
    if preset.clear_all_target_rows:
        selected_table.existing_rows = []
    selected_table.schema = [field.model_copy(deep=True) for field in preset.target_schema]
    selected_table.existing_rows = [dict(row) for row in preset.target_samples]
    if preset.target_table_description is not None:
        selected_table.description = preset.target_table_description
    session = Session(
        id=new_id("session"),
        created_at=now,
        expires_at=now + timedelta(seconds=settings.session_ttl_seconds),
        preset_id=preset_id,
        target_database=target_database,
        selected_target_table_id=preset.target_table_id,
        status_message="Preset loaded. Ready to generate.",
    )
    apply_target_table(session, preset.target_table_id)

    session_dir = storage.session_dir(session.id)
    source_dir = session_dir / "sources"
    source_dir.mkdir(parents=True, exist_ok=True)

    source_tables: list[SourceTableSpec] = []
    for filename, content in preset.source_files.items():
        path = source_dir / filename
        path.write_text(content, encoding="utf-8")
        df = pd.read_csv(path)
        source_tables.append(
            SourceTableSpec(
                id=new_id("source"),
                name=stem_name(filename),
                filename=filename,
                path=str(path),
                columns=df.columns.tolist(),
                inferred_types={column: infer_dtype(df[column]) for column in df.columns},
                rows=[
                    json_safe_value(row)
                    for row in df.where(pd.notnull(df), None).to_dict(orient="records")
                ],
                preview_rows=[
                    json_safe_value(row)
                    for row in df.head(session.settings.preview_rows)
                    .where(pd.notnull(df.head(session.settings.preview_rows)), None)
                    .to_dict(orient="records")
                ],
                row_count=int(len(df.index)),
                description="",
            )
        )
    session.source_tables = source_tables
    session.mode = "sample_assisted" if session.target_samples else "metadata_only"
    session.status = SessionStatus.READY
    storage.save(session)
    return SessionSummary(
        session=session,
        available_presets=list_presets(),
        server_timeout_seconds=settings.timeout_seconds,
    )
