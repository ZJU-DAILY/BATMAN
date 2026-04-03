from __future__ import annotations

import asyncio
from datetime import timedelta
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse

from app.config import settings
from app.engine.executor import IntermediateExecutor
from app.engine.exporter import exporter
from app.engine.planner import PipelinePlanner
from app.engine.utils import guess_preview_rows, infer_dtype, json_safe_value, new_id, now_utc, stem_name
from app.engine.validator import PipelineValidator
from app.models import (
    AcceptPayload,
    CandidatePipeline,
    CreateTargetTablePayload,
    FeedbackPayload,
    GenerationStatusResponse,
    PipelineOutlineItem,
    OutputResponse,
    RevisionRecord,
    RevisionStatus,
    ReviewSnapshot,
    Session,
    SessionStatus,
    SessionSummary,
    SuggestionPayload,
    SuggestionResponse,
    SettingsPayload,
    SourceUploadPayload,
    SourceTableSpec,
    TargetFieldSpec,
    TargetTableSpec,
    TargetTablePayload,
    TargetSamplesPayload,
    TargetSchemaPayload,
)
from app.presets import list_presets
from app.services.diagnosis_service import diagnosis_service
from app.services.explanation_service import explanation_service
from app.services.interactive_suggestion_service import interactive_suggestion_service
from app.services.revision_service import revision_service
from app.services.suggestion_service import suggestion_service
from app.storage import storage
from app.target_catalog import apply_target_table, build_empty_target_database, get_target_table


router = APIRouter(prefix="/api/sessions", tags=["sessions"])
planner = PipelinePlanner()
executor = IntermediateExecutor()
validator = PipelineValidator()
active_generation_tasks: dict[str, asyncio.Task] = {}


def _session_summary(session: Session) -> SessionSummary:
    return SessionSummary(
        session=session,
        available_presets=list_presets(),
        server_timeout_seconds=settings.timeout_seconds,
    )


def _score_candidate(candidate: CandidatePipeline) -> float:
    validation = candidate.validation_summary
    score = 0.0
    if validation.executable:
        score += 100
    if validation.column_match:
        score += 25
    if validation.required_fields_met:
        score += 15
    score += 20 * validation.type_compatibility
    if validation.example_similarity is not None:
        score += 20 * validation.example_similarity
    return round(score, 2)


def _is_retryable_generation_error(exc: Exception) -> bool:
    if isinstance(exc, HTTPException):
        return False
    message = str(exc)
    non_retryable_markers = (
        "Please upload at least one source table and define a target schema before generation.",
        "BAT generation is unavailable because the model service is not configured.",
        "Session not found",
    )
    return not any(marker in message for marker in non_retryable_markers)


def _ensure_session(session_id: str) -> Session:
    storage.cleanup_expired()
    try:
        session = storage.load(session_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Session not found") from exc
    if session.target_database is None:
        session.target_database = build_empty_target_database()
    if session.selected_target_table_id and not session.target_schema:
        table = get_target_table(session.target_database, session.selected_target_table_id)
        if table is not None:
            apply_target_table(session, session.selected_target_table_id)
    return session


def _session_dir(session_id: str) -> Path:
    return storage.session_dir(session_id)


def _update_session_ready_state(session: Session) -> Session:
    if session.source_tables and session.target_schema:
        session.status = SessionStatus.READY
        session.status_message = "Ready to generate."
    else:
        session.status = SessionStatus.DRAFT
        session.status_message = "Waiting for source tables."
    return storage.save(session)


def _reset_candidates(session: Session) -> None:
    session.candidates = []
    session.selected_candidate_id = None
    session.accepted_candidate_id = None
    session.revision_history = []


def _candidate_by_id(session: Session, candidate_id: str) -> CandidatePipeline:
    candidate = next((item for item in session.candidates if item.id == candidate_id), None)
    if not candidate:
        raise HTTPException(status_code=404, detail="Candidate not found")
    return candidate


def _revision_by_id(session: Session, revision_id: str) -> RevisionRecord:
    revision = next((item for item in session.revision_history if item.id == revision_id), None)
    if revision is None:
        raise HTTPException(status_code=404, detail="Revision record not found")
    return revision


def _build_pipeline_outline(candidate: CandidatePipeline) -> list[PipelineOutlineItem]:
    preview_by_step_id = {preview.step_id: preview for preview in candidate.step_previews}
    outline: list[PipelineOutlineItem] = []
    for step in candidate.pipeline_spec.steps:
        preview = preview_by_step_id.get(step.step_id)
        outline.append(
            PipelineOutlineItem(
                step_id=step.step_id,
                title=step.title,
                operator=step.operator,
                inputs=list(step.inputs),
                output_table=step.output,
                row_count=preview.row_count if preview else None,
                columns=list(preview.columns) if preview else [],
                added_columns=list(preview.added_columns) if preview else [],
                removed_columns=list(preview.removed_columns) if preview else [],
                renamed_columns=dict(preview.renamed_columns) if preview else {},
            )
        )
    return outline


def _build_review_snapshot(candidate: CandidatePipeline, node_id: str) -> ReviewSnapshot:
    selected_assessment = next((item for item in candidate.node_assessments if item.node_id == node_id), None)
    selected_step_preview = next((item for item in candidate.step_previews if item.step_id == node_id), None)
    selected_warning_items = [
        item
        for item in candidate.warning_items
        if node_id in item.node_ids
    ]
    return ReviewSnapshot(
        candidate_id=candidate.id,
        candidate_source=candidate.source,
        summary=candidate.summary,
        selected_node_id=node_id,
        selected_node_assessment=selected_assessment,
        selected_node_warning_items=selected_warning_items,
        selected_step_preview=selected_step_preview,
        pipeline_outline=_build_pipeline_outline(candidate),
        final_preview_rows=candidate.final_preview_rows[:3],
        validation_summary=candidate.validation_summary,
    )


def _normalize_target_table_payload(payload: CreateTargetTablePayload) -> tuple[str, str, list[TargetFieldSpec], list[dict[str, object]]]:
    table_name = payload.name.strip()
    if not table_name:
        raise HTTPException(status_code=400, detail="Target table name is required.")

    cleaned_schema = [
        TargetFieldSpec(
            name=field.name.strip(),
            type=field.type,
            description=field.description.strip(),
            required=field.required,
        )
        for field in payload.schema
        if field.name.strip()
    ]
    if not cleaned_schema:
        raise HTTPException(status_code=400, detail="Add at least one target schema field.")

    field_names = [field.name for field in cleaned_schema]
    if len(set(field_names)) != len(field_names):
        raise HTTPException(status_code=400, detail="Target schema field names must be unique.")

    normalized_rows: list[dict[str, object]] = []
    for row in payload.existing_rows:
        normalized_row = {field_name: row.get(field_name, "") for field_name in field_names}
        if any(str(value).strip() for value in normalized_row.values()):
            normalized_rows.append(normalized_row)

    return table_name, payload.description.strip(), cleaned_schema, normalized_rows


async def _build_candidate(
    session: Session,
    spec,
    source: str,
    *,
    parent_candidate_id: str | None = None,
    revision_record: RevisionRecord | None = None,
) -> CandidatePipeline:
    step_previews, final_preview_rows, final_df = executor.execute(session, spec)
    validation = validator.validate(final_df, session.target_schema, session.target_samples, warnings=list(spec.warnings))
    candidate = CandidatePipeline(
        id=new_id("candidate"),
        pipeline_spec=spec,
        step_previews=step_previews,
        final_preview_rows=final_preview_rows,
        validation_summary=validation,
        score=0.0,
        summary="",
        suggestions=[],
        created_at=now_utc(),
        source=source,
        parent_candidate_id=parent_candidate_id,
    )
    candidate = await diagnosis_service.enrich_candidate(session, candidate)
    candidate = await explanation_service.enrich_candidate(session, candidate)
    if revision_record is not None:
        provisional_after_snapshot = _build_review_snapshot(candidate, revision_record.node_id)
        candidate = await diagnosis_service.reconcile_revision(session, candidate, revision_record, provisional_after_snapshot)
        candidate = await explanation_service.enrich_candidate(session, candidate)
    candidate.suggestions = suggestion_service.build(session, candidate)
    candidate.score = _score_candidate(candidate)
    return candidate


async def _generate_for_session(session_id: str, revising: bool = False) -> None:
    session = _ensure_session(session_id)
    session.status = SessionStatus.REVISING if revising else SessionStatus.GENERATING
    session.status_message = "Analyzing inputs and generating candidate pipelines."
    session.last_error = ""
    storage.save(session)

    try:
        max_attempts = max(1, settings.generation_max_attempts)
        attempts_used = 0
        last_error: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            attempts_used = attempt
            try:
                specs = await planner.generate_pipeline_specs(session)
                candidate_source = "bat_search"
                if not specs:
                    raise ValueError("Please upload at least one source table and define a target schema before generation.")

                built_candidates = [
                    await _build_candidate(session, spec, candidate_source)
                    for spec in specs[:2]
                ]

                built_candidates.sort(key=lambda item: item.score, reverse=True)
                session.candidates = built_candidates[:2]
                session.selected_candidate_id = session.candidates[0].id if session.candidates else None
                session.accepted_candidate_id = None
                session.last_error = ""
                session.status = SessionStatus.REVIEW_READY
                session.status_message = (
                    "Pipeline ready for review."
                    if attempts_used == 1
                    else f"Pipeline ready for review after {attempts_used} attempts."
                )
                break
            except Exception as exc:
                last_error = exc
                if attempt >= max_attempts or not _is_retryable_generation_error(exc):
                    session.status = SessionStatus.ERROR
                    session.status_message = "Generation failed."
                    session.last_error = str(exc)
                    break

                session.status = SessionStatus.REVISING if revising else SessionStatus.GENERATING
                session.last_error = ""
                session.status_message = (
                    f"Retrying candidate generation in the background "
                    f"({attempt + 1}/{max_attempts}) after a recoverable generation failure."
                )
                storage.save(session)
                backoff_seconds = max(0.0, settings.generation_retry_backoff_seconds)
                if backoff_seconds > 0:
                    await asyncio.sleep(backoff_seconds * attempt)
        else:
            if last_error is not None:
                session.status = SessionStatus.ERROR
                session.status_message = "Generation failed."
                session.last_error = str(last_error)
    finally:
        storage.save(session)
        active_generation_tasks.pop(session_id, None)


async def _revise_for_session(session_id: str, revision_id: str, candidate_id: str, node_id: str, text: str) -> None:
    session = _ensure_session(session_id)
    revision_record = _revision_by_id(session, revision_id)
    try:
        base_candidate = _candidate_by_id(session, candidate_id)
        revised_spec = await revision_service.revise(session, base_candidate, node_id, text)
        revised_candidate = await _build_candidate(
            session,
            revised_spec,
            "llm_revision",
            parent_candidate_id=base_candidate.id,
            revision_record=revision_record,
        )
        session.candidates = [revised_candidate] + [candidate for candidate in session.candidates if candidate.id != candidate_id]
        session.selected_candidate_id = revised_candidate.id
        session.accepted_candidate_id = None
        revision_record.revised_candidate_id = revised_candidate.id
        revision_record.after_snapshot = _build_review_snapshot(revised_candidate, revision_record.node_id)
        revision_record.status = RevisionStatus.APPLIED
        revision_record.completed_at = now_utc()
        revision_record.error = None
        session.last_error = ""
        session.status = SessionStatus.REVIEW_READY
        session.status_message = "Review updated from your feedback."
    except Exception as exc:
        revision_record.status = RevisionStatus.FAILED
        revision_record.after_snapshot = None
        revision_record.completed_at = now_utc()
        revision_record.error = str(exc)
        session.status = SessionStatus.ERROR
        session.status_message = "Revision failed."
        session.last_error = str(exc)
    finally:
        storage.save(session)
        active_generation_tasks.pop(session_id, None)


@router.post("", response_model=SessionSummary)
async def create_session() -> SessionSummary:
    storage.cleanup_expired()
    now = now_utc()
    session = Session(
        id=new_id("session"),
        created_at=now,
        expires_at=now + timedelta(seconds=settings.session_ttl_seconds),
        target_database=build_empty_target_database(),
        status_message="Waiting for source tables.",
    )
    return _session_summary(storage.save(session))


@router.get("/{session_id}", response_model=SessionSummary)
async def get_session(session_id: str) -> SessionSummary:
    session = _ensure_session(session_id)
    return _session_summary(session)


@router.post("/{session_id}/sources", response_model=SessionSummary)
async def upload_sources(session_id: str, payload: SourceUploadPayload) -> SessionSummary:
    session = _ensure_session(session_id)
    source_dir = _session_dir(session_id) / "sources"
    source_dir.mkdir(parents=True, exist_ok=True)

    uploaded_tables: list[SourceTableSpec] = []
    for index, upload in enumerate(payload.files):
        suffix = Path(upload.filename or f"source_{index}.csv").suffix or ".csv"
        table_name = stem_name(upload.filename or f"source_{index}")
        path = source_dir / f"{table_name}{suffix}"
        path.write_text(upload.content, encoding="utf-8")
        df = pd.read_csv(path)
        uploaded_tables.append(
            SourceTableSpec(
                id=new_id("source"),
                name=table_name,
                filename=upload.filename or path.name,
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
            )
        )

    session.source_tables = uploaded_tables
    session.preset_id = None
    _reset_candidates(session)
    return _session_summary(_update_session_ready_state(session))


@router.put("/{session_id}/target-table", response_model=SessionSummary)
async def select_target_table(session_id: str, payload: TargetTablePayload) -> SessionSummary:
    session = _ensure_session(session_id)
    database = session.target_database or build_empty_target_database()
    table = get_target_table(database, payload.target_table_id)
    if table is None:
        raise HTTPException(status_code=404, detail="Target table not found")
    apply_target_table(session, payload.target_table_id)
    _reset_candidates(session)
    return _session_summary(_update_session_ready_state(session))


@router.post("/{session_id}/target-tables", response_model=SessionSummary)
async def create_target_table(session_id: str, payload: CreateTargetTablePayload) -> SessionSummary:
    session = _ensure_session(session_id)
    database = session.target_database or build_empty_target_database()
    table_name, description, cleaned_schema, normalized_rows = _normalize_target_table_payload(payload)

    database.tables.append(
        TargetTableSpec(
        id=new_id("target"),
        name=table_name,
        description=description,
        schema=cleaned_schema,
        existing_rows=normalized_rows,
        )
    )
    session.target_database = database
    apply_target_table(session, database.tables[-1].id)
    _reset_candidates(session)
    return _session_summary(_update_session_ready_state(session))


@router.put("/{session_id}/target-tables/{target_table_id}", response_model=SessionSummary)
async def update_target_table(session_id: str, target_table_id: str, payload: CreateTargetTablePayload) -> SessionSummary:
    session = _ensure_session(session_id)
    database = session.target_database or build_empty_target_database()
    table = get_target_table(database, target_table_id)
    if table is None:
        raise HTTPException(status_code=404, detail="Target table not found")

    table_name, description, cleaned_schema, normalized_rows = _normalize_target_table_payload(payload)
    table.name = table_name
    table.description = description
    table.schema = cleaned_schema
    table.existing_rows = normalized_rows
    session.target_database = database
    if session.selected_target_table_id == target_table_id:
        apply_target_table(session, target_table_id)
    _reset_candidates(session)
    return _session_summary(_update_session_ready_state(session))


@router.delete("/{session_id}/target-tables/{target_table_id}", response_model=SessionSummary)
async def delete_target_table(session_id: str, target_table_id: str) -> SessionSummary:
    session = _ensure_session(session_id)
    database = session.target_database or build_empty_target_database()
    table_index = next((index for index, table in enumerate(database.tables) if table.id == target_table_id), None)
    if table_index is None:
        raise HTTPException(status_code=404, detail="Target table not found")

    database.tables.pop(table_index)
    session.target_database = database

    if session.selected_target_table_id == target_table_id:
        if database.tables:
            apply_target_table(session, database.tables[0].id)
        else:
            session.selected_target_table_id = None
            session.target_schema = []
            session.target_samples = []

    _reset_candidates(session)

    if session.source_tables and session.target_schema:
        return _session_summary(_update_session_ready_state(session))

    session.status = SessionStatus.DRAFT
    session.status_message = "Select a target table." if session.source_tables else "Waiting for source tables."
    return _session_summary(storage.save(session))


@router.put("/{session_id}/target-schema", response_model=SessionSummary)
async def save_target_schema(session_id: str, payload: TargetSchemaPayload) -> SessionSummary:
    session = _ensure_session(session_id)
    session.target_schema = payload.fields
    _reset_candidates(session)
    return _session_summary(_update_session_ready_state(session))


@router.put("/{session_id}/target-samples", response_model=SessionSummary)
async def save_target_samples(session_id: str, payload: TargetSamplesPayload) -> SessionSummary:
    session = _ensure_session(session_id)
    session.target_samples = payload.rows
    _reset_candidates(session)
    return _session_summary(_update_session_ready_state(session))


@router.put("/{session_id}/settings", response_model=SessionSummary)
async def save_settings(session_id: str, payload: SettingsPayload) -> SessionSummary:
    session = _ensure_session(session_id)
    session.settings = payload.settings
    for table in session.source_tables:
        df = pd.read_csv(table.path)
        table.preview_rows = [
            json_safe_value(row)
            for row in df.head(session.settings.preview_rows)
            .where(pd.notnull(df.head(session.settings.preview_rows)), None)
            .to_dict(orient="records")
        ]
    return _session_summary(_update_session_ready_state(session))


@router.post("/{session_id}/generate", response_model=GenerationStatusResponse)
async def generate(session_id: str) -> GenerationStatusResponse:
    session = _ensure_session(session_id)
    if not session.source_tables or not session.target_schema:
        raise HTTPException(status_code=400, detail="Please upload source tables and define the target schema first.")
    if session_id in active_generation_tasks:
        return GenerationStatusResponse(status=session.status, message=session.status_message, selected_candidate_id=session.selected_candidate_id)
    session.status = SessionStatus.GENERATING
    session.status_message = "Analyzing inputs and generating candidate pipelines."
    storage.save(session)
    task = asyncio.create_task(_generate_for_session(session_id))
    active_generation_tasks[session_id] = task
    return GenerationStatusResponse(status=SessionStatus.GENERATING, message="Generation started.")


@router.get("/{session_id}/generation-status", response_model=GenerationStatusResponse)
async def generation_status(session_id: str) -> GenerationStatusResponse:
    session = _ensure_session(session_id)
    return GenerationStatusResponse(
        status=session.status,
        message=session.last_error or session.status_message,
        selected_candidate_id=session.selected_candidate_id,
        accepted_candidate_id=session.accepted_candidate_id,
    )


@router.get("/{session_id}/candidates", response_model=list[CandidatePipeline])
async def list_candidates(session_id: str) -> list[CandidatePipeline]:
    session = _ensure_session(session_id)
    return session.candidates


@router.get("/{session_id}/candidates/{candidate_id}", response_model=CandidatePipeline)
async def get_candidate(session_id: str, candidate_id: str) -> CandidatePipeline:
    session = _ensure_session(session_id)
    return _candidate_by_id(session, candidate_id)


@router.post("/{session_id}/feedback", response_model=SessionSummary)
async def apply_feedback(session_id: str, payload: FeedbackPayload) -> SessionSummary:
    session = _ensure_session(session_id)
    candidate = _candidate_by_id(session, payload.candidate_id)
    if session_id in active_generation_tasks:
        return _session_summary(session)
    revision_record = RevisionRecord(
        id=new_id("revision"),
        text=payload.text,
        node_id=payload.node_id,
        base_candidate_id=payload.candidate_id,
        status=RevisionStatus.PENDING,
        before_snapshot=_build_review_snapshot(candidate, payload.node_id),
        created_at=now_utc(),
    )
    session.revision_history = session.revision_history + [revision_record]
    session.accepted_candidate_id = None
    session.status = SessionStatus.REVISING
    session.status_message = "Revising pipeline from your feedback."
    session.last_error = ""
    storage.save(session)
    task = asyncio.create_task(_revise_for_session(session_id, revision_record.id, payload.candidate_id, payload.node_id, payload.text))
    active_generation_tasks[session_id] = task
    return _session_summary(session)


@router.post("/{session_id}/live-suggestions", response_model=SuggestionResponse)
async def live_suggestions(session_id: str, payload: SuggestionPayload) -> SuggestionResponse:
    session = _ensure_session(session_id)
    candidate = _candidate_by_id(session, payload.candidate_id)
    suggestions = await interactive_suggestion_service.generate(session, candidate, payload.node_id, payload.text)
    return SuggestionResponse(suggestions=suggestions)


@router.post("/{session_id}/accept", response_model=SessionSummary)
async def accept_candidate(session_id: str, payload: AcceptPayload) -> SessionSummary:
    session = _ensure_session(session_id)
    candidate = _candidate_by_id(session, payload.candidate_id)
    refreshed_candidate = await explanation_service.enrich_candidate(session, candidate)
    for index, item in enumerate(session.candidates):
        if item.id == payload.candidate_id:
            session.candidates[index] = refreshed_candidate
            break
    session.accepted_candidate_id = payload.candidate_id
    session.selected_candidate_id = payload.candidate_id
    session.status = SessionStatus.EXPORT_READY if refreshed_candidate.validation_summary.pipeline_correct else SessionStatus.ACCEPTED
    session.status_message = (
        "Pipeline accepted and ready to export."
        if refreshed_candidate.validation_summary.pipeline_correct
        else "Pipeline accepted, but the output still has warnings."
    )
    storage.save(session)
    return _session_summary(session)


@router.get("/{session_id}/output", response_model=OutputResponse)
async def get_output(session_id: str) -> OutputResponse:
    session = _ensure_session(session_id)
    if not session.accepted_candidate_id:
        raise HTTPException(status_code=400, detail="Accept a pipeline before opening the output page.")
    candidate = _candidate_by_id(session, session.accepted_candidate_id)
    before_df, after_df = exporter.build_target_table_frames(session, candidate)
    export_ready = candidate.validation_summary.pipeline_correct
    return OutputResponse(
        candidate=candidate,
        final_preview_rows=candidate.final_preview_rows,
        schema_check=candidate.validation_summary,
        target_table_before_preview_rows=guess_preview_rows(before_df, session.settings.preview_rows),
        target_table_after_preview_rows=guess_preview_rows(after_df, session.settings.preview_rows),
        existing_row_count=int(len(before_df.index)),
        generated_row_count=int(max(len(after_df.index) - len(before_df.index), 0)),
        export_ready=export_ready,
    )


@router.get("/{session_id}/export")
async def export_output(session_id: str, format: str = Query(default="all")):
    session = _ensure_session(session_id)
    if not session.accepted_candidate_id:
        raise HTTPException(status_code=400, detail="Accept a pipeline before exporting.")
    candidate = _candidate_by_id(session, session.accepted_candidate_id)
    files = exporter.export(session, candidate, _session_dir(session_id) / "exports" / candidate.id)
    if format not in files:
        raise HTTPException(status_code=400, detail="Unsupported export format.")
    file_path = files[format]
    media_type = "application/zip" if format == "all" else "application/octet-stream"
    return FileResponse(path=file_path, filename=file_path.name, media_type=media_type)
