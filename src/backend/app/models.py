from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, computed_field


class DataType(str, Enum):
    STRING = "STRING"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    DATE = "DATE"
    DATETIME = "DATETIME"
    BOOLEAN = "BOOLEAN"


class ExplanationDetail(str, Enum):
    BRIEF = "Brief"
    STANDARD = "Standard"
    DETAILED = "Detailed"


class SessionStatus(str, Enum):
    DRAFT = "draft"
    READY = "ready_for_generation"
    GENERATING = "generating"
    REVIEW_READY = "review_ready"
    REVISING = "revising"
    ACCEPTED = "accepted"
    EXPORT_READY = "export_ready"
    ERROR = "error"


class OperatorType(str, Enum):
    SOURCE_TABLE = "source_table"
    RENAME = "rename"
    JOIN = "join"
    UNION = "union"
    GROUPBY = "groupby"
    PIVOT = "pivot"
    UNPIVOT = "unpivot"
    DATE_FORMATTING = "date_formatting"
    COLUMN_ARITHMETIC = "column_arithmetic"
    ADD_COLUMNS = "add_columns"
    DROP_COLUMNS = "drop_columns"


class NodeStatus(str, Enum):
    OK = "ok"
    ISSUE = "issue"


class SourceTableSpec(BaseModel):
    id: str
    name: str
    filename: str
    path: str
    description: str = ""
    columns: list[str]
    inferred_types: dict[str, DataType]
    rows: list[dict[str, Any]] = Field(default_factory=list)
    preview_rows: list[dict[str, Any]]
    row_count: int


class TargetTableSpec(BaseModel):
    id: str
    name: str
    description: str = ""
    schema: list["TargetFieldSpec"] = Field(default_factory=list)
    existing_rows: list[dict[str, Any]] = Field(default_factory=list)


class TargetDatabaseSpec(BaseModel):
    id: str
    name: str
    description: str = ""
    tables: list[TargetTableSpec] = Field(default_factory=list)


class TargetFieldSpec(BaseModel):
    name: str
    type: DataType = DataType.STRING
    description: str = ""
    required: bool = True


class GenerationSettings(BaseModel):
    explanation_detail: ExplanationDetail = ExplanationDetail.STANDARD
    bat_max_rollout_steps: int = Field(default=10, ge=1, le=30)
    bat_max_depth: int = Field(default=5, ge=1, le=10)
    bat_exploration_constant: float = Field(default=1.0, ge=0.1, le=3.0)
    bat_temperature: float = Field(default=0.1, ge=0.0, le=1.0)
    bat_top_p: float = Field(default=0.8, gt=0.0, le=1.0)
    preview_rows: int = 5


class PipelineStep(BaseModel):
    step_id: str
    operator: OperatorType
    title: str
    inputs: list[str]
    output: str
    params: dict[str, Any] = Field(default_factory=dict)
    notes: str = ""


class PipelineSpec(BaseModel):
    steps: list[PipelineStep]
    final_table: str
    warnings: list[str] = Field(default_factory=list)
    rationale: str = ""
    raw_code_lines: list[str] = Field(default_factory=list)


class StepPreview(BaseModel):
    step_id: str
    title: str
    operator: OperatorType
    output_table: str
    row_count: int
    columns: list[str]
    preview_rows: list[dict[str, Any]]
    added_columns: list[str] = Field(default_factory=list)
    removed_columns: list[str] = Field(default_factory=list)
    renamed_columns: dict[str, str] = Field(default_factory=dict)
    notes: str = ""
    warnings: list[str] = Field(default_factory=list)


class ValidationFieldCheck(BaseModel):
    field_name: str
    expected_type: DataType
    actual_type: DataType | None = None
    status: str
    required: bool = True


class ValidationSummary(BaseModel):
    executable: bool
    column_match: bool
    required_fields_met: bool
    type_compatibility: float
    sample_consistency: float | None = None
    example_similarity: float | None = None
    pipeline_correct: bool = False
    warnings: list[str] = Field(default_factory=list)
    row_count: int | None = None
    column_count: int | None = None
    field_checks: list[ValidationFieldCheck] = Field(default_factory=list)


class NodeAssessment(BaseModel):
    node_id: str
    status: NodeStatus = NodeStatus.OK
    reason: str = ""


class WarningItem(BaseModel):
    id: str
    title: str
    detail: str = ""
    node_ids: list[str] = Field(default_factory=list)
    source: str = "heuristic"


class CandidatePipeline(BaseModel):
    id: str
    pipeline_spec: PipelineSpec
    step_previews: list[StepPreview]
    final_preview_rows: list[dict[str, Any]] = Field(default_factory=list)
    validation_summary: ValidationSummary
    score: float
    summary: str
    suggestions: list[str] = Field(default_factory=list)
    warning_items: list[WarningItem] = Field(default_factory=list)
    node_explanations: dict[str, str] = Field(default_factory=dict)
    node_assessments: list[NodeAssessment] = Field(default_factory=list)
    created_at: datetime
    source: str = "heuristic"


class FeedbackItem(BaseModel):
    id: str
    text: str
    candidate_id: str
    created_at: datetime


class Session(BaseModel):
    id: str
    created_at: datetime
    expires_at: datetime
    status: SessionStatus = SessionStatus.DRAFT
    status_message: str = ""
    mode: str = "metadata_only"
    preset_id: str | None = None
    settings: GenerationSettings = Field(default_factory=GenerationSettings)
    source_tables: list[SourceTableSpec] = Field(default_factory=list)
    target_database: TargetDatabaseSpec | None = None
    selected_target_table_id: str | None = None
    target_schema: list[TargetFieldSpec] = Field(default_factory=list)
    target_samples: list[dict[str, Any]] = Field(default_factory=list)
    candidates: list[CandidatePipeline] = Field(default_factory=list)
    selected_candidate_id: str | None = None
    accepted_candidate_id: str | None = None
    feedback_history: list[FeedbackItem] = Field(default_factory=list)
    last_error: str = ""

    @computed_field  # type: ignore[misc]
    @property
    def has_samples(self) -> bool:
        return bool(self.target_samples)


class SessionSummary(BaseModel):
    session: Session
    available_presets: list[dict[str, str]]
    server_timeout_seconds: int


class TargetSchemaPayload(BaseModel):
    fields: list[TargetFieldSpec]


class SourceUploadItem(BaseModel):
    filename: str
    content: str


class SourceUploadPayload(BaseModel):
    files: list[SourceUploadItem]


class TargetSamplesPayload(BaseModel):
    rows: list[dict[str, Any]]


class TargetTablePayload(BaseModel):
    target_table_id: str


class CreateTargetTablePayload(BaseModel):
    name: str
    description: str = ""
    schema: list[TargetFieldSpec] = Field(default_factory=list)
    existing_rows: list[dict[str, Any]] = Field(default_factory=list)


class SettingsPayload(BaseModel):
    settings: GenerationSettings


class FeedbackPayload(BaseModel):
    candidate_id: str
    node_id: str
    text: str


class SuggestionPayload(BaseModel):
    candidate_id: str
    node_id: str
    text: str


class AcceptPayload(BaseModel):
    candidate_id: str


class GenerationStatusResponse(BaseModel):
    status: SessionStatus
    message: str = ""
    selected_candidate_id: str | None = None
    accepted_candidate_id: str | None = None


class OutputResponse(BaseModel):
    candidate: CandidatePipeline
    final_preview_rows: list[dict[str, Any]]
    schema_check: ValidationSummary
    target_table_before_preview_rows: list[dict[str, Any]] = Field(default_factory=list)
    target_table_after_preview_rows: list[dict[str, Any]] = Field(default_factory=list)
    existing_row_count: int = 0
    generated_row_count: int = 0
    export_ready: bool = False


class SuggestionResponse(BaseModel):
    suggestions: list[str] = Field(default_factory=list)
