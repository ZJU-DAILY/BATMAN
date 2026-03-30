export type DataType = "STRING" | "INTEGER" | "FLOAT" | "DATE" | "DATETIME" | "BOOLEAN";
export type ExplanationDetail = "Brief" | "Standard" | "Detailed";
export type SessionStatus =
  | "draft"
  | "ready_for_generation"
  | "generating"
  | "review_ready"
  | "revising"
  | "accepted"
  | "export_ready"
  | "error";

export interface SourceTableSpec {
  id: string;
  name: string;
  filename: string;
  path: string;
  description: string;
  columns: string[];
  inferred_types: Record<string, DataType>;
  rows: Record<string, unknown>[];
  preview_rows: Record<string, unknown>[];
  row_count: number;
}

export interface TargetTableSpec {
  id: string;
  name: string;
  description: string;
  schema: TargetFieldSpec[];
  existing_rows: Record<string, unknown>[];
}

export interface TargetDatabaseSpec {
  id: string;
  name: string;
  description: string;
  tables: TargetTableSpec[];
}

export interface TargetFieldSpec {
  name: string;
  type: DataType;
  description: string;
  required: boolean;
}

export interface GenerationSettings {
  explanation_detail: ExplanationDetail;
  bat_max_rollout_steps: number;
  bat_max_depth: number;
  bat_exploration_constant: number;
  bat_temperature: number;
  bat_top_p: number;
  preview_rows: number;
}

export interface PipelineStep {
  step_id: string;
  operator: string;
  title: string;
  inputs: string[];
  output: string;
  params: Record<string, unknown>;
  notes: string;
}

export interface StepPreview {
  step_id: string;
  title: string;
  operator: string;
  output_table: string;
  row_count: number;
  columns: string[];
  preview_rows: Record<string, unknown>[];
  added_columns: string[];
  removed_columns: string[];
  renamed_columns: Record<string, string>;
  notes: string;
  warnings: string[];
}

export interface ValidationFieldCheck {
  field_name: string;
  expected_type: DataType;
  actual_type?: DataType | null;
  status: string;
  required: boolean;
}

export interface ValidationSummary {
  executable: boolean;
  column_match: boolean;
  required_fields_met: boolean;
  type_compatibility: number;
  sample_consistency?: number | null;
  example_similarity?: number | null;
  pipeline_correct: boolean;
  warnings: string[];
  row_count?: number | null;
  column_count?: number | null;
  field_checks: ValidationFieldCheck[];
}

export interface NodeAssessment {
  node_id: string;
  status: "ok" | "issue";
  reason: string;
}

export interface WarningItem {
  id: string;
  title: string;
  detail: string;
  node_ids: string[];
  source: string;
}

export interface PipelineSpec {
  steps: PipelineStep[];
  final_table: string;
  warnings: string[];
  rationale: string;
}

export interface CandidatePipeline {
  id: string;
  pipeline_spec: PipelineSpec;
  step_previews: StepPreview[];
  final_preview_rows: Record<string, unknown>[];
  validation_summary: ValidationSummary;
  score: number;
  summary: string;
  suggestions: string[];
  warning_items: WarningItem[];
  node_explanations: Record<string, string>;
  node_assessments: NodeAssessment[];
  created_at: string;
  source: string;
}

export interface FeedbackItem {
  id: string;
  text: string;
  candidate_id: string;
  created_at: string;
}

export interface Session {
  id: string;
  created_at: string;
  expires_at: string;
  status: SessionStatus;
  status_message: string;
  mode: string;
  preset_id?: string | null;
  settings: GenerationSettings;
  source_tables: SourceTableSpec[];
  target_database?: TargetDatabaseSpec | null;
  selected_target_table_id?: string | null;
  target_schema: TargetFieldSpec[];
  target_samples: Record<string, unknown>[];
  candidates: CandidatePipeline[];
  selected_candidate_id?: string | null;
  accepted_candidate_id?: string | null;
  feedback_history: FeedbackItem[];
  last_error: string;
  has_samples: boolean;
}

export interface PresetSummary {
  id: string;
  name: string;
  description: string;
}

export interface SessionSummary {
  session: Session;
  available_presets: PresetSummary[];
  server_timeout_seconds: number;
}

export interface OutputResponse {
  candidate: CandidatePipeline;
  final_preview_rows: Record<string, unknown>[];
  schema_check: ValidationSummary;
  target_table_before_preview_rows: Record<string, unknown>[];
  target_table_after_preview_rows: Record<string, unknown>[];
  existing_row_count: number;
  generated_row_count: number;
  export_ready: boolean;
}

export interface GenerationStatusResponse {
  status: SessionStatus;
  message: string;
  selected_candidate_id?: string | null;
  accepted_candidate_id?: string | null;
}

export interface SuggestionResponse {
  suggestions: string[];
}
