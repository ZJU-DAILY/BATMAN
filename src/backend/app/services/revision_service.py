from __future__ import annotations

import json
from typing import Any

from app.config import settings
from app.engine.compiler import PipelineCompiler
from app.engine.normalizer import CodeToPipelineNormalizer
from app.engine.structure_validator import OPERATOR_SIGNATURES, StepDependencyTrace, structure_validator
from app.models import CandidatePipeline, OperatorType, PipelineSpec, PipelineStep, Session
from app.services.llm_client import llm_client


REVISION_SYSTEM_PROMPT = (
    "You revise pandas transformation code for an interactive data-preparation web app. "
    "Return JSON only. Preserve the locked code prefix exactly. Rewrite only the selected line and later lines. "
    "Use the real source dataframe names, keep source dataframes read-only, write every table-level transformation to a fresh dataframe variable, "
    "mutate columns only after an explicit .copy() scaffold, use named aggregation tuples for groupby, "
    "keep downstream dependencies consistent after any column-name changes, assign final_output exactly once as the final line, "
    "and keep the revised pipeline executable, normalizable, and valid for downstream review."
)

REVISION_REPAIR_SYSTEM_PROMPT = (
    "You repair invalid pandas transformation revisions for an interactive data-preparation web app. "
    "Return JSON only. Preserve the locked code prefix exactly. Repair only the selected line and later lines. "
    "Use the provided validation failure and downstream dependency summary to restore a coherent executable pipeline. "
    "Use the real source dataframe names, keep source dataframes read-only, write every table-level transformation to a fresh dataframe variable, "
    "mutate columns only after an explicit .copy() scaffold, use named aggregation tuples for groupby, "
    "keep downstream dependencies consistent after any column-name changes, assign final_output exactly once as the final line, "
    "and keep the repaired pipeline executable, normalizable, and valid for downstream review."
)


class PipelineRevisionService:
    def __init__(self) -> None:
        self.compiler = PipelineCompiler()
        self.normalizer = CodeToPipelineNormalizer()

    async def revise(self, session: Session, candidate: CandidatePipeline, node_id: str, text: str) -> PipelineSpec:
        if not llm_client.enabled:
            raise ValueError("LLM revision is unavailable because the model service is not configured.")
        if not text.strip():
            raise ValueError("Revision text is empty.")

        start_index = self._mutable_start_index(candidate, node_id)
        original_code_lines, step_line_map = self.compiler.compile_transformation_body(candidate.pipeline_spec)
        mutable_line_index = self._mutable_line_index(candidate, node_id, start_index, step_line_map)
        current_traces = structure_validator.trace_spec(session, candidate.pipeline_spec, origin="Current pipeline")

        prompt = self._prompt(
            session,
            candidate,
            node_id,
            start_index,
            mutable_line_index,
            text.strip(),
            original_code_lines,
            current_traces,
        )
        first_response = await self._complete_code_request(prompt, REVISION_SYSTEM_PROMPT)
        first_code_lines = self._parse_code_payload(first_response)

        try:
            return self._validate_revision_attempt(
                session=session,
                candidate=candidate,
                mutable_start_index=start_index,
                mutable_line_index=mutable_line_index,
                original_code_lines=original_code_lines,
                revised_code_lines=first_code_lines,
                origin_label="Revision",
            )
        except ValueError as exc:
            first_error = str(exc)

        repair_prompt = self._repair_prompt(
            session=session,
            candidate=candidate,
            node_id=node_id,
            mutable_start_index=start_index,
            mutable_line_index=mutable_line_index,
            text=text.strip(),
            original_code_lines=original_code_lines,
            invalid_code_lines=first_code_lines,
            failure_reason=first_error,
            current_traces=current_traces,
        )
        repair_response = await self._complete_code_request(repair_prompt, REVISION_REPAIR_SYSTEM_PROMPT)
        repair_code_lines = self._parse_code_payload(repair_response)

        return self._validate_revision_attempt(
            session=session,
            candidate=candidate,
            mutable_start_index=start_index,
            mutable_line_index=mutable_line_index,
            original_code_lines=original_code_lines,
            revised_code_lines=repair_code_lines,
            origin_label="Revision repair",
        )

    def validate_revised_spec(
        self,
        session: Session,
        candidate: CandidatePipeline,
        revised: PipelineSpec,
        mutable_start_index: int,
    ) -> list[StepDependencyTrace]:
        base_steps = candidate.pipeline_spec.steps
        revised_steps = revised.steps
        if not revised_steps:
            raise ValueError("The revised pipeline must contain at least one step.")
        if len(revised_steps) < mutable_start_index:
            raise ValueError("The revised pipeline removed locked steps before the selected node.")

        locked_prefix = [self._comparable_step_payload(step) for step in base_steps[:mutable_start_index]]
        revised_prefix = [self._comparable_step_payload(step) for step in revised_steps[:mutable_start_index]]
        if locked_prefix != revised_prefix:
            raise ValueError("The revised pipeline changed a locked step before the selected node.")

        self._validate_selected_step_semantics(candidate, revised, mutable_start_index)

        try:
            return structure_validator.trace_spec(session, revised, origin="Revised pipeline")
        except ValueError as exc:
            raise self._rewrite_validation_error(candidate, revised, mutable_start_index, exc) from exc

    def _mutable_start_index(self, candidate: CandidatePipeline, node_id: str) -> int:
        steps = candidate.pipeline_spec.steps
        if not steps:
            raise ValueError("The selected candidate has no executable steps.")
        if node_id == "node_input":
            return 0
        if node_id == "node_output":
            return len(steps) - 1
        for index, step in enumerate(steps):
            if step.step_id == node_id:
                return index
        raise ValueError("The selected node could not be found in the current pipeline.")

    def _mutable_line_index(
        self,
        candidate: CandidatePipeline,
        node_id: str,
        mutable_start_index: int,
        step_line_map: dict[str, int],
    ) -> int:
        steps = candidate.pipeline_spec.steps
        if not steps:
            return 1
        if node_id == "node_input":
            return 1
        selected_step = steps[mutable_start_index]
        return step_line_map.get(selected_step.step_id, 1)

    def _prompt(
        self,
        session: Session,
        candidate: CandidatePipeline,
        node_id: str,
        mutable_start_index: int,
        mutable_line_index: int,
        text: str,
        original_code_lines: list[str],
        current_traces: list[StepDependencyTrace],
    ) -> str:
        steps = candidate.pipeline_spec.steps
        step_previews = {preview.step_id: preview for preview in candidate.step_previews}
        selected_step = steps[mutable_start_index] if steps else None
        selected_preview = step_previews.get(selected_step.step_id) if selected_step else None
        selected_warning_items = [
            item.model_dump(mode="json")
            for item in candidate.warning_items
            if selected_step and selected_step.step_id in item.node_ids
        ]
        selected_assessment = next((item for item in candidate.node_assessments if selected_step and item.node_id == selected_step.step_id), None)
        current_code = [f"{index}. {line}" for index, line in enumerate(original_code_lines, start=1)]

        source_payload = [
            {
                "name": table.name,
                "available_dataframe_names": [table.name],
                "columns": table.columns,
                "inferred_types": table.inferred_types,
                "preview_rows": table.preview_rows[:3],
            }
            for table in session.source_tables
        ]
        step_preview_payload = [
            {
                "step_id": preview.step_id,
                "operator": preview.operator.value if hasattr(preview.operator, "value") else str(preview.operator),
                "output_table": preview.output_table,
                "columns": preview.columns,
                "preview_rows": preview.preview_rows[:3],
                "added_columns": preview.added_columns,
                "removed_columns": preview.removed_columns,
                "renamed_columns": preview.renamed_columns,
            }
            for preview in candidate.step_previews
        ]

        locked_prefix = [step.model_dump(mode="json") for step in steps[:mutable_start_index]]
        mutable_suffix = [step.model_dump(mode="json") for step in steps[mutable_start_index:]]
        selected_guidance = self._selected_step_guidance(selected_step.operator if selected_step else None)

        return "\n".join(
            [
                "You are revising pandas transformation code for an interactive data-preparation app.",
                "",
                "Task contract:",
                f"- Lines 1 through {max(0, mutable_line_index - 1)} are locked and must remain byte-for-byte unchanged.",
                f"- You may rewrite line {mutable_line_index} and anything after it.",
                "- Return the full revised code as JSON only.",
                "- The revised pipeline may diverge from the current target schema, but it must remain executable, normalizable, and valid for downstream review.",
                "",
                "General constraints:",
                "- Source DataFrames are already loaded; do not add imports, file I/O, print statements, helper functions, comments, or markdown.",
                "- Use only the real source dataframe names listed below plus new descriptive intermediate dataframe names that you introduce.",
                "- Do not use placeholder aliases such as test_0, test_1, df, left_df, or right_df unless one is truly a real source name.",
                "- Source dataframes are read-only. Never reassign or mutate a source dataframe name.",
                "- Every table-level transformation must assign to a fresh dataframe variable name.",
                "- The only allowed multi-line scaffold is: new_df = old_df.copy() followed by one or more new_df[...] = ... column updates.",
                "- Never mutate a dataframe created by rename, merge, groupby, concat, melt, or pivot; create a fresh .copy() dataframe first.",
                "- Do not reuse the same dataframe variable as the output of multiple table-level steps.",
                "- Revise the selected step by editing its existing operator semantics instead of silently replacing it with a different operator family.",
                "- Interpret user-mentioned column names against the selected step's current output schema, params, and downstream dependencies.",
                "- If you change column names, update all downstream references that depend on those columns, or restore compatible columns later before they are required.",
                "- Prefer the smallest coherent suffix edit that satisfies the instruction.",
                "- Preserve unaffected downstream steps, their order, and their operator families unless the instruction explicitly changes them or downstream consistency makes a change unavoidable.",
                "- Do not remove downstream aggregation, formatting, selection, or export-preparation steps unless the instruction explicitly removes that behavior or the revised columns make that step impossible after coherent propagation.",
                "- Do not silently change one step into another incompatible semantic shape if a same-purpose revision can satisfy the instruction.",
                "- For rename-oriented changes, preserve unaffected renamed outputs unless the instruction explicitly changes or removes them.",
                "- When grouping rows, use named aggregation tuples only.",
                "- final_output must be assigned exactly once and only on the final code line.",
                "- Each returned array item must be one complete Python statement with no embedded line breaks.",
                "",
                *selected_guidance,
                "",
                "Return JSON only in this exact shape:",
                "{",
                '  "code": [',
                '    "python statement 1",',
                '    "python statement 2"',
                "  ]",
                "}",
                "",
                f"User instruction: {text}",
                f"Selected node id: {node_id}",
                f"Selected step index: {mutable_start_index}",
                f"Selected code line index: {mutable_line_index}",
                f"Selected step: {self._compact_json(selected_step.model_dump(mode='json') if selected_step else None)}",
                f"Selected step preview: {self._compact_json(selected_preview.model_dump(mode='json') if selected_preview else None)}",
                f"Selected step assessment: {self._compact_json(selected_assessment.model_dump(mode='json') if selected_assessment else None)}",
                f"Selected step warnings: {self._compact_json(selected_warning_items)}",
                f"Current mutable dependency summary: {self._compact_json(self._downstream_dependency_summary(current_traces, mutable_start_index))}",
                "",
                f"Supported operators after normalization: {self._compact_json(OPERATOR_SIGNATURES)}",
                f"Source tables: {self._compact_json(source_payload)}",
                f"Target schema: {self._compact_json([field.model_dump(mode='json') for field in session.target_schema])}",
                f"Target sample rows: {self._compact_json(session.target_samples[:3])}",
                f"Current step previews: {self._compact_json(step_preview_payload)}",
                f"Current pipeline summary: {self._compact_json(candidate.summary)}",
                "",
                "Current transformation code:",
                *current_code,
                "",
                f"Locked prefix steps: {self._compact_json(locked_prefix)}",
                f"Mutable suffix steps: {self._compact_json(mutable_suffix)}",
            ]
        )

    def _repair_prompt(
        self,
        *,
        session: Session,
        candidate: CandidatePipeline,
        node_id: str,
        mutable_start_index: int,
        mutable_line_index: int,
        text: str,
        original_code_lines: list[str],
        invalid_code_lines: list[str],
        failure_reason: str,
        current_traces: list[StepDependencyTrace],
    ) -> str:
        steps = candidate.pipeline_spec.steps
        selected_step = steps[mutable_start_index] if steps else None
        selected_warning_items = [
            item.model_dump(mode="json")
            for item in candidate.warning_items
            if selected_step and selected_step.step_id in item.node_ids
        ]
        selected_assessment = next((item for item in candidate.node_assessments if selected_step and item.node_id == selected_step.step_id), None)
        current_code = [f"{index}. {line}" for index, line in enumerate(original_code_lines, start=1)]
        invalid_code = [f"{index}. {line}" for index, line in enumerate(invalid_code_lines, start=1)]
        selected_guidance = self._selected_step_guidance(selected_step.operator if selected_step else None)

        return "\n".join(
            [
                "The first revision attempt failed validation. Repair it while preserving the locked code prefix.",
                "",
                "Repair contract:",
                f"- Lines 1 through {max(0, mutable_line_index - 1)} are locked and must remain byte-for-byte unchanged.",
                f"- Repair line {mutable_line_index} and anything after it.",
                "- Return the full repaired code as JSON only.",
                "- The repaired pipeline may diverge from the current target schema, but it must remain executable, normalizable, and valid for downstream review.",
                "",
                "General constraints:",
                "- Keep the selected step change aligned with the user instruction while preserving downstream consistency.",
                "- Keep the selected step in the same operator family unless the instruction explicitly requires a different transformation type.",
                "- Interpret user-mentioned column names against the selected step's current output schema, params, and downstream dependencies.",
                "- If you change column names, update all downstream references that depend on those columns, or restore compatible columns later before they are required.",
                "- Prefer the smallest coherent suffix edit that satisfies the instruction.",
                "- Preserve unaffected downstream steps, their order, and their operator families unless the instruction explicitly changes them or downstream consistency makes a change unavoidable.",
                "- Do not remove downstream aggregation, formatting, selection, or export-preparation steps unless the instruction explicitly removes that behavior or the repaired columns make that step impossible after coherent propagation.",
                "- Do not silently change one step into another incompatible semantic shape if a same-purpose revision can satisfy the instruction.",
                "- For rename-oriented changes, preserve unaffected renamed outputs unless the instruction explicitly changes or removes them.",
                "- Source dataframes are read-only.",
                "- Every table-level transformation must assign to a fresh dataframe variable name.",
                "- The only allowed multi-line scaffold is: new_df = old_df.copy() followed by one or more new_df[...] = ... column updates.",
                "- Use named aggregation tuples only.",
                "- final_output must be assigned exactly once and only on the final code line.",
                "- Each returned array item must be one complete Python statement with no embedded line breaks.",
                "",
                *selected_guidance,
                "",
                "Return JSON only in this exact shape:",
                "{",
                '  "code": [',
                '    "python statement 1",',
                '    "python statement 2"',
                "  ]",
                "}",
                "",
                f"User instruction: {text}",
                f"Selected node id: {node_id}",
                f"Selected step index: {mutable_start_index}",
                f"Selected code line index: {mutable_line_index}",
                f"Selected step: {self._compact_json(selected_step.model_dump(mode='json') if selected_step else None)}",
                f"Selected step assessment: {self._compact_json(selected_assessment.model_dump(mode='json') if selected_assessment else None)}",
                f"Selected step warnings: {self._compact_json(selected_warning_items)}",
                f"Current mutable dependency summary: {self._compact_json(self._downstream_dependency_summary(current_traces, mutable_start_index))}",
                f"Validation failure: {failure_reason}",
                "",
                "Current transformation code:",
                *current_code,
                "",
                "Invalid revised code:",
                *(invalid_code or ["(no valid revised code was returned)"]),
                "",
                f"Source tables: {self._compact_json([{'name': table.name, 'columns': table.columns, 'preview_rows': table.preview_rows[:3]} for table in session.source_tables])}",
                f"Target schema: {self._compact_json([field.model_dump(mode='json') for field in session.target_schema])}",
                f"Target sample rows: {self._compact_json(session.target_samples[:3])}",
                f"Current pipeline summary: {self._compact_json(candidate.summary)}",
            ]
        )

    async def _complete_code_request(self, prompt: str, system_prompt: str) -> str | None:
        return await llm_client.complete_text(
            prompt=prompt,
            model=settings.generation_model,
            temperature=0.1,
            top_p=0.8,
            system_prompt=system_prompt,
        )

    def _validate_revision_attempt(
        self,
        *,
        session: Session,
        candidate: CandidatePipeline,
        mutable_start_index: int,
        mutable_line_index: int,
        original_code_lines: list[str],
        revised_code_lines: list[str],
        origin_label: str,
    ) -> PipelineSpec:
        if not revised_code_lines:
            raise ValueError(f"{origin_label} returned invalid revision code.")

        self._validate_locked_code_prefix(original_code_lines, revised_code_lines, mutable_line_index)
        structure_validator.validate_new_code(session, revised_code_lines, origin=f"{origin_label} code")
        revised = self._normalize_code(revised_code_lines)
        self.validate_revised_spec(session, candidate, revised, mutable_start_index)
        return revised

    def _downstream_dependency_summary(
        self,
        traces: list[StepDependencyTrace],
        mutable_start_index: int,
    ) -> list[dict[str, Any]]:
        return [
            {
                "step_id": trace.step_id,
                "operator": trace.operator,
                "line_index": trace.line_index,
                "inputs": trace.inputs,
                "output": trace.output,
                "required_input_columns": trace.required_input_columns,
                "output_columns": trace.output_columns,
            }
            for trace in traces[mutable_start_index:]
        ]

    def _validate_locked_code_prefix(
        self,
        original_code_lines: list[str],
        revised_code_lines: list[str],
        mutable_line_index: int,
    ) -> None:
        locked_prefix = original_code_lines[: max(0, mutable_line_index - 1)]
        revised_prefix = revised_code_lines[: len(locked_prefix)]
        if len(revised_prefix) < len(locked_prefix):
            raise ValueError("The revised code removed locked lines before the selected node.")
        if [line.rstrip() for line in locked_prefix] != [line.rstrip() for line in revised_prefix]:
            raise ValueError("The revised code changed a locked line before the selected node.")

    def _rewrite_validation_error(
        self,
        candidate: CandidatePipeline,
        revised: PipelineSpec,
        mutable_start_index: int,
        exc: Exception,
    ) -> ValueError:
        message = str(exc)
        selected_original = candidate.pipeline_spec.steps[mutable_start_index]
        selected_revised = revised.steps[mutable_start_index] if len(revised.steps) > mutable_start_index else None

        dependency_failure_markers = (
            "missing column",
            "missing source column",
            "formats missing date column",
            "groups by missing column",
            "joins on missing",
            "unknown input table",
        )
        if any(marker in message for marker in dependency_failure_markers):
            original_label = selected_original.operator.value.replace("_", " ")
            revised_label = (
                selected_revised.operator.value.replace("_", " ")
                if selected_revised is not None
                else "missing step"
            )
            return ValueError(
                f'Revision changed the selected {original_label} step into a downstream-incompatible suffix '
                f'({revised_label}). {message}'
            )
        return ValueError(message)

    def _validate_selected_step_semantics(
        self,
        candidate: CandidatePipeline,
        revised: PipelineSpec,
        mutable_start_index: int,
    ) -> None:
        if mutable_start_index >= len(candidate.pipeline_spec.steps):
            return
        if mutable_start_index >= len(revised.steps):
            raise ValueError("The revised pipeline removed the selected step.")

        selected_original = candidate.pipeline_spec.steps[mutable_start_index]
        selected_revised = revised.steps[mutable_start_index]
        if selected_original.operator == selected_revised.operator:
            if selected_original.operator == OperatorType.RENAME:
                original_suffix_ops = [step.operator for step in candidate.pipeline_spec.steps[mutable_start_index + 1 :]]
                revised_suffix_ops = [step.operator for step in revised.steps[mutable_start_index + 1 :]]
                if revised_suffix_ops[: len(original_suffix_ops)] != original_suffix_ops:
                    original_labels = ", ".join(op.value for op in original_suffix_ops) or "(none)"
                    revised_labels = ", ".join(op.value for op in revised_suffix_ops) or "(none)"
                    raise ValueError(
                        "Revision changed the downstream transformation sequence after the selected rename step. "
                        f"Expected downstream operators to stay coherent as [{original_labels}] but received [{revised_labels}]. "
                        "Keep downstream aggregation, formatting, joins, and column-selection steps intact unless the user explicitly changes them."
                    )
            return

        original_label = selected_original.operator.value.replace("_", " ")
        revised_label = selected_revised.operator.value.replace("_", " ")
        raise ValueError(
            f"Revision changed the selected {original_label} step into a different operator ({revised_label}). "
            "Keep the selected step in the same operator family and propagate the consequences through the downstream suffix."
        )

    def _selected_step_guidance(self, operator: OperatorType | None) -> list[str]:
        if operator is None:
            return []
        if operator == OperatorType.RENAME:
            return [
                "Selected-step guidance:",
                "- The selected step is a rename step.",
                "- Revise the rename mapping itself instead of replacing the step with add-columns or copy-style aliasing.",
                "- Preserve unaffected rename mappings unless the user explicitly changes or removes them.",
                "- Keep downstream aggregation, formatting, and column-selection steps intact unless the user instruction explicitly changes those later behaviors.",
            ]
        if operator == OperatorType.JOIN:
            return [
                "Selected-step guidance:",
                "- The selected step is a join step.",
                "- Keep it as a join and revise join keys, join type, or participating tables without breaking downstream inputs.",
            ]
        if operator == OperatorType.GROUPBY:
            return [
                "Selected-step guidance:",
                "- The selected step is a groupby step.",
                "- Keep it as a groupby and revise grouping keys or aggregations while preserving downstream columns or updating them coherently.",
            ]
        if operator == OperatorType.DATE_FORMATTING:
            return [
                "Selected-step guidance:",
                "- The selected step is a date-formatting step.",
                "- Keep it as a date-formatting step and preserve the downstream date column contract.",
            ]
        if operator == OperatorType.DROP_COLUMNS:
            return [
                "Selected-step guidance:",
                "- The selected step is a drop-columns step.",
                "- Keep it as a column-selection step and revise keep/drop choices without replacing it with an unrelated operator.",
            ]
        if operator == OperatorType.ADD_COLUMNS:
            return [
                "Selected-step guidance:",
                "- The selected step is an add-columns step.",
                "- Keep it as a field-derivation step and update derived columns without collapsing downstream dependencies.",
            ]
        if operator == OperatorType.COLUMN_ARITHMETIC:
            return [
                "Selected-step guidance:",
                "- The selected step is a column-arithmetic step.",
                "- Keep it as a computed-field step and preserve its output column contract or update downstream references coherently.",
            ]
        if operator == OperatorType.SOURCE_TABLE:
            return [
                "Selected-step guidance:",
                "- The selected step is a source-table step.",
                "- Keep it as a source-table step and revise source usage without mutating or overwriting source dataframe names.",
            ]
        return [
            "Selected-step guidance:",
            f"- The selected step is a {operator.value.replace('_', ' ')} step.",
            "- Keep it in the same operator family while making the requested revision and preserving downstream executability.",
        ]

    def _comparable_step_payload(self, step: PipelineStep) -> dict[str, Any]:
        params = {key: value for key, value in step.params.items() if key != "_code_line_index"}
        return {
            "step_id": step.step_id,
            "operator": step.operator.value,
            "inputs": list(step.inputs),
            "output": step.output,
            "params": params,
        }

    def _normalize_code(self, revised_code_lines: list[str]) -> PipelineSpec:
        try:
            return self.normalizer.normalize(revised_code_lines)
        except Exception as exc:
            raise ValueError("The revision model returned code that could not be normalized into a pipeline.") from exc

    def _parse_code_payload(self, raw: str | None) -> list[str]:
        if not raw:
            return []
        payload = self._parse_json_payload(raw)
        if isinstance(payload, dict):
            code = payload.get("code", [])
        elif isinstance(payload, list):
            code = payload
        else:
            return []
        if not isinstance(code, list):
            return []
        return [line for line in code if isinstance(line, str) and line.strip()]

    def _compact_json(self, value: Any) -> str:
        return json.dumps(value, ensure_ascii=True, default=str)

    def _parse_json_payload(self, raw: str) -> Any:
        text = raw.strip()
        if text.startswith("```"):
            lines = [line for line in text.splitlines() if not line.strip().startswith("```")]
            text = "\n".join(lines).strip()
        try:
            return json.loads(text)
        except Exception:
            return None


revision_service = PipelineRevisionService()
