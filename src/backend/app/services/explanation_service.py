from __future__ import annotations

import json
from typing import Any

from app.config import settings
from app.models import CandidatePipeline, ExplanationDetail, NodeAssessment, NodeStatus, Session
from app.services.llm_client import llm_client


class ExplanationService:
    STEP_TEMPLATES = {
        "source_table": "This step brings one source table into the flow.",
        "rename": "This step changes column names so later steps can use clearer names.",
        "date_formatting": "This step rewrites date values into the format needed in the final table.",
        "groupby": "This step groups similar rows and calculates the values needed in the final table.",
        "join": "This step matches rows from another table and brings in extra information.",
        "drop_columns": "This step removes columns that are no longer needed.",
        "union": "This step combines matching rows into one table.",
        "pivot": "This step turns repeated row values into separate columns.",
        "unpivot": "This step turns selected columns into rows.",
        "column_arithmetic": "This step calculates a new value from existing columns.",
        "add_columns": "This step adds or updates a column needed later in the flow.",
    }

    async def enrich_candidate(self, session: Session, candidate: CandidatePipeline) -> CandidatePipeline:
        detail = session.settings.explanation_detail

        fallback_notes = self._fallback_step_notes(candidate, detail)
        candidate.node_explanations = self._fallback_node_explanations(session, candidate, fallback_notes)
        if not candidate.node_assessments:
            candidate.node_assessments = self._fallback_node_assessments(candidate)
        candidate.summary = self._default_summary(candidate)

        for preview in candidate.step_previews:
            preview.notes = fallback_notes.get(preview.step_id, preview.notes)

        payload = await self._generate_node_explanations(session, candidate, detail)
        if payload:
            candidate.summary = payload.get("summary") or candidate.summary
            node_explanations = payload.get("node_explanations", {})
            if isinstance(node_explanations, dict):
                merged_explanations = dict(candidate.node_explanations)
                for node_id, text in node_explanations.items():
                    if isinstance(text, str) and text.strip():
                        merged_explanations[node_id] = text.strip()
                candidate.node_explanations = merged_explanations

        for preview in candidate.step_previews:
            note = candidate.node_explanations.get(preview.step_id)
            if note:
                preview.notes = note
        return candidate

    async def _generate_node_explanations(
        self,
        session: Session,
        candidate: CandidatePipeline,
        detail: ExplanationDetail,
    ) -> dict[str, Any] | None:
        if not llm_client.enabled:
            return None

        response = await llm_client.complete_text(
            prompt=self._node_explanations_prompt(session, candidate, detail),
            model=settings.explanation_model,
            temperature=max(0.05, session.settings.bat_temperature),
            top_p=session.settings.bat_top_p,
            system_prompt=(
                "You explain automatic data preparation pipelines to non-technical demo users. "
                "Use short, plain English grounded in source tables, target fields, sample rows, and step-level table changes. "
                "For each step, say what changed in the table and why that step exists. "
                "Avoid technical words such as schema, operator, groupby, aggregation, grain, join key, dataframe, node, or step id unless absolutely necessary. "
                "If a technical idea is unavoidable, immediately restate it in everyday language. "
                "Write the overall summary as a concise explanation of the pipeline flow in transformation order. "
                "Return valid JSON only."
            ),
        )
        if not response:
            return None

        parsed = self._parse_json_payload(response)
        if not isinstance(parsed, dict):
            return None
        if not isinstance(parsed.get("node_explanations"), dict):
            parsed["node_explanations"] = {}
        return parsed

    def _fallback_step_notes(self, candidate: CandidatePipeline, detail: ExplanationDetail) -> dict[str, str]:
        notes: dict[str, str] = {}
        for preview, step in zip(candidate.step_previews, candidate.pipeline_spec.steps, strict=False):
            base = self.STEP_TEMPLATES.get(step.operator.value, "This step moves the data closer to the target schema.")
            notes[preview.step_id] = self._compose_notes(base, step.notes, detail)
        return notes

    def _fallback_node_explanations(
        self,
        session: Session,
        candidate: CandidatePipeline,
        fallback_notes: dict[str, str],
    ) -> dict[str, str]:
        input_text = (
            f"The pipeline starts from {len(session.source_tables)} input table(s), "
            "which provide the raw columns used to construct the target result."
        )
        output_text = self._default_summary(candidate)
        explanations = {
            "node_input": input_text,
            "node_output": output_text,
        }
        explanations.update(fallback_notes)
        return explanations

    def _fallback_node_assessments(self, candidate: CandidatePipeline) -> list[NodeAssessment]:
        warning_map = {
            node_id: item.title
            for item in candidate.warning_items
            for node_id in item.node_ids
        }
        assessments: list[NodeAssessment] = [
            NodeAssessment(node_id="node_input", status=NodeStatus.OK, reason="Input tables are available for pipeline generation."),
            NodeAssessment(
                node_id="node_output",
                status=NodeStatus.ISSUE if "node_output" in warning_map else NodeStatus.OK,
                reason=self._default_summary(candidate),
            ),
        ]
        for preview in candidate.step_previews:
            status = NodeStatus.ISSUE if preview.step_id in warning_map else NodeStatus.OK
            reason = warning_map.get(preview.step_id, "No obvious issue was detected for this step.")
            assessments.append(NodeAssessment(node_id=preview.step_id, status=status, reason=reason))
        return assessments

    def _compose_notes(self, base: str, extra: str, detail: ExplanationDetail) -> str:
        if detail == ExplanationDetail.BRIEF:
            return base
        if detail == ExplanationDetail.DETAILED and extra:
            return f"{base} {extra}"
        return extra or base

    def _default_summary(self, candidate: CandidatePipeline) -> str:
        phrases = {
            "rename": "renames source fields to match the target names",
            "date_formatting": "normalizes date values into the target format",
            "groupby": "aggregates rows to the required business grain",
            "join": "joins in supporting fields from related tables",
            "drop_columns": "keeps only the target-ready columns",
            "union": "appends compatible records into one table",
            "pivot": "reshapes row values into output columns",
            "unpivot": "reshapes wide columns into row-wise records",
            "column_arithmetic": "computes derived values from existing fields",
            "add_columns": "adds derived fields needed downstream",
        }
        operators = [step.operator.value for step in candidate.pipeline_spec.steps if step.operator.value != "source_table"]
        ordered_phrases: list[str] = []
        for operator in operators:
            phrase = phrases.get(operator)
            if phrase and phrase not in ordered_phrases:
                ordered_phrases.append(phrase)

        has_confirmation_warning = any(item.source == "ambiguity" for item in candidate.warning_items)

        if ordered_phrases:
            if len(ordered_phrases) == 1:
                flow = ordered_phrases[0]
            elif len(ordered_phrases) == 2:
                flow = f"{ordered_phrases[0]} and {ordered_phrases[1]}"
            else:
                flow = f"{', '.join(ordered_phrases[:-1])}, and {ordered_phrases[-1]}"

            if candidate.validation_summary.pipeline_correct and has_confirmation_warning:
                return f"The pipeline {flow}, and the result is executable, but one or more business choices still need confirmation."
            if candidate.validation_summary.pipeline_correct:
                return f"The pipeline {flow} so the final table is ready for export and matches the target table."
            if candidate.validation_summary.executable:
                return f"The pipeline {flow}, and the result is executable but still needs review."
            return f"The pipeline {flow}, but it still needs refinement before export."

        if candidate.validation_summary.pipeline_correct and has_confirmation_warning:
            return "The current pipeline runs successfully, but one or more business choices still need confirmation."
        if candidate.validation_summary.pipeline_correct:
            return "The current pipeline runs successfully and matches the target table closely."
        if candidate.validation_summary.executable:
            return "The pipeline is executable, but the final result still needs review."
        return "The pipeline still needs refinement before it is ready to export."

    def _node_explanations_prompt(
        self,
        session: Session,
        candidate: CandidatePipeline,
        detail: ExplanationDetail,
    ) -> str:
        source_context = "\n".join(
            [
                (
                    f"- {table.name} ({table.filename})\n"
                    f"  columns: {', '.join(table.columns)}\n"
                    f"  sample rows: {self._compact_rows(table.preview_rows[:2])}"
                )
                for table in session.source_tables
            ]
        )
        target_context = "\n".join(
            [
                f"- {field.name} ({field.type.value}, required={field.required}): {field.description or 'No description'}"
                for field in session.target_schema
            ]
        )
        step_context = "\n".join(
            [
                (
                    f"- {preview.step_id}\n"
                    f"  operator: {preview.operator.value}\n"
                    f"  title: {preview.title}\n"
                    f"  inputs: {', '.join(step.inputs) or 'N/A'}\n"
                    f"  output: {preview.output_table}\n"
                    f"  columns after step: {', '.join(preview.columns)}\n"
                    f"  added columns: {', '.join(preview.added_columns) or 'None'}\n"
                    f"  removed columns: {', '.join(preview.removed_columns) or 'None'}\n"
                    f"  key params: {self._compact_json(step.params)}\n"
                    f"  warnings: {self._compact_json(preview.warnings)}\n"
                    f"  sample rows after step: {self._compact_rows(preview.preview_rows[:2])}"
                )
                for preview, step in zip(candidate.step_previews, candidate.pipeline_spec.steps, strict=False)
            ]
        )
        warning_lines = [warning for warning in candidate.validation_summary.warnings[:4]]
        warning_lines.extend(
            f"{item.title}: {item.detail}".strip(": ")
            for item in candidate.warning_items[:4]
        )
        warnings = "\n".join(f"- {warning}" for warning in warning_lines) or "- No warnings"
        sample_context = self._compact_rows(session.target_samples[:2]) if session.target_samples else "No target sample rows provided"
        return (
            "Explain the pipeline nodes in plain, easy English for a non-technical demo user.\n"
            f"Detail level: {detail.value}\n"
            "Requirements:\n"
            "- Write for someone who understands rows and columns but does not know data engineering jargon.\n"
            "- Make the summary a short pipeline story that explains the important changes in order.\n"
            "- The summary should focus on what the pipeline does, not just whether the final output is ready.\n"
            "- Do not use bullet points in the summary.\n"
            "- Use the actual table meaning implied by source names, target field descriptions, and sample rows.\n"
            "- Prefer everyday words like column, rows, match, add up, date format, and final table.\n"
            "- Avoid technical words such as schema, operator, groupby, aggregation, grain, join key, dataframe, node, or step id unless absolutely necessary.\n"
            "- Do not copy raw step titles or parameter names as the whole explanation unless they are real table or column names.\n"
            "- For each node, explain what changed in the table and why this step exists.\n"
            "- Explain node_input, each step node, and node_output.\n"
            "- Keep each explanation to one or two short sentences.\n"
            "- Return JSON only with this exact shape:\n"
            '{\n'
            '  "summary": "2-4 sentence pipeline summary describing the important transformations in order",\n'
            '  "node_explanations": {\n'
            '    "node_input": "business explanation",\n'
            '    "step_1": "business explanation",\n'
            '    "node_output": "business explanation"\n'
            '  }\n'
            '}\n\n'
            f"Source tables:\n{source_context}\n\n"
            f"Target schema:\n{target_context}\n\n"
            f"Target samples:\n{sample_context}\n\n"
            f"Pipeline steps:\n{step_context}\n\n"
            f"Validation warnings:\n{warnings}\n"
        )

    def _compact_rows(self, rows: list[dict[str, Any]]) -> str:
        if not rows:
            return "[]"
        return json.dumps(rows, ensure_ascii=True, default=str)

    def _compact_json(self, value: Any) -> str:
        if not value:
            return "{}"
        return json.dumps(value, ensure_ascii=True, default=str)

    def _parse_json_payload(self, raw: str) -> dict[str, Any] | None:
        text = raw.strip()
        if text.startswith("```"):
            lines = [line for line in text.splitlines() if not line.strip().startswith("```")]
            text = "\n".join(lines).strip()
        try:
            payload = json.loads(text)
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        return payload


explanation_service = ExplanationService()
