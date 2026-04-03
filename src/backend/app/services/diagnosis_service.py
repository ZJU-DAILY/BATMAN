from __future__ import annotations

import json
import re
from typing import Any

from app.config import settings
from app.engine.utils import new_id
from app.models import CandidatePipeline, NodeAssessment, NodeStatus, RevisionRecord, ReviewSnapshot, Session, WarningItem
from app.services.llm_client import llm_client


class DiagnosisService:
    async def enrich_candidate(self, session: Session, candidate: CandidatePipeline) -> CandidatePipeline:
        warning_items = self._fallback_warning_items(candidate) if not candidate.validation_summary.pipeline_correct else []
        payload = await self._generate_warning_items(session, candidate)
        if payload:
            parsed_items = self._parse_warning_items(payload, candidate)
            if parsed_items:
                warning_items = self._merge_warning_items(parsed_items, warning_items)

        candidate.warning_items = warning_items
        candidate.node_assessments = self._node_assessments(candidate, warning_items)
        return candidate

    async def reconcile_revision(
        self,
        session: Session,
        candidate: CandidatePipeline,
        revision_record: RevisionRecord,
        after_snapshot: ReviewSnapshot,
    ) -> CandidatePipeline:
        if not llm_client.enabled:
            return candidate

        draft_ambiguity_warnings = [item for item in candidate.warning_items if item.source == "ambiguity"]
        if not draft_ambiguity_warnings:
            return candidate

        payload = await self._generate_reconciled_warning_items(
            session,
            candidate,
            revision_record,
            after_snapshot,
            draft_ambiguity_warnings,
        )
        if not payload:
            return candidate

        reconciled_ambiguity_items = self._parse_reconciled_warning_items(payload, candidate)
        if reconciled_ambiguity_items is None:
            return candidate
        reconciled_ambiguity_items = self._filter_reconciled_ambiguity_items(revision_record, reconciled_ambiguity_items)

        non_ambiguity_items = [item for item in candidate.warning_items if item.source != "ambiguity"]
        candidate.warning_items = self._merge_warning_items(non_ambiguity_items, reconciled_ambiguity_items)
        candidate.node_assessments = self._node_assessments(candidate, candidate.warning_items)
        return candidate

    async def _generate_warning_items(self, session: Session, candidate: CandidatePipeline) -> dict[str, Any] | None:
        if not llm_client.enabled:
            return None

        response = await llm_client.complete_text(
            prompt=self._prompt(session, candidate),
            model=settings.explanation_model,
            temperature=max(0.05, session.settings.bat_temperature),
            top_p=session.settings.bat_top_p,
            system_prompt=(
                "You diagnose which transformation steps most likely need user review. "
                "Use the final validation result and step-level previews to produce concise warning items. "
                "You may report either concrete data-quality problems or business-semantics ambiguities that still need human confirmation. "
                "Do not mark the input node as problematic. "
                "Return valid JSON only."
            ),
        )
        if not response:
            return None
        return self._parse_json_payload(response)

    async def _generate_reconciled_warning_items(
        self,
        session: Session,
        candidate: CandidatePipeline,
        revision_record: RevisionRecord,
        after_snapshot: ReviewSnapshot,
        draft_ambiguity_warnings: list[WarningItem],
    ) -> dict[str, Any] | None:
        response = await llm_client.complete_text(
            prompt=self._revision_reconciliation_prompt(
                session,
                candidate,
                revision_record,
                after_snapshot,
                draft_ambiguity_warnings,
            ),
            model=settings.explanation_model,
            temperature=max(0.05, session.settings.bat_temperature),
            top_p=session.settings.bat_top_p,
            system_prompt=(
                "You decide whether ambiguity warnings are still valid after a user-requested revision. "
                "Compare the before and after snapshots carefully. "
                "Only keep ambiguity warnings that are still genuinely unresolved in the revised candidate. "
                "Return valid JSON only."
            ),
        )
        if not response:
            return None
        return self._parse_json_payload(response)

    def _fallback_warning_items(self, candidate: CandidatePipeline) -> list[WarningItem]:
        validation = candidate.validation_summary
        items: list[WarningItem] = []

        missing_fields = [check.field_name for check in validation.field_checks if check.status == "Missing" and check.required]
        if missing_fields:
            node_ids = self._related_node_ids(candidate, missing_fields)
            items.append(
                WarningItem(
                    id=new_id("warning"),
                    title="Required target fields are missing",
                    detail=f"Missing fields: {', '.join(missing_fields)}.",
                    node_ids=node_ids,
                    source="heuristic",
                )
            )

        mismatched_fields = [check.field_name for check in validation.field_checks if check.status == "Type mismatch"]
        if mismatched_fields:
            node_ids = self._related_node_ids(candidate, mismatched_fields)
            items.append(
                WarningItem(
                    id=new_id("warning"),
                    title="Some output fields use the wrong value format",
                    detail=f"Type mismatches: {', '.join(mismatched_fields)}.",
                    node_ids=node_ids,
                    source="heuristic",
                )
            )

        if validation.example_similarity is not None and validation.example_similarity < 0.65:
            items.append(
                WarningItem(
                    id=new_id("warning"),
                    title="Generated rows do not resemble the existing target table",
                    detail="The output structure may be close, but the value style or formatting still differs from the target rows.",
                    node_ids=self._last_step_node_ids(candidate),
                    source="heuristic",
                )
            )

        if not validation.executable:
            items.append(
                WarningItem(
                    id=new_id("warning"),
                    title="Pipeline execution failed",
                    detail="The candidate did not produce a usable output table.",
                    node_ids=self._last_step_node_ids(candidate),
                    source="heuristic",
                )
            )

        if not items:
            items.append(
                WarningItem(
                    id=new_id("warning"),
                    title="The output still needs refinement",
                    detail="The final result does not yet satisfy all target table checks.",
                    node_ids=self._last_step_node_ids(candidate),
                    source="heuristic",
                )
            )
        return items[:4]

    def _node_assessments(self, candidate: CandidatePipeline, warning_items: list[WarningItem]) -> list[NodeAssessment]:
        warning_map: dict[str, list[WarningItem]] = {}
        for item in warning_items:
            for node_id in item.node_ids:
                warning_map.setdefault(node_id, []).append(item)

        assessments = [NodeAssessment(node_id="node_input", status=NodeStatus.OK, reason="Source tables are available.")]
        for preview in candidate.step_previews:
            related = warning_map.get(preview.step_id, [])
            assessments.append(
                NodeAssessment(
                    node_id=preview.step_id,
                    status=NodeStatus.ISSUE if related else NodeStatus.OK,
                    reason=related[0].title if related else "No warning was attached to this step.",
                )
            )

        output_related = warning_map.get("node_output", [])
        assessments.append(
            NodeAssessment(
                node_id="node_output",
                status=NodeStatus.ISSUE if output_related else NodeStatus.OK,
                reason=output_related[0].title if output_related else "The final output passed the current checks.",
            )
        )
        return assessments

    def _related_node_ids(self, candidate: CandidatePipeline, field_names: list[str]) -> list[str]:
        normalized = {name.lower() for name in field_names}
        related: list[str] = []
        for preview in reversed(candidate.step_previews):
            preview_terms = {column.lower() for column in preview.columns}
            preview_terms.update(column.lower() for column in preview.added_columns)
            preview_terms.update(key.lower() for key in preview.renamed_columns)
            preview_terms.update(value.lower() for value in preview.renamed_columns.values())
            if normalized.intersection(preview_terms):
                related.append(preview.step_id)
            if len(related) >= 2:
                break
        if not related:
            related = self._last_step_node_ids(candidate)
        if "node_output" not in related:
            related.append("node_output")
        return related

    def _last_step_node_ids(self, candidate: CandidatePipeline) -> list[str]:
        last_step_id = candidate.step_previews[-1].step_id if candidate.step_previews else "node_output"
        if last_step_id == "node_output":
            return ["node_output"]
        return [last_step_id, "node_output"]

    def _prompt(self, session: Session, candidate: CandidatePipeline) -> str:
        validation = candidate.validation_summary.model_dump(mode="json")
        source_lines = [
            {
                "name": table.name,
                "columns": table.columns,
                "row_count": table.row_count,
                "preview_rows": table.preview_rows[:3],
            }
            for table in session.source_tables
        ]
        step_lines = []
        for preview, step in zip(candidate.step_previews, candidate.pipeline_spec.steps, strict=False):
            step_lines.append(
                {
                    "step_id": preview.step_id,
                    "operator": preview.operator.value,
                    "title": preview.title,
                    "inputs": step.inputs,
                    "row_count": preview.row_count,
                    "columns": preview.columns,
                    "added_columns": preview.added_columns,
                    "removed_columns": preview.removed_columns,
                    "renamed_columns": preview.renamed_columns,
                    "preview_rows": preview.preview_rows[:2],
                }
            )
        return (
            "Diagnose a generated data-preparation pipeline.\n"
            "Rules:\n"
            "- Return 0 to 4 warning items.\n"
            "- You may return warning items even when the pipeline is executable or structurally correct if a business choice still needs human confirmation.\n"
            "- If validation_summary.pipeline_correct is true, only return ambiguity-style confirmation warnings. Do not invent concrete failures that are not supported by validation.\n"
            "- For executable pipelines, prefer 0 to 2 warnings and focus only on the most important user-facing uncertainties.\n"
            "- Prefer a single strongest warning over several weak warnings when one business choice dominates the uncertainty.\n"
            "- Use source=\"ambiguity\" for low-confidence business choices that still need confirmation.\n"
            "- Use source=\"validation\" for concrete output problems such as missing fields, bad value formats, or execution issues.\n"
            "- If the pipeline looks both structurally correct and semantically unambiguous, return an empty warnings array.\n"
            "- Do not warn about ordinary implementation details that already produce a correct-looking result, such as a constant join key used to broadcast a single-row lookup table.\n"
            "- When a table clearly looks like shared store metadata with one row and the join only broadcasts that metadata onto many fact rows, treat that as acceptable unless the output evidence suggests a real mismatch.\n"
            "- Do not warn about a straightforward date reformatting step when there is only one plausible date source column and the output dates already look normalized.\n"
            "- Only raise a date-related ambiguity warning when there are multiple plausible date columns, multiple plausible date grains, or clear evidence that the chosen interpretation may be wrong.\n"
            "- Only raise a semantic ambiguity warning when there are at least two realistically competing business meanings that fit the target field.\n"
            "- Do not treat missing rows in a short preview as proof of a problem.\n"
            "- Each warning item must point to the most likely step ids and may also include node_output.\n"
            "- Never use node_input.\n"
            "- Keep titles short and concrete.\n"
            "- Return JSON only in this shape:\n"
            '{"warnings": [{"title": "short title", "detail": "one sentence", "node_ids": ["step_2", "node_output"], "source": "ambiguity"}]}\n\n'
            f"Source tables: {json.dumps(source_lines, ensure_ascii=True, default=str)}\n"
            f"Target schema: {json.dumps([field.model_dump(mode='json') for field in session.target_schema], ensure_ascii=True)}\n"
            f"Existing target rows: {json.dumps(session.target_samples[:3], ensure_ascii=True, default=str)}\n"
            f"Validation summary: {json.dumps(validation, ensure_ascii=True, default=str)}\n"
            f"Candidate source: {json.dumps(candidate.source, ensure_ascii=True, default=str)}\n"
            f"Pipeline rationale: {json.dumps(candidate.pipeline_spec.rationale, ensure_ascii=True, default=str)}\n"
            f"Pipeline steps: {json.dumps(step_lines, ensure_ascii=True, default=str)}\n"
            f"Final output preview: {json.dumps(candidate.final_preview_rows[:3], ensure_ascii=True, default=str)}\n"
        )

    def _revision_reconciliation_prompt(
        self,
        session: Session,
        candidate: CandidatePipeline,
        revision_record: RevisionRecord,
        after_snapshot: ReviewSnapshot,
        draft_ambiguity_warnings: list[WarningItem],
    ) -> str:
        return (
            "Reconcile ambiguity warnings after a user-requested revision.\n"
            "Rules:\n"
            "- Compare the before and after snapshots carefully.\n"
            "- This is a scoped reconciliation pass, not a fresh open-ended diagnosis pass.\n"
            "- The user feedback describes the intended business correction.\n"
            "- Start from the ambiguity that motivated the revision and decide whether that ambiguity cluster still remains.\n"
            "- Keep an ambiguity warning only if the revised candidate still leaves that same business uncertainty unresolved.\n"
            "- If the revised candidate clearly implements the user's requested business choice, remove the corresponding ambiguity warning.\n"
            "- Do not introduce unrelated new ambiguity warnings that were not part of the user's requested correction, even if they could be debated in isolation.\n"
            "- Only surface a new ambiguity warning if it is a direct continuation or restatement of the same business choice the user just revised.\n"
            "- Do not treat changed wording, summaries, or explanations as evidence by themselves. Judge the actual step previews, validation summary, and output preview.\n"
            "- You may rewrite the remaining ambiguity warnings so they match the revised candidate.\n"
            "- If the revised candidate is structurally correct and clearly follows the user instruction, prefer an empty warnings array over inventing a replacement confirmation warning.\n"
            "- Only decide ambiguity warnings. Do not recreate validation or execution warnings here.\n"
            "- Return JSON only in this shape:\n"
            '{"warnings": [{"title": "short title", "detail": "one sentence", "node_ids": ["step_2", "node_output"], "source": "ambiguity"}]}\n\n'
            f"User feedback: {json.dumps(revision_record.text, ensure_ascii=True, default=str)}\n"
            f"Selected node id: {json.dumps(revision_record.node_id, ensure_ascii=True, default=str)}\n"
            f"Before snapshot: {json.dumps(revision_record.before_snapshot.model_dump(mode='json'), ensure_ascii=True, default=str)}\n"
            f"After snapshot: {json.dumps(after_snapshot.model_dump(mode='json'), ensure_ascii=True, default=str)}\n"
            f"Original selected-node ambiguity warnings: {json.dumps([item.model_dump(mode='json') for item in revision_record.before_snapshot.selected_node_warning_items], ensure_ascii=True, default=str)}\n"
            f"Draft ambiguity warnings: {json.dumps([item.model_dump(mode='json') for item in draft_ambiguity_warnings], ensure_ascii=True, default=str)}\n"
            f"Current candidate source: {json.dumps(candidate.source, ensure_ascii=True, default=str)}\n"
            f"Target schema: {json.dumps([field.model_dump(mode='json') for field in session.target_schema], ensure_ascii=True)}\n"
        )

    def _parse_warning_items(self, payload: dict[str, Any], candidate: CandidatePipeline) -> list[WarningItem]:
        raw_items = payload.get("warnings", []) if isinstance(payload, dict) else []
        if not isinstance(raw_items, list):
            return []
        valid_node_ids = {preview.step_id for preview in candidate.step_previews} | {"node_output"}
        items: list[WarningItem] = []
        for raw in raw_items:
            if not isinstance(raw, dict):
                continue
            title = str(raw.get("title", "")).strip()
            if not title:
                continue
            node_ids = [node_id for node_id in raw.get("node_ids", []) if isinstance(node_id, str) and node_id in valid_node_ids]
            if not node_ids:
                node_ids = self._last_step_node_ids(candidate)
            detail = str(raw.get("detail", "")).strip()
            source = str(raw.get("source", "")).strip().lower()
            if not source:
                source = self._infer_warning_source(title, detail)
            if candidate.validation_summary.pipeline_correct and source != "ambiguity":
                continue
            items.append(
                WarningItem(
                    id=new_id("warning"),
                    title=title,
                    detail=detail,
                    node_ids=node_ids,
                    source=source,
                )
            )
        return items[:4]

    def _filter_reconciled_ambiguity_items(
        self,
        revision_record: RevisionRecord,
        items: list[WarningItem],
    ) -> list[WarningItem]:
        if not items:
            return items

        scope_node_ids = {revision_record.node_id, "node_output"}
        scope_terms = self._warning_scope_terms(revision_record.text)
        for warning in revision_record.before_snapshot.selected_node_warning_items:
            scope_node_ids.update(warning.node_ids)
            scope_terms.update(self._warning_scope_terms(warning.title))
            scope_terms.update(self._warning_scope_terms(warning.detail))

        if not scope_terms:
            return items[:4]

        filtered: list[WarningItem] = []
        for item in items:
            if scope_node_ids.intersection(item.node_ids):
                filtered.append(item)
                continue

            item_terms = self._warning_scope_terms(f"{item.title} {item.detail}")
            if scope_terms.intersection(item_terms):
                filtered.append(item)

        return filtered[:4]

    def _parse_reconciled_warning_items(
        self,
        payload: dict[str, Any],
        candidate: CandidatePipeline,
    ) -> list[WarningItem] | None:
        if not isinstance(payload, dict):
            return None
        raw_items = payload.get("warnings", [])
        if not isinstance(raw_items, list):
            return None
        valid_node_ids = {preview.step_id for preview in candidate.step_previews} | {"node_output"}
        items: list[WarningItem] = []
        for raw in raw_items:
            if not isinstance(raw, dict):
                continue
            title = str(raw.get("title", "")).strip()
            if not title:
                continue
            node_ids = [node_id for node_id in raw.get("node_ids", []) if isinstance(node_id, str) and node_id in valid_node_ids]
            if not node_ids:
                node_ids = self._last_step_node_ids(candidate)
            items.append(
                WarningItem(
                    id=new_id("warning"),
                    title=title,
                    detail=str(raw.get("detail", "")).strip(),
                    node_ids=node_ids,
                    source="ambiguity",
                )
            )
        return items[:4]

    def _infer_warning_source(self, title: str, detail: str) -> str:
        text = f"{title} {detail}".lower()
        ambiguity_markers = (
            "confirm",
            "ambigu",
            "plausible",
            "could mean",
            "need confirmation",
            "which ",
            "business rule",
            "business meaning",
            "business choice",
        )
        validation_markers = (
            "missing",
            "wrong value format",
            "type mismatch",
            "execution failed",
            "usable output",
        )
        if any(marker in text for marker in ambiguity_markers):
            return "ambiguity"
        if any(marker in text for marker in validation_markers):
            return "validation"
        return "llm"

    def _merge_warning_items(self, primary: list[WarningItem], secondary: list[WarningItem]) -> list[WarningItem]:
        merged: list[WarningItem] = []
        seen: set[tuple[str, str, tuple[str, ...], str]] = set()
        for item in [*primary, *secondary]:
            key = (
                item.title.strip().lower(),
                item.detail.strip().lower(),
                tuple(sorted(item.node_ids)),
                item.source.strip().lower(),
            )
            if key in seen:
                continue
            seen.add(key)
            merged.append(item)
        return merged[:4]

    def _parse_json_payload(self, raw: str) -> dict[str, Any] | None:
        text = raw.strip()
        if text.startswith("```"):
            lines = [line for line in text.splitlines() if not line.strip().startswith("```")]
            text = "\n".join(lines).strip()
        try:
            payload = json.loads(text)
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None

    def _warning_scope_terms(self, text: str) -> set[str]:
        stopwords = {
            "the",
            "and",
            "for",
            "with",
            "that",
            "this",
            "from",
            "into",
            "your",
            "user",
            "step",
            "steps",
            "pipeline",
            "selected",
            "current",
            "revised",
            "candidate",
            "business",
            "choice",
            "confirm",
            "confirmation",
            "needs",
            "need",
            "please",
            "should",
            "still",
            "correct",
            "output",
            "field",
            "fields",
            "mapping",
            "report",
            "table",
            "tables",
            "daily",
        }
        return {
            token
            for token in re.findall(r"[a-zA-Z0-9_]+", text.lower())
            if len(token) > 2 and token not in stopwords
        }


diagnosis_service = DiagnosisService()
