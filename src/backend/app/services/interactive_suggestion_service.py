from __future__ import annotations

import json
import re
from typing import Any

from app.models import CandidatePipeline, Session
from app.services.llm_client import llm_client


class InteractiveSuggestionService:
    async def generate(self, session: Session, candidate: CandidatePipeline, node_id: str, text: str) -> list[str]:
        trimmed = text.strip()
        if not trimmed:
            return []

        fallback = self._rank_suggestions(self._fallback(trimmed), candidate, node_id)
        if not llm_client.enabled:
            return fallback

        prompt = self._prompt(session, candidate, node_id, trimmed)
        response = await llm_client.complete_text(
            prompt=prompt,
            temperature=max(0.1, session.settings.bat_temperature),
            top_p=session.settings.bat_top_p,
            system_prompt=(
                "You write short continuation suggestions for non-technical users refining a data preparation pipeline. "
                "Keep the language plain, friendly, and business-focused. Avoid coding, SQL, pandas, or database jargon. "
                "Return valid JSON only."
            ),
        )
        if not response:
            return fallback

        parsed = self._parse_json_payload(response)
        suggestions = parsed.get("suggestions", []) if isinstance(parsed, dict) else []
        clean = [
            completion
            for item in suggestions
            if isinstance(item, str)
            for completion in [self._soften_language(self._normalize_completion(trimmed, item))]
            if completion
        ]
        ranked = self._rank_suggestions(clean, candidate, node_id)
        return ranked[:5] or fallback

    def _fallback(self, text: str) -> list[str]:
        return [
            suffix
            for suffix in [
                self._normalize_completion(text, " while keeping each product row in the result"),
                self._normalize_completion(text, " and show the daily store total next to each row"),
                self._normalize_completion(text, " without removing information people still need to see"),
            ]
            if suffix
        ][:5]

    def _prompt(self, session: Session, candidate: CandidatePipeline, node_id: str, text: str) -> str:
        node_explanation = candidate.node_explanations.get(node_id, "")
        node_assessment = next((item for item in candidate.node_assessments if item.node_id == node_id), None)
        step_preview = next((item for item in candidate.step_previews if item.step_id == node_id), None)
        node_rows = step_preview.preview_rows[:2] if step_preview else candidate.final_preview_rows[:2]
        node_warning_items = [
            item.model_dump()
            for item in candidate.warning_items
            if node_id in item.node_ids
        ]
        return (
            "Generate 3 to 5 completion suggestions for refining a pipeline node.\n"
            "Requirements:\n"
            "- Tailor the completions to the current user text and the actual table meaning.\n"
            "- Treat the user text as a fixed prefix that must remain unchanged.\n"
            "- Each suggestion must be only the continuation suffix that can be appended directly after the user text.\n"
            "- Do not repeat the prefix, rewrite it, or replace it with a new sentence.\n"
            "- Start each suffix with a space or punctuation when needed so appending stays natural.\n"
            "- Use plain everyday business language for non-technical users.\n"
            "- Avoid code, SQL, pandas, or database wording such as join, group by, schema, column, field, or window function.\n"
            "- Prefer phrases like keep each product row, show the store total, use the same date format, or bring in the matching store details.\n"
            "- Do not use generic placeholders.\n"
            "- Each completion should stay short and actionable.\n"
            "- Return JSON only in this shape: {\"suggestions\": [\"...\", \"...\"]}\n\n"
            f"User prefix: {text}\n"
            f"Node id: {node_id}\n"
            f"Node explanation: {node_explanation}\n"
            f"Node assessment: {json.dumps(node_assessment.model_dump() if node_assessment else {}, ensure_ascii=True, default=str)}\n"
            f"Node warnings: {json.dumps(node_warning_items, ensure_ascii=True, default=str)}\n"
            f"Node sample rows: {json.dumps(node_rows, ensure_ascii=True, default=str)}\n"
            f"Target schema: {json.dumps([field.model_dump() for field in session.target_schema], ensure_ascii=True, default=str)}\n"
            f"Existing pipeline summary: {candidate.summary}\n"
        )

    def _normalize_completion(self, prefix: str, suggestion: str) -> str:
        prefix = prefix.rstrip()
        suggestion = suggestion.strip()
        if not prefix or not suggestion:
            return ""

        if suggestion.lower().startswith(prefix.lower()):
            suggestion = suggestion[len(prefix) :].strip()

        if not suggestion:
            return ""

        if suggestion[0].isalnum():
            suggestion = f" {suggestion}"

        return suggestion

    def _soften_language(self, suggestion: str) -> str:
        softened = suggestion
        replacements = [
            (r"\bGROUP BY\b", "grouping the rows"),
            (r"\bgroup by\b", "grouping the rows"),
            (r"\bwindow function\b", "keeping the same rows and adding a total beside each one"),
            (r"\bjoin\b", "bring in the matching details"),
            (r"\bschema\b", "format"),
            (r"\bcolumns\b", "details"),
            (r"\bcolumn\b", "new detail"),
            (r"\bfields\b", "details"),
            (r"\bfield\b", "detail"),
        ]
        for pattern, replacement in replacements:
            softened = re.sub(pattern, replacement, softened)
        return softened

    def _rank_suggestions(self, suggestions: list[str], candidate: CandidatePipeline, node_id: str) -> list[str]:
        unique: list[str] = []
        seen: set[str] = set()
        for suggestion in suggestions:
            key = suggestion.strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            unique.append(suggestion)

        node_assessment = next((item for item in candidate.node_assessments if item.node_id == node_id), None)
        node_warning_text = " ".join(
            f"{item.title} {item.detail}".strip()
            for item in candidate.warning_items
            if node_id in item.node_ids
        )
        step_preview = next((item for item in candidate.step_previews if item.step_id == node_id), None)
        operator = step_preview.operator.value if step_preview else ""

        relevant_fields = [
            check.field_name.lower()
            for check in candidate.validation_summary.field_checks
            if check.status in {"Missing", "Type mismatch"}
        ]
        relevant_field_terms = {
            token
            for field_name in relevant_fields
            for token in re.split(r"[^a-z0-9]+", field_name)
            if len(token) >= 3
        }
        context_tokens = set(re.findall(r"[a-z0-9_]+", " ".join([node_assessment.reason if node_assessment else "", node_warning_text]).lower()))
        operator_hints = {
            "rename": {"name", "same", "match"},
            "date_formatting": {"date", "format"},
            "groupby": {"total", "daily", "each", "row"},
            "join": {"matching", "details", "store", "region"},
            "drop_columns": {"keep", "without"},
            "add_columns": {"add", "show", "next"},
        }.get(operator, set())

        scored: list[tuple[float, int, str]] = []
        for index, suggestion in enumerate(unique):
            lowered = suggestion.lower()
            score = 0.0
            score += sum(2.5 for field_name in relevant_fields if field_name in lowered)
            score += sum(2.0 for token in relevant_field_terms if token in lowered)
            score += sum(1.0 for token in context_tokens if token and token in lowered)
            score += sum(1.5 for token in operator_hints if token in lowered)
            if node_assessment and node_assessment.status == "issue":
                score += 1.0
            score += max(0.0, 0.2 - (len(lowered) * 0.001))
            scored.append((score, index, suggestion))

        scored.sort(key=lambda item: (-item[0], item[1]))
        return [item[2] for item in scored]

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


interactive_suggestion_service = InteractiveSuggestionService()
