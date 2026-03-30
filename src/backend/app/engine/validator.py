from __future__ import annotations

from collections import Counter
import re
from typing import Any

import pandas as pd

from app.engine.utils import infer_dtype, normalize_name
from app.models import DataType, TargetFieldSpec, ValidationFieldCheck, ValidationSummary


def _compatible(expected: DataType, actual: DataType | None) -> bool:
    if actual is None:
        return False
    if expected == actual:
        return True
    if expected == DataType.DATE and actual == DataType.DATETIME:
        return True
    if expected == DataType.FLOAT and actual == DataType.INTEGER:
        return True
    return False


class PipelineValidator:
    def validate(
        self,
        df: pd.DataFrame,
        target_schema: list[TargetFieldSpec],
        target_samples: list[dict[str, Any]],
        warnings: list[str] | None = None,
        executable: bool = True,
    ) -> ValidationSummary:
        warnings = list(warnings or [])
        actual_types = {column: infer_dtype(df[column]) for column in df.columns}
        target_names = [field.name for field in target_schema]
        normalized_actual = {normalize_name(col): col for col in df.columns}

        field_checks: list[ValidationFieldCheck] = []
        matches = 0
        required_ok = True
        compatible_count = 0
        for field in target_schema:
            matched_column = normalized_actual.get(normalize_name(field.name))
            actual_type = actual_types.get(matched_column) if matched_column else None
            status = "Matched" if matched_column else "Missing"
            if matched_column:
                matches += 1
                if _compatible(field.type, actual_type):
                    compatible_count += 1
                else:
                    status = "Type mismatch"
            elif field.required:
                required_ok = False
            field_checks.append(
                ValidationFieldCheck(
                    field_name=field.name,
                    expected_type=field.type,
                    actual_type=actual_type,
                    status=status,
                    required=field.required,
                )
            )

        column_match = list(df.columns) == target_names or set(df.columns) == set(target_names)
        type_compatibility = compatible_count / len(target_schema) if target_schema else 1.0
        example_similarity = self._example_similarity(df, target_schema, target_samples) if target_samples else None
        sample_consistency = example_similarity
        pipeline_correct = (
            executable
            and column_match
            and required_ok
            and type_compatibility == 1.0
            and (example_similarity is None or example_similarity >= 0.65)
        )

        if matches < len(target_schema):
            warnings.append("Some target fields are still missing from the final output.")
        if not column_match:
            warnings.append("The final column set is not a perfect match for the target schema.")
        if not required_ok:
            warnings.append("At least one required target field is missing.")
        if example_similarity is not None and example_similarity < 0.65:
            warnings.append("The generated output does not yet look similar to the existing target table rows.")

        return ValidationSummary(
            executable=executable,
            column_match=column_match,
            required_fields_met=required_ok,
            type_compatibility=type_compatibility,
            sample_consistency=sample_consistency,
            example_similarity=example_similarity,
            pipeline_correct=pipeline_correct,
            warnings=warnings,
            row_count=len(df.index),
            column_count=len(df.columns),
            field_checks=field_checks,
        )

    def _example_similarity(
        self,
        df: pd.DataFrame,
        target_schema: list[TargetFieldSpec],
        target_samples: list[dict[str, Any]],
    ) -> float:
        if df.empty or not target_samples:
            return 0.0

        output_lookup = {normalize_name(col): col for col in df.columns}
        target_df = pd.DataFrame(target_samples)
        if target_df.empty:
            return 0.0

        scores: list[float] = []
        for field in target_schema:
            output_column = output_lookup.get(normalize_name(field.name))
            target_column = field.name if field.name in target_df.columns else None
            if not output_column or not target_column:
                continue
            output_signatures = self._signature_distribution(df[output_column], field.type)
            target_signatures = self._signature_distribution(target_df[target_column], field.type)
            scores.append(self._distribution_similarity(output_signatures, target_signatures))

        if not scores:
            return 0.0
        return round(sum(scores) / len(scores), 4)

    def _signature_distribution(self, series: pd.Series, expected_type: DataType) -> dict[str, float]:
        non_null = [self._value_signature(value, expected_type) for value in series.head(120) if not pd.isna(value)]
        if not non_null:
            return {}
        counts = Counter(non_null)
        total = sum(counts.values())
        return {key: value / total for key, value in counts.items()}

    def _distribution_similarity(self, left: dict[str, float], right: dict[str, float]) -> float:
        if not left and not right:
            return 1.0
        if not left or not right:
            return 0.0
        keys = set(left) | set(right)
        return round(sum(min(left.get(key, 0.0), right.get(key, 0.0)) for key in keys), 4)

    def _value_signature(self, value: Any, expected_type: DataType) -> str:
        text = str(value).strip()
        if not text:
            return "empty"

        if expected_type == DataType.INTEGER:
            normalized = text.replace(",", "")
            sign = "neg" if normalized.startswith("-") else "pos"
            digits = normalized.lstrip("+-")
            return f"int:{sign}:{len(digits)}"

        if expected_type == DataType.FLOAT:
            normalized = text.replace(",", "")
            sign = "neg" if normalized.startswith("-") else "pos"
            parts = normalized.lstrip("+-").split(".", 1)
            integer_digits = len(parts[0])
            fraction_digits = len(parts[1]) if len(parts) > 1 else 0
            return f"float:{sign}:{integer_digits}:{fraction_digits}"

        if expected_type in {DataType.DATE, DataType.DATETIME}:
            pieces = [piece for piece in re.split(r"[\sT:/\-_.]+", text) if piece]
            separator = "-" if "-" in text else "/" if "/" in text else "." if "." in text else "none"
            has_time = "T" in text or ":" in text or (expected_type == DataType.DATETIME and len(pieces) > 3)
            ordering = "ymd" if pieces and len(pieces[0]) == 4 else "other"
            if len(pieces) >= 3 and len(pieces[-1]) == 4 and ordering == "other":
                ordering = "mdy_or_dmy"
            part_lengths = "-".join(str(len(piece)) for piece in pieces[:6])
            kind = "datetime" if has_time else "date"
            return f"{kind}:{separator}:{ordering}:{part_lengths}"

        if expected_type == DataType.BOOLEAN:
            lowered = text.lower()
            if lowered in {"true", "false"}:
                return "bool:word"
            if lowered in {"1", "0"}:
                return "bool:digit"
            return "bool:other"

        has_alpha = any(char.isalpha() for char in text)
        has_digit = any(char.isdigit() for char in text)
        casing = "upper" if text.isupper() else "lower" if text.islower() else "mixed"
        separator = (
            "underscore"
            if "_" in text
            else "hyphen"
            if "-" in text
            else "slash"
            if "/" in text
            else "space"
            if " " in text
            else "plain"
        )
        length_bucket = "short" if len(text) <= 4 else "medium" if len(text) <= 10 else "long"
        kind = "alnum" if has_alpha and has_digit else "alpha" if has_alpha else "digit" if has_digit else "symbol"
        return f"string:{kind}:{casing}:{separator}:{length_bucket}"
