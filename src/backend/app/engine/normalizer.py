from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from typing import Any

from app.models import OperatorType, PipelineSpec, PipelineStep


@dataclass(slots=True)
class ParsedStep:
    operator: OperatorType
    title: str
    inputs: list[str]
    output: str
    params: dict[str, Any]
    notes: str
    line_index: int


class CodeToPipelineNormalizer:
    def normalize(self, code_lines: list[str]) -> PipelineSpec:
        self._known_outputs: set[str] = set()
        self._pending_copy_aliases: dict[str, str] = {}
        parsed_steps: list[ParsedStep] = []
        for index, line in enumerate(code_lines, start=1):
            parsed = self._parse_line(line, index)
            if parsed is not None:
                if self._merge_with_previous(parsed_steps, parsed):
                    continue
                parsed_steps.append(parsed)
                self._known_outputs.add(parsed.output)
                self._pending_copy_aliases.pop(parsed.output, None)

        if not parsed_steps:
            raise ValueError("No executable pipeline steps were extracted from the generated code.")

        final_output = parsed_steps[-1].output
        steps = [
            PipelineStep(
                step_id=f"step_{idx}",
                operator=step.operator,
                title=step.title,
                inputs=step.inputs,
                output=step.output,
                params=step.params,
                notes=step.notes,
            )
            for idx, step in enumerate(parsed_steps, start=1)
        ]
        return PipelineSpec(
            steps=steps,
            final_table=final_output,
            warnings=[],
            rationale="Generated from LLM-authored pandas code and normalized into the structured pipeline format.",
            raw_code_lines=code_lines,
        )

    def _merge_with_previous(self, parsed_steps: list[ParsedStep], parsed: ParsedStep) -> bool:
        if not parsed_steps:
            return False
        previous = parsed_steps[-1]
        if previous.operator != OperatorType.ADD_COLUMNS or parsed.operator != OperatorType.ADD_COLUMNS:
            return False
        if previous.output != parsed.output:
            return False

        previous_mappings = previous.params.get("mappings")
        next_mappings = parsed.params.get("mappings")
        if not isinstance(previous_mappings, list) or not isinstance(next_mappings, list):
            return False

        if not previous.inputs and parsed.inputs:
            previous.inputs = list(parsed.inputs)
        previous_mappings.extend(next_mappings)
        previous.params["mappings"] = previous_mappings
        previous.params["_code_line_index"] = min(
            self._safe_line_index(previous.params.get("_code_line_index"), previous.line_index),
            parsed.line_index,
        )
        previous.title = "Add or update columns"
        previous.notes = "New or updated fields are derived from existing columns."
        return True

    def _safe_line_index(self, value: Any, fallback: int) -> int:
        return value if isinstance(value, int) and value >= 1 else fallback

    def _parse_line(self, line: str, line_index: int) -> ParsedStep | None:
        stripped = line.strip()
        if not stripped:
            return None
        tree = ast.parse(stripped)
        if not tree.body:
            return None
        node = tree.body[0]

        if isinstance(node, ast.Assign):
            primary_target = node.targets[0]
            if isinstance(primary_target, ast.Name):
                output = primary_target.id
                parsed = self._parse_assignment_value(output, node.value, stripped, line_index)
                if parsed is not None:
                    return parsed
            if isinstance(primary_target, ast.Subscript):
                return self._parse_column_assignment(primary_target, node.value, stripped, line_index)
            if isinstance(primary_target, ast.Attribute):
                return self._parse_attribute_assignment(primary_target, node.value, stripped, line_index)
        return None

    def _parse_assignment_value(
        self,
        output: str,
        value: ast.AST,
        line: str,
        line_index: int,
    ) -> ParsedStep | None:
        if isinstance(value, ast.Subscript):
            keep = self._extract_keep_columns(value.slice)
            source_name = self._extract_name(value.value)
            if keep is not None:
                return ParsedStep(
                    operator=OperatorType.DROP_COLUMNS,
                    title="Keep selected columns",
                    inputs=[source_name] if source_name else [],
                    output=output,
                    params={"keep": keep, "_code_line_index": line_index},
                    notes="Only target-facing columns are kept for the next step.",
                    line_index=line_index,
                )
        if isinstance(value, ast.Call):
            call_name = self._call_name(value)
            base_name = self._call_base_name(value)
            if isinstance(value.func, ast.Attribute) and value.func.attr == "copy" and isinstance(value.func.value, ast.Subscript):
                keep = self._extract_keep_columns(value.func.value.slice)
                source_name = self._resolve_input_name(self._extract_name(value.func.value.value))
                if keep is not None:
                    return ParsedStep(
                        operator=OperatorType.DROP_COLUMNS,
                        title="Keep selected columns",
                        inputs=[source_name] if source_name else [],
                        output=output,
                        params={"keep": keep, "_code_line_index": line_index},
                        notes="Only target-facing columns are kept for the next step.",
                        line_index=line_index,
                    )
            if call_name.endswith(".copy") and base_name:
                resolved_base = self._resolve_input_name(base_name)
                if base_name in self._known_outputs or resolved_base != base_name:
                    self._pending_copy_aliases[output] = resolved_base
                    self._known_outputs.add(output)
                    return None
                return ParsedStep(
                    operator=OperatorType.SOURCE_TABLE,
                    title=f"Load {resolved_base}",
                    inputs=[resolved_base],
                    output=output,
                    params={"source_table": resolved_base, "_code_line_index": line_index},
                    notes="The pipeline starts from this source dataframe.",
                    line_index=line_index,
                )
            if call_name.endswith(".rename"):
                mapping = self._keyword_literal(value, "columns", default={})
                source_name = self._resolve_input_name(self._call_base_name(value))
                return ParsedStep(
                    operator=OperatorType.RENAME,
                    title="Rename fields",
                    inputs=[source_name] if source_name else [],
                    output=output,
                    params={"mapping": mapping, "_code_line_index": line_index},
                    notes="Field names are standardized to match the target schema.",
                    line_index=line_index,
                )
            if ".merge" in call_name or call_name == "pd.merge":
                left_name, right_name = self._extract_merge_inputs(value)
                return ParsedStep(
                    operator=OperatorType.JOIN,
                    title="Join intermediate tables",
                    inputs=[item for item in [left_name, right_name] if item],
                    output=output,
                    params={
                        "left_on": self._keyword_literal(value, "left_on", self._keyword_literal(value, "on")),
                        "right_on": self._keyword_literal(value, "right_on", self._keyword_literal(value, "on")),
                        "how": self._keyword_literal(value, "how", "inner"),
                        "_code_line_index": line_index,
                    },
                    notes="Intermediate results are joined to bring required fields together.",
                    line_index=line_index,
                )
            if ".groupby" in call_name:
                return ParsedStep(
                    operator=OperatorType.GROUPBY,
                    title="Aggregate rows",
                    inputs=self._extract_groupby_inputs(value),
                    output=output,
                    params={
                        "keys": self._extract_groupby_keys(value),
                        "aggregations": self._extract_groupby_aggregations(value),
                        "_code_line_index": line_index,
                    },
                    notes="Rows are grouped to compute an aggregate metric at the target grain.",
                    line_index=line_index,
                )
            unparsed = ast.unparse(value)
            if ".groupby(" in unparsed:
                return ParsedStep(
                    operator=OperatorType.GROUPBY,
                    title="Aggregate rows",
                    inputs=self._extract_groupby_inputs(value),
                    output=output,
                    params={
                        "keys": self._extract_groupby_keys(value),
                        "aggregations": self._extract_groupby_aggregations(value),
                        "_code_line_index": line_index,
                    },
                    notes="Rows are grouped to compute an aggregate metric at the target grain.",
                    line_index=line_index,
                )
            if call_name == "pd.concat":
                return ParsedStep(
                    operator=OperatorType.UNION,
                    title="Append rows",
                    inputs=self._resolve_inputs(self._extract_concat_inputs(value)),
                    output=output,
                    params={"_code_line_index": line_index},
                    notes="Compatible rows are stacked into one table.",
                    line_index=line_index,
                )
            if call_name.endswith(".melt"):
                source_name = self._resolve_input_name(self._call_base_name(value))
                return ParsedStep(
                    operator=OperatorType.UNPIVOT,
                    title="Unpivot columns",
                    inputs=[source_name] if source_name else [],
                    output=output,
                    params={
                        "id_vars": self._keyword_literal(value, "id_vars"),
                        "value_vars": self._keyword_literal(value, "value_vars"),
                        "var_name": self._keyword_literal(value, "var_name", "variable"),
                        "value_name": self._keyword_literal(value, "value_name", "value"),
                        "_code_line_index": line_index,
                    },
                    notes="Wide columns are reshaped into row-wise records.",
                    line_index=line_index,
                )
            if call_name.endswith(".pivot_table") or call_name.endswith(".pivot"):
                source_name = self._resolve_input_name(self._call_base_name(value))
                return ParsedStep(
                    operator=OperatorType.PIVOT,
                    title="Pivot rows into columns",
                    inputs=[source_name] if source_name else [],
                    output=output,
                    params={
                        "index": self._keyword_literal(value, "index"),
                        "columns": self._keyword_literal(value, "columns"),
                        "values": self._keyword_literal(value, "values"),
                        "aggfunc": self._keyword_literal(value, "aggfunc", "first"),
                        "_code_line_index": line_index,
                    },
                    notes="Row-wise values are pivoted into new columns.",
                    line_index=line_index,
                )
            if isinstance(value.func, ast.Subscript):
                keep = self._extract_keep_columns(value.func.slice)
                source_name = self._resolve_input_name(self._extract_name(value.func.value))
                if keep is not None:
                    return ParsedStep(
                        operator=OperatorType.DROP_COLUMNS,
                        title="Keep selected columns",
                        inputs=[source_name] if source_name else [],
                        output=output,
                        params={"keep": keep, "_code_line_index": line_index},
                        notes="Only target-facing columns are kept for the next step.",
                        line_index=line_index,
                    )

        return ParsedStep(
            operator=OperatorType.COLUMN_ARITHMETIC,
            title="Transform data",
            inputs=self._resolve_inputs(self._collect_name_inputs(value)),
            output=output,
            params={"_code_line_index": line_index},
            notes="A derived field or intermediate transformation is computed.",
            line_index=line_index,
        )

    def _parse_column_assignment(
        self,
        target: ast.Subscript,
        value: ast.AST,
        line: str,
        line_index: int,
    ) -> ParsedStep | None:
        dataframe_name = self._extract_name(target.value)
        column_name = self._literal(target.slice)
        if dataframe_name is None or not isinstance(column_name, str):
            return None
        input_name = self._resolve_input_name(dataframe_name)
        value_text = ast.unparse(value)
        if "to_datetime" in value_text or "strftime" in value_text:
            input_format = self._extract_datetime_input_format(value)
            output_format = self._extract_strftime_output_format(value)
            source_column = self._extract_datetime_source_column(value, input_name)
            params = {
                "column": column_name,
                "input_format": input_format,
                "output_format": output_format,
                "_code_line_index": line_index,
            }
            if isinstance(source_column, str) and source_column and source_column != column_name:
                params["source_column"] = source_column
            return ParsedStep(
                operator=OperatorType.DATE_FORMATTING,
                title="Normalize date values",
                inputs=[input_name],
                output=dataframe_name,
                params=params,
                notes="Date-like values are reformatted for the target schema.",
                line_index=line_index,
            )
        if isinstance(value, ast.Call) and self._call_name(value).endswith(".eval"):
            expression = self._literal(self._argument_or_keyword(value, 0, "expr"))
            if isinstance(expression, str):
                return ParsedStep(
                    operator=OperatorType.COLUMN_ARITHMETIC,
                    title=f"Compute {column_name}",
                    inputs=[input_name],
                    output=dataframe_name,
                    params={
                        "output_column": column_name,
                        "expression": expression,
                        "_code_line_index": line_index,
                    },
                    notes="A derived field is computed from the current row values.",
                    line_index=line_index,
                )
        mapping: dict[str, Any]
        if isinstance(value, ast.Subscript):
            source_dataframe = self._resolve_input_name(self._extract_name(value.value))
            source_column = self._literal(value.slice)
            if source_dataframe == input_name and isinstance(source_column, str):
                mapping = {"kind": "copy", "output": column_name, "source": source_column}
            else:
                mapping = {"kind": "expression", "output": column_name, "expression": value_text}
        else:
            if isinstance(value, ast.Constant) and (
                isinstance(value.value, (str, int, float, bool)) or value.value is None
            ):
                mapping = {"kind": "constant", "output": column_name, "value": value.value}
            else:
                mapping = {"kind": "expression", "output": column_name, "expression": value_text}
        return ParsedStep(
            operator=OperatorType.ADD_COLUMNS,
            title=f"Add or update {column_name}",
            inputs=[input_name],
            output=dataframe_name,
            params={"mappings": [mapping], "_code_line_index": line_index},
            notes="A new or updated field is derived from existing columns.",
            line_index=line_index,
        )

    def _parse_attribute_assignment(
        self,
        target: ast.Attribute,
        value: ast.AST,
        line: str,
        line_index: int,
    ) -> ParsedStep | None:
        if target.attr != "columns":
            return None
        dataframe_name = self._extract_name(target.value)
        if dataframe_name is None:
            return None
        columns = self._literal(value)
        if not isinstance(columns, list):
            return None
        return ParsedStep(
            operator=OperatorType.RENAME,
            title="Rename columns",
            inputs=[self._resolve_input_name(dataframe_name)],
            output=dataframe_name,
            params={"columns_override": columns, "_code_line_index": line_index},
            notes="The dataframe columns are renamed to align with the target schema.",
            line_index=line_index,
        )

    def _call_name(self, node: ast.Call) -> str:
        if isinstance(node.func, ast.Attribute):
            base = self._extract_name(node.func.value)
            return f"{base}.{node.func.attr}" if base else node.func.attr
        if isinstance(node.func, ast.Name):
            return node.func.id
        return ast.unparse(node.func)

    def _call_base_name(self, node: ast.Call) -> str | None:
        if isinstance(node.func, ast.Attribute):
            return self._extract_name(node.func.value)
        return None

    def _extract_name(self, node: ast.AST | None) -> str | None:
        if node is None:
            return None
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return self._extract_name(node.value)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            return self._extract_name(node.func.value)
        if isinstance(node, ast.Subscript):
            return self._extract_name(node.value)
        return None

    def _literal(self, node: ast.AST | None) -> Any:
        if node is None:
            return None
        try:
            return ast.literal_eval(node)
        except Exception:
            return ast.unparse(node)

    def _keyword_literal(self, call: ast.Call, keyword: str, default: Any = None) -> Any:
        for item in call.keywords:
            if item.arg == keyword:
                return self._literal(item.value)
        return default

    def _argument_or_keyword(self, call: ast.Call, index: int, keyword: str) -> ast.AST | None:
        for item in call.keywords:
            if item.arg == keyword:
                return item.value
        if index < len(call.args):
            return call.args[index]
        return None

    def _collect_name_inputs(self, node: ast.AST) -> list[str]:
        names: list[str] = []
        for item in ast.walk(node):
            if isinstance(item, ast.Name):
                if item.id not in names and item.id != "pd":
                    names.append(item.id)
        return names[:3]

    def _extract_concat_inputs(self, call: ast.Call) -> list[str]:
        target = self._argument_or_keyword(call, 0, "objs")
        if isinstance(target, ast.List):
            return [self._extract_name(item) for item in target.elts if self._extract_name(item)]
        return []

    def _extract_merge_inputs(self, call: ast.Call) -> tuple[str | None, str | None]:
        if isinstance(call.func, ast.Attribute) and call.func.attr == "merge":
            base_name = self._extract_name(call.func.value)
            if base_name == "pd":
                left_node = self._argument_or_keyword(call, 0, "left")
                right_node = self._argument_or_keyword(call, 1, "right")
            else:
                left_node = call.func.value
                right_node = self._argument_or_keyword(call, 0, "right")
            left_name = self._resolve_input_name(self._extract_name(left_node))
            right_name = self._resolve_input_name(self._extract_name(right_node))
            return left_name, right_name

        left_node = self._argument_or_keyword(call, 0, "left")
        right_node = self._argument_or_keyword(call, 1, "right")
        left_name = self._resolve_input_name(self._extract_name(left_node))
        right_name = self._resolve_input_name(self._extract_name(right_node))
        return left_name, right_name

    def _extract_groupby_inputs(self, call: ast.Call) -> list[str]:
        names = self._resolve_inputs(self._collect_name_inputs(call))
        return names[:1]

    def _extract_groupby_keys(self, call: ast.Call) -> list[str]:
        text = ast.unparse(call)
        regex_match = re.search(r"\.groupby\((\[.*?\]|'[^']+'|\"[^\"]+\")", text)
        if regex_match:
            try:
                literal = ast.literal_eval(regex_match.group(1))
                if isinstance(literal, list):
                    return [str(item) for item in literal]
                if isinstance(literal, str):
                    return [literal]
            except Exception:
                pass
        current: ast.Call | ast.AST = call
        while isinstance(current, ast.Call):
            func = current.func
            if isinstance(func, ast.Attribute) and func.attr == "groupby":
                if func.value:
                    keys = self._argument_or_keyword(current, 0, "by")
                    literal = self._literal(keys)
                    if isinstance(literal, list):
                        return [str(item) for item in literal]
                    if isinstance(literal, str):
                        return [literal]
                break
            if isinstance(func, ast.Attribute):
                current = func.value
            else:
                break
        return []

    def _extract_groupby_aggregations(self, call: ast.Call) -> list[dict[str, Any]]:
        text = ast.unparse(call)
        sum_match = re.search(r"\[['\"]([^'\"]+)['\"]\]\.sum\(", text)
        if sum_match:
            source = sum_match.group(1)
            return [{"source": source, "func": "sum", "output": source}]
        agg_call = self._find_chained_call(call, "agg")
        if agg_call is not None:
            aggregations: list[dict[str, Any]] = []
            for keyword in agg_call.keywords:
                if keyword.arg is None:
                    mapping = self._literal(keyword.value)
                    if isinstance(mapping, dict):
                        aggregations.extend(self._coerce_groupby_mapping(mapping))
                else:
                    aggregations.extend(self._coerce_groupby_mapping({keyword.arg: self._literal(keyword.value)}))
            if aggregations:
                return aggregations
        return []

    def _find_chained_call(self, call: ast.Call, attr_name: str) -> ast.Call | None:
        current: ast.AST = call
        while isinstance(current, ast.Call):
            func = current.func
            if isinstance(func, ast.Attribute) and func.attr == attr_name:
                return current
            if isinstance(func, ast.Attribute):
                current = func.value
            else:
                break
        return None

    def _coerce_groupby_mapping(self, mapping: dict[Any, Any]) -> list[dict[str, Any]]:
        aggregations: list[dict[str, Any]] = []
        for output, spec in mapping.items():
            if not isinstance(output, str):
                continue
            if isinstance(spec, tuple) and len(spec) >= 2:
                source, func = spec[0], spec[1]
                if isinstance(source, str) and isinstance(func, str):
                    aggregations.append({"source": source, "func": func, "output": output})
            elif isinstance(spec, str):
                aggregations.append({"source": output, "func": spec, "output": output})
        return aggregations

    def _extract_keep_columns(self, node: ast.AST | None) -> list[str] | None:
        literal = self._literal(node)
        if isinstance(literal, list) and all(isinstance(item, str) for item in literal):
            return [str(item) for item in literal]
        if isinstance(node, ast.ListComp) and node.generators:
            source = self._literal(node.generators[0].iter)
            if isinstance(source, list) and all(isinstance(item, str) for item in source):
                return [str(item) for item in source]
        return None

    def _extract_datetime_input_format(self, node: ast.AST) -> Any:
        for item in ast.walk(node):
            if not isinstance(item, ast.Call):
                continue
            func = item.func
            if isinstance(func, ast.Attribute) and func.attr == "to_datetime":
                return self._keyword_literal(item, "format")
            if isinstance(func, ast.Name) and func.id == "to_datetime":
                return self._keyword_literal(item, "format")
        return None

    def _extract_datetime_source_column(self, node: ast.AST, dataframe_name: str | None) -> str | None:
        for item in ast.walk(node):
            if not isinstance(item, ast.Call):
                continue
            func = item.func
            is_to_datetime = (
                isinstance(func, ast.Attribute) and func.attr == "to_datetime"
            ) or (isinstance(func, ast.Name) and func.id == "to_datetime")
            if not is_to_datetime:
                continue
            source_node = self._argument_or_keyword(item, 0, "arg")
            if not isinstance(source_node, ast.Subscript):
                continue
            source_dataframe = self._resolve_input_name(self._extract_name(source_node.value))
            source_column = self._literal(source_node.slice)
            if source_dataframe == dataframe_name and isinstance(source_column, str) and source_column:
                return source_column
        return None

    def _extract_strftime_output_format(self, node: ast.AST) -> Any:
        if not isinstance(node, ast.Call):
            return None
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr == "strftime":
            return self._literal(self._argument_or_keyword(node, 0, "format"))
        return None

    def _resolve_input_name(self, name: str | None) -> str | None:
        if name is None:
            return None
        current = name
        seen: set[str] = set()
        while current in self._pending_copy_aliases and current not in seen:
            seen.add(current)
            current = self._pending_copy_aliases[current]
        return current

    def _resolve_inputs(self, names: list[str]) -> list[str]:
        resolved: list[str] = []
        for name in names:
            resolved_name = self._resolve_input_name(name)
            if resolved_name and resolved_name not in resolved:
                resolved.append(resolved_name)
        return resolved
