from __future__ import annotations

import ast
from dataclasses import dataclass
from typing import Any

from app.engine.utils import source_alias
from app.models import PipelineSpec, PipelineStep, Session


OPERATOR_SIGNATURES = {
    "source_table": {"inputs": "[] or [source_name]", "params": {"source_table": "string"}},
    "rename": {"inputs": "[table_name]", "params": {"mapping": {"old_name": "new_name"}}},
    "join": {"inputs": "[left_table, right_table]", "params": {"left_on": ["col"], "right_on": ["col"], "how": "left|inner|right|outer"}},
    "union": {"inputs": "[table_a, table_b, ...]", "params": {}},
    "groupby": {
        "inputs": "[table_name]",
        "params": {"keys": ["col"], "aggregations": [{"source": "Sales", "func": "sum", "output": "Total_sales"}]},
    },
    "pivot": {"inputs": "[table_name]", "params": {"index": ["col"], "columns": "col", "values": "col", "aggfunc": "first"}},
    "unpivot": {"inputs": "[table_name]", "params": {"id_vars": ["col"], "value_vars": ["col"], "var_name": "variable", "value_name": "value"}},
    "date_formatting": {
        "inputs": "[table_name]",
        "params": {"column": "Date", "source_column": "Transaction_date", "input_format": "%Y/%m/%d", "output_format": "%Y-%m-%d"},
    },
    "column_arithmetic": {"inputs": "[table_name]", "params": {"output_column": "metric", "expression": "Sales * 1.0"}},
    "add_columns": {"inputs": "[table_name]", "params": {"mappings": [{"kind": "copy|constant|template|expression", "output": "field"}]}},
    "drop_columns": {"inputs": "[table_name]", "params": {"keep": ["col_a", "col_b"]}},
}


@dataclass(slots=True)
class AssignmentInfo:
    line_index: int
    mutation_allowed: bool


@dataclass(slots=True)
class StepDependencyTrace:
    step_id: str
    operator: str
    line_index: int
    inputs: list[str]
    output: str
    required_input_columns: dict[str, list[str]]
    output_columns: list[str]


class PipelineStructureValidator:
    def validate_new_code(self, session: Session, code_lines: list[str], *, origin: str) -> None:
        if not code_lines:
            raise ValueError(f"{origin} returned no code lines.")

        source_names = {table.name for table in session.source_tables}
        legacy_aliases = self._legacy_alias_map(session)
        assignments: dict[str, AssignmentInfo] = {}

        for line_index, line in enumerate(code_lines, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                tree = ast.parse(stripped)
            except SyntaxError as exc:
                raise ValueError(f"{origin} returned invalid Python at line {line_index}: {exc.msg}.") from exc
            if not tree.body:
                continue
            self._reject_legacy_alias_references(tree, legacy_aliases, origin)
            node = tree.body[0]
            if isinstance(node, ast.Assign):
                primary_target = node.targets[0]
                if isinstance(primary_target, ast.Name):
                    self._validate_named_assignment_expression(
                        target_name=primary_target.id,
                        value=node.value,
                        line_index=line_index,
                        origin=origin,
                    )
                    self._validate_named_assignment_target(
                        target_name=primary_target.id,
                        line_index=line_index,
                        source_names=source_names,
                        assignments=assignments,
                        origin=origin,
                    )
                    assignments[primary_target.id] = AssignmentInfo(
                        line_index=line_index,
                        mutation_allowed=self._is_copy_assignment(node.value),
                    )
                    continue
                if isinstance(primary_target, ast.Subscript):
                    self._validate_dataframe_mutation_target(
                        dataframe_name=self._extract_name(primary_target.value),
                        line_index=line_index,
                        source_names=source_names,
                        assignments=assignments,
                        origin=origin,
                    )
                    continue
                if isinstance(primary_target, ast.Attribute):
                    self._validate_dataframe_mutation_target(
                        dataframe_name=self._extract_name(primary_target.value),
                        line_index=line_index,
                        source_names=source_names,
                        assignments=assignments,
                        origin=origin,
                    )
                    continue
            if isinstance(node, ast.AugAssign):
                self._validate_dataframe_mutation_target(
                    dataframe_name=self._extract_name(node.target),
                    line_index=line_index,
                    source_names=source_names,
                    assignments=assignments,
                    origin=origin,
                )

    def validate_spec(self, session: Session, spec: PipelineSpec, *, origin: str) -> None:
        self._analyze_spec(session, spec, origin=origin)

    def trace_spec(self, session: Session, spec: PipelineSpec, *, origin: str) -> list[StepDependencyTrace]:
        return self._analyze_spec(session, spec, origin=origin)

    def _analyze_spec(self, session: Session, spec: PipelineSpec, *, origin: str) -> list[StepDependencyTrace]:
        steps = spec.steps
        if not steps:
            raise ValueError(f"{origin} produced an empty pipeline.")

        source_names = {table.name for table in session.source_tables}
        source_columns = {table.name: list(table.columns) for table in session.source_tables}
        legacy_aliases = self._legacy_alias_map(session)
        known_tables = set(source_names)
        known_columns = {name: list(columns) for name, columns in source_columns.items()}
        step_ids: dict[str, int] = {}
        outputs: dict[str, int] = {}
        traces: list[StepDependencyTrace] = []

        for step_index, step in enumerate(steps, start=1):
            line_index = self._code_line_index(step, step_index)
            if step.step_id in step_ids:
                raise ValueError(
                    f'{origin} produced duplicate step_id "{step.step_id}" (lines {step_ids[step.step_id]}, {line_index}).'
                )
            step_ids[step.step_id] = line_index

            if step.output in legacy_aliases:
                raise ValueError(
                    f'{origin} produced a legacy dataframe alias "{step.output}" as a step output. '
                    f'Use descriptive intermediate names and the real source table name "{legacy_aliases[step.output]}" where needed.'
                )
            if step.output in source_names:
                raise ValueError(
                    f'{origin} reuses source dataframe name "{step.output}" as a step output (line {line_index}). '
                    "Each step must write to a new dataframe variable."
                )
            if step.output in outputs:
                raise ValueError(
                    f'{origin} produced dataframe output "{step.output}" more than once '
                    f"(lines {outputs[step.output]}, {line_index}). Each step must write to a new dataframe variable."
                )
            outputs[step.output] = line_index

            self._validate_operator_shape(step, origin=origin)
            required_columns = self.required_input_columns(step)

            if step.operator.value == "source_table":
                source_table = step.params.get("source_table")
                if source_table in legacy_aliases:
                    raise ValueError(
                        f'{origin} used legacy source alias "{source_table}" at line {line_index}. '
                        f'Use the real source table name "{legacy_aliases[source_table]}" instead.'
                    )
                if source_table not in source_names:
                    raise ValueError(
                        f'{origin} references unavailable source table "{source_table}" at line {line_index}. '
                        f"Valid source tables: {self._valid_sources(session)}."
                    )
                known_columns[step.output] = list(source_columns.get(source_table, []))
            else:
                if not step.inputs:
                    raise ValueError(f'{origin} has step "{step.step_id}" with no input tables (line {line_index}).')
                missing = [name for name in step.inputs if name not in known_tables]
                if missing:
                    details = []
                    for name in missing:
                        if name in legacy_aliases:
                            details.append(f'"{name}" (legacy alias for "{legacy_aliases[name]}")')
                        else:
                            details.append(f'"{name}"')
                    available = ", ".join(sorted(known_tables))
                    raise ValueError(
                        f'{origin} has step "{step.step_id}" referencing unknown input table(s): {", ".join(details)} '
                        f"at line {line_index}. Available tables before this step: {available}."
                    )
                known_columns[step.output] = self._validate_and_infer_output_columns(
                    step=step,
                    known_columns=known_columns,
                    origin=origin,
                    line_index=line_index,
                )

            known_tables.add(step.output)
            traces.append(
                StepDependencyTrace(
                    step_id=step.step_id,
                    operator=step.operator.value,
                    line_index=line_index,
                    inputs=list(step.inputs),
                    output=step.output,
                    required_input_columns=required_columns,
                    output_columns=list(known_columns.get(step.output, [])),
                )
            )

        if spec.final_table not in outputs:
            produced = ", ".join(sorted(outputs))
            raise ValueError(
                f'{origin} references final_table "{spec.final_table}" that is not produced by the pipeline. '
                f"Produced tables: {produced}."
            )
        return traces

    def required_input_columns(self, step: PipelineStep) -> dict[str, list[str]]:
        requirements = {name: [] for name in step.inputs}
        operator = step.operator.value

        if operator == "rename":
            mapping = step.params.get("mapping")
            if isinstance(mapping, dict) and step.inputs:
                requirements[step.inputs[0]] = [str(column) for column in mapping.keys()]
            return requirements

        if operator == "date_formatting" and step.inputs:
            source_column = step.params.get("source_column")
            column = source_column if isinstance(source_column, str) and source_column else step.params.get("column")
            if isinstance(column, str) and column:
                requirements[step.inputs[0]] = [column]
            return requirements

        if operator == "groupby" and step.inputs:
            keys = [str(item) for item in step.params.get("keys", []) if isinstance(item, str)]
            aggregations = step.params.get("aggregations") if isinstance(step.params.get("aggregations"), list) else []
            sources = [
                str(item.get("source"))
                for item in aggregations
                if isinstance(item, dict) and isinstance(item.get("source"), str)
            ]
            requirements[step.inputs[0]] = self._unique_preserve_order(keys + sources)
            return requirements

        if operator == "join":
            shared = step.params.get("on")
            left = step.params.get("left_on") or shared or []
            right = step.params.get("right_on") or shared or []
            if len(step.inputs) >= 1:
                requirements[step.inputs[0]] = [str(item) for item in left if isinstance(item, str)]
            if len(step.inputs) >= 2:
                requirements[step.inputs[1]] = [str(item) for item in right if isinstance(item, str)]
            return requirements

        if operator == "unpivot" and step.inputs:
            id_vars = step.params.get("id_vars") or []
            value_vars = step.params.get("value_vars") or []
            requirements[step.inputs[0]] = [str(item) for item in id_vars + value_vars if isinstance(item, str)]
            return requirements

        if operator == "pivot" and step.inputs:
            fields: list[str] = []
            index = step.params.get("index")
            if isinstance(index, list):
                fields.extend([str(item) for item in index if isinstance(item, str)])
            elif isinstance(index, str):
                fields.append(index)
            for key in ("columns", "values"):
                value = step.params.get(key)
                if isinstance(value, str):
                    fields.append(value)
            requirements[step.inputs[0]] = self._unique_preserve_order(fields)
            return requirements

        if operator == "add_columns" and step.inputs:
            mappings = step.params.get("mappings") if isinstance(step.params.get("mappings"), list) else []
            sources = [
                str(item.get("source"))
                for item in mappings
                if isinstance(item, dict) and item.get("kind") == "copy" and isinstance(item.get("source"), str)
            ]
            requirements[step.inputs[0]] = self._unique_preserve_order(sources)
            return requirements

        return requirements

    def _validate_named_assignment_target(
        self,
        *,
        target_name: str,
        line_index: int,
        source_names: set[str],
        assignments: dict[str, AssignmentInfo],
        origin: str,
    ) -> None:
        if target_name in source_names:
            raise ValueError(
                f'{origin} reassigns source dataframe "{target_name}" at line {line_index}. '
                "Source dataframes are read-only; write into a new dataframe variable instead."
            )
        if target_name in assignments:
            raise ValueError(
                f'{origin} reuses dataframe output "{target_name}" across multiple steps '
                f"(lines {assignments[target_name].line_index}, {line_index}). Each step must write to a new dataframe variable."
            )

    def _validate_dataframe_mutation_target(
        self,
        *,
        dataframe_name: str | None,
        line_index: int,
        source_names: set[str],
        assignments: dict[str, AssignmentInfo],
        origin: str,
    ) -> None:
        if not dataframe_name:
            raise ValueError(f"{origin} contains an unsupported mutation target at line {line_index}.")
        if dataframe_name in source_names:
            raise ValueError(
                f'{origin} mutates source dataframe "{dataframe_name}" in place at line {line_index}. '
                'Copy it to a new dataframe variable first, for example `new_df = old_df.copy()`.'
            )
        assignment = assignments.get(dataframe_name)
        if assignment is None:
            raise ValueError(
                f'{origin} writes into unknown dataframe "{dataframe_name}" at line {line_index}. '
                "Create the dataframe with a prior assignment before mutating it."
            )
        if not assignment.mutation_allowed:
            raise ValueError(
                f'{origin} mutates dataframe "{dataframe_name}" at line {line_index} even though it was not created by a .copy() scaffold '
                f"(line {assignment.line_index}). Create a fresh dataframe with `{dataframe_name}_next = {dataframe_name}.copy()` before updating columns."
            )

    def _reject_legacy_alias_references(
        self,
        tree: ast.AST,
        legacy_aliases: dict[str, str],
        origin: str,
    ) -> None:
        for node in ast.walk(tree):
            if not isinstance(node, ast.Name):
                continue
            if node.id not in legacy_aliases:
                continue
            raise ValueError(
                f'{origin} uses legacy dataframe alias "{node.id}". '
                f'Use the real source table name "{legacy_aliases[node.id]}" instead.'
            )

    def _validate_named_assignment_expression(
        self,
        *,
        target_name: str,
        value: ast.AST,
        line_index: int,
        origin: str,
    ) -> None:
        if not isinstance(value, ast.Call):
            return
        if self._contains_groupby(value):
            self._validate_groupby_assignment_shape(
                target_name=target_name,
                value=value,
                line_index=line_index,
                origin=origin,
            )

    def _validate_operator_shape(self, step: PipelineStep, *, origin: str) -> None:
        params = step.params
        operator = step.operator.value

        if operator not in OPERATOR_SIGNATURES:
            raise ValueError(f'{origin} uses unsupported operator "{operator}" in step "{step.step_id}".')
        if operator == "source_table":
            if not isinstance(params.get("source_table"), str) or not params.get("source_table"):
                raise ValueError(f'{origin} step "{step.step_id}" must provide params.source_table.')
            return
        if operator == "rename":
            mapping = params.get("mapping")
            columns_override = params.get("columns_override")
            if not isinstance(mapping, dict) and not isinstance(columns_override, list):
                raise ValueError(f'{origin} step "{step.step_id}" must provide a rename mapping or columns_override.')
            return
        if operator == "join":
            if len(step.inputs) < 2:
                raise ValueError(f'{origin} step "{step.step_id}" must provide two join inputs.')
            how = params.get("how")
            if how is not None and not isinstance(how, str):
                raise ValueError(f'{origin} step "{step.step_id}" has an invalid join type.')
            return
        if operator == "groupby":
            if not isinstance(params.get("keys"), list):
                raise ValueError(f'{origin} step "{step.step_id}" must provide params.keys as a list.')
            aggregations = params.get("aggregations")
            if not isinstance(aggregations, list) or not aggregations:
                raise ValueError(f'{origin} step "{step.step_id}" must provide at least one aggregation.')
            return
        if operator == "date_formatting":
            if not isinstance(params.get("column"), str) or not params.get("column"):
                raise ValueError(f'{origin} step "{step.step_id}" must provide params.column.')
            source_column = params.get("source_column")
            if source_column is not None and (not isinstance(source_column, str) or not source_column):
                raise ValueError(f'{origin} step "{step.step_id}" has an invalid params.source_column.')
            return
        if operator == "column_arithmetic":
            if not isinstance(params.get("output_column"), str) or not isinstance(params.get("expression"), str):
                raise ValueError(f'{origin} step "{step.step_id}" must provide output_column and expression.')
            return
        if operator == "add_columns":
            if not isinstance(params.get("mappings"), list):
                raise ValueError(f'{origin} step "{step.step_id}" must provide params.mappings as a list.')
            return
        if operator == "drop_columns":
            keep = params.get("keep")
            drop = params.get("drop")
            if not isinstance(keep, list) and not isinstance(drop, list):
                raise ValueError(f'{origin} step "{step.step_id}" must provide params.keep or params.drop as a list.')
            return

    def _validate_and_infer_output_columns(
        self,
        *,
        step: PipelineStep,
        known_columns: dict[str, list[str]],
        origin: str,
        line_index: int,
    ) -> list[str]:
        input_columns = [list(known_columns.get(name, [])) for name in step.inputs]
        primary_columns = input_columns[0] if input_columns else []

        if step.operator.value == "rename":
            mapping = step.params.get("mapping") if isinstance(step.params.get("mapping"), dict) else {}
            columns_override = step.params.get("columns_override")
            if isinstance(columns_override, list) and columns_override:
                return [str(item) for item in columns_override]
            missing_sources = [source for source in mapping if source not in primary_columns]
            if missing_sources:
                available = ", ".join(primary_columns)
                raise ValueError(
                    f'{origin} step "{step.step_id}" renames missing column(s) {", ".join(map(repr, missing_sources))} at line {line_index}. '
                    f"Available columns: {available}."
                )
            renamed = [str(mapping.get(column, column)) for column in primary_columns]
            return self._unique_preserve_order(renamed)

        if step.operator.value == "date_formatting":
            column = step.params.get("column")
            source_column = step.params.get("source_column")
            input_column = source_column if isinstance(source_column, str) and source_column else column
            if input_column not in primary_columns:
                available = ", ".join(primary_columns)
                raise ValueError(
                    f'{origin} step "{step.step_id}" formats missing date column "{input_column}" at line {line_index}. '
                    f"Available columns: {available}."
                )
            if isinstance(column, str) and column and column not in primary_columns:
                return self._unique_preserve_order(primary_columns + [column])
            return list(primary_columns)

        if step.operator.value == "groupby":
            keys = [str(item) for item in step.params.get("keys", [])]
            missing_keys = [key for key in keys if key not in primary_columns]
            if missing_keys:
                available = ", ".join(primary_columns)
                raise ValueError(
                    f'{origin} step "{step.step_id}" groups by missing column(s) {", ".join(map(repr, missing_keys))} at line {line_index}. '
                    f"Available columns: {available}."
                )
            aggregations = step.params.get("aggregations") if isinstance(step.params.get("aggregations"), list) else []
            outputs: list[str] = []
            for aggregation in aggregations:
                record = aggregation if isinstance(aggregation, dict) else {}
                source = record.get("source")
                output = record.get("output")
                if source not in primary_columns:
                    available = ", ".join(primary_columns)
                    raise ValueError(
                        f'{origin} step "{step.step_id}" aggregates missing source column "{source}" at line {line_index}. '
                        f"Available columns: {available}."
                    )
                if not isinstance(output, str) or not output:
                    raise ValueError(
                        f'{origin} step "{step.step_id}" must provide a non-empty aggregation output name at line {line_index}.'
                    )
                outputs.append(output)
            return self._unique_preserve_order(keys + outputs)

        if step.operator.value == "join":
            left_columns = input_columns[0] if len(input_columns) > 0 else []
            right_columns = input_columns[1] if len(input_columns) > 1 else []
            left_on = step.params.get("left_on") or step.params.get("on") or []
            right_on = step.params.get("right_on") or step.params.get("on") or []
            for key in [str(item) for item in left_on]:
                if key not in left_columns:
                    available = ", ".join(left_columns)
                    raise ValueError(
                        f'{origin} step "{step.step_id}" joins on missing left key "{key}" at line {line_index}. '
                        f"Available left columns: {available}."
                    )
            for key in [str(item) for item in right_on]:
                if key not in right_columns:
                    available = ", ".join(right_columns)
                    raise ValueError(
                        f'{origin} step "{step.step_id}" joins on missing right key "{key}" at line {line_index}. '
                        f"Available right columns: {available}."
                    )
            return self._unique_preserve_order(left_columns + right_columns)

        if step.operator.value in {"union", "pivot", "unpivot"}:
            return list(primary_columns)

        if step.operator.value == "column_arithmetic":
            output_column = step.params.get("output_column")
            if not isinstance(output_column, str) or not output_column:
                raise ValueError(f'{origin} step "{step.step_id}" must provide output_column at line {line_index}.')
            return self._unique_preserve_order(primary_columns + [output_column])

        if step.operator.value == "add_columns":
            mappings = step.params.get("mappings") if isinstance(step.params.get("mappings"), list) else []
            next_columns = list(primary_columns)
            for mapping in mappings:
                record = mapping if isinstance(mapping, dict) else {}
                output = record.get("output")
                kind = record.get("kind", "constant")
                if kind == "copy" and record.get("source") not in primary_columns:
                    available = ", ".join(primary_columns)
                    raise ValueError(
                        f'{origin} step "{step.step_id}" copies missing source column "{record.get("source")}" at line {line_index}. '
                        f"Available columns: {available}."
                    )
                if isinstance(output, str) and output:
                    next_columns.append(output)
            return self._unique_preserve_order(next_columns)

        if step.operator.value == "drop_columns":
            keep = step.params.get("keep")
            if isinstance(keep, list):
                return [str(column) for column in keep if column in primary_columns]
            drop = step.params.get("drop")
            if isinstance(drop, list):
                drop_set = {str(column) for column in drop}
                return [column for column in primary_columns if column not in drop_set]
            return list(primary_columns)

        return list(primary_columns)

    def _legacy_alias_map(self, session: Session) -> dict[str, str]:
        alias_map: dict[str, str] = {}
        for index, table in enumerate(session.source_tables):
            alias = source_alias(index)
            if alias == table.name:
                continue
            alias_map[alias] = table.name
        return alias_map

    def _valid_sources(self, session: Session) -> str:
        return ", ".join(table.name for table in session.source_tables) or "(none)"

    def _code_line_index(self, step: PipelineStep, fallback: int) -> int:
        line_index = step.params.get("_code_line_index")
        return line_index if isinstance(line_index, int) and line_index >= 1 else fallback

    def _extract_name(self, node: ast.AST | None) -> str | None:
        if node is None:
            return None
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return self._extract_name(node.value)
        if isinstance(node, ast.Subscript):
            return self._extract_name(node.value)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            return self._extract_name(node.func.value)
        return None

    def _is_copy_assignment(self, node: ast.AST) -> bool:
        return isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "copy"

    def _contains_groupby(self, node: ast.Call) -> bool:
        current: ast.AST = node
        while isinstance(current, ast.Call):
            func = current.func
            if isinstance(func, ast.Attribute) and func.attr == "groupby":
                return True
            if isinstance(func, ast.Attribute):
                current = func.value
            else:
                break
        return False

    def _validate_groupby_assignment_shape(
        self,
        *,
        target_name: str,
        value: ast.Call,
        line_index: int,
        origin: str,
    ) -> None:
        agg_call = self._find_chained_call(value, "agg")
        if agg_call is None:
            raise ValueError(
                f'{origin} uses unsupported groupby syntax for dataframe "{target_name}" at line {line_index}. '
                "Use named aggregation exactly like `.groupby([...], dropna=False).agg(**{'OutputColumn': ('SourceColumn', 'sum')}).reset_index()`."
            )
        if agg_call.args:
            raise ValueError(
                f'{origin} uses positional groupby aggregation arguments at line {line_index}. '
                "Use named aggregation tuples instead of dict or list arguments."
            )
        if not agg_call.keywords:
            raise ValueError(
                f'{origin} uses empty groupby aggregation syntax at line {line_index}. '
                "Provide at least one named aggregation tuple."
            )
        for keyword in agg_call.keywords:
            if keyword.arg is None:
                mapping = self._literal(keyword.value)
                if not isinstance(mapping, dict) or not mapping:
                    raise ValueError(
                        f'{origin} uses unsupported groupby aggregation syntax at line {line_index}. '
                        "Use named aggregation tuples such as `**{'Total_store_sales': ('Daily_sales', 'sum')}`."
                    )
                for output, spec in mapping.items():
                    self._validate_groupby_tuple_spec(output=output, spec=spec, line_index=line_index, origin=origin)
            else:
                self._validate_groupby_tuple_spec(
                    output=keyword.arg,
                    spec=self._literal(keyword.value),
                    line_index=line_index,
                    origin=origin,
                )

    def _validate_groupby_tuple_spec(self, *, output: Any, spec: Any, line_index: int, origin: str) -> None:
        if not isinstance(output, str) or not output:
            raise ValueError(
                f'{origin} uses a non-string groupby output name at line {line_index}. '
                "Each named aggregation must use a string output column."
            )
        if not isinstance(spec, (tuple, list)) or len(spec) < 2:
            raise ValueError(
                f'{origin} uses unsupported groupby aggregation for "{output}" at line {line_index}. '
                "Use tuples like ('SourceColumn', 'sum')."
            )
        source, func = spec[0], spec[1]
        if not isinstance(source, str) or not isinstance(func, str) or not source or not func:
            raise ValueError(
                f'{origin} uses unsupported groupby aggregation for "{output}" at line {line_index}. '
                "Use tuples like ('SourceColumn', 'sum')."
            )

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

    def _literal(self, node: ast.AST | None) -> Any:
        if node is None:
            return None
        try:
            return ast.literal_eval(node)
        except Exception:
            return None

    def _unique_preserve_order(self, items: list[str]) -> list[str]:
        ordered: list[str] = []
        for item in items:
            if item not in ordered:
                ordered.append(item)
        return ordered


structure_validator = PipelineStructureValidator()
