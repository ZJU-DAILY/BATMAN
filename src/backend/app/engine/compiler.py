from __future__ import annotations

import ast
import copy

import pandas as pd

from app.models import OperatorType, PipelineSpec, PipelineStep


class PipelineCompiler:
    def execute_raw_code(self, code_lines: list[str], tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
        env = {"pd": pd, **{name: frame.copy() for name, frame in tables.items()}}
        for line in code_lines:
            if line.strip():
                exec(line, env)
        last_var = self.extract_last_variable("\n".join(code_lines))
        if not last_var or last_var not in env:
            raise ValueError("The generated code did not produce a final dataframe variable.")
        result = env[last_var]
        if not isinstance(result, pd.DataFrame):
            raise TypeError("The generated code did not end in a dataframe result.")
        return result

    def apply_step(self, step: PipelineStep, tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
        operator = step.operator
        if operator == OperatorType.SOURCE_TABLE:
            result = tables[step.params["source_table"]].copy()
        elif operator == OperatorType.RENAME:
            result = tables[step.inputs[0]].copy().rename(columns=step.params.get("mapping", {}))
        elif operator == OperatorType.DATE_FORMATTING:
            result = tables[step.inputs[0]].copy()
            column = step.params["column"]
            source_column = step.params.get("source_column", column)
            input_format = step.params.get("input_format")
            output_format = step.params.get("output_format", "%Y-%m-%d")
            parsed = pd.to_datetime(result[source_column], format=input_format, errors="coerce")
            result[column] = parsed.dt.strftime(output_format)
        elif operator == OperatorType.GROUPBY:
            source = tables[step.inputs[0]].copy()
            agg_spec = {
                item["output"]: (item["source"], item.get("func", "sum"))
                for item in step.params["aggregations"]
            }
            result = source.groupby(step.params["keys"], dropna=False).agg(**agg_spec).reset_index()
        elif operator == OperatorType.JOIN:
            result = tables[step.inputs[0]].copy().merge(
                tables[step.inputs[1]].copy(),
                how=step.params.get("how", "left"),
                left_on=step.params.get("left_on"),
                right_on=step.params.get("right_on"),
                suffixes=("_left", "_right"),
            )
        elif operator == OperatorType.UNION:
            result = pd.concat([tables[name].copy() for name in step.inputs], ignore_index=True)
        elif operator == OperatorType.UNPIVOT:
            result = tables[step.inputs[0]].copy().melt(
                id_vars=step.params.get("id_vars"),
                value_vars=step.params.get("value_vars"),
                var_name=step.params.get("var_name", "variable"),
                value_name=step.params.get("value_name", "value"),
            )
        elif operator == OperatorType.PIVOT:
            result = (
                tables[step.inputs[0]]
                .copy()
                .pivot_table(
                    index=step.params.get("index"),
                    columns=step.params.get("columns"),
                    values=step.params.get("values"),
                    aggfunc=step.params.get("aggfunc", "first"),
                )
                .reset_index()
            )
            if result.columns.nlevels > 1:
                result.columns = [str(col[-1] if isinstance(col, tuple) else col) for col in result.columns]
        elif operator == OperatorType.COLUMN_ARITHMETIC:
            result = tables[step.inputs[0]].copy()
            result[step.params["output_column"]] = result.eval(step.params["expression"])
        elif operator == OperatorType.ADD_COLUMNS:
            result = tables[step.inputs[0]].copy()
            for mapping in step.params.get("mappings", []):
                kind = mapping.get("kind", "constant")
                output = mapping["output"]
                if kind == "constant":
                    result[output] = mapping.get("value")
                elif kind == "copy":
                    result[output] = result[mapping["source"]]
                elif kind == "template":
                    result[output] = result.apply(
                        lambda row: mapping["template"].format(**{col: row.get(col) for col in result.columns}),
                        axis=1,
                    )
        elif operator == OperatorType.DROP_COLUMNS:
            source = tables[step.inputs[0]].copy()
            if "keep" in step.params:
                keep = [col for col in step.params["keep"] if col in source.columns]
                result = source[keep].copy()
            else:
                drop = [col for col in step.params.get("drop", []) if col in source.columns]
                result = source.drop(columns=drop)
        else:
            raise ValueError(f"Unsupported operator: {operator}")
        tables[step.output] = result
        return result

    def compile_python(self, spec: PipelineSpec, source_filenames: dict[str, str]) -> str:
        lines = ["import pandas as pd", "", "# Load source tables"]
        for table_name, filename in source_filenames.items():
            lines.append(f"{table_name} = pd.read_csv(r'{filename}')")
        lines.append("")
        lines.append("# Execute pipeline")
        if spec.raw_code_lines:
            lines.extend(spec.raw_code_lines)
            lines.append("")
            lines.append(f"target = {spec.final_table}.copy()")
            lines.append("target.to_csv('bat_output.csv', index=False)")
            return "\n".join(lines) + "\n"
        for step in spec.steps:
            lines.extend(self._python_for_step(step))
        lines.append("")
        lines.append(f"target = {spec.final_table}.copy()")
        lines.append("target.to_csv('bat_output.csv', index=False)")
        return "\n".join(lines) + "\n"

    def compile_transformation_body(self, spec: PipelineSpec) -> tuple[list[str], dict[str, int]]:
        if spec.raw_code_lines:
            line_map: dict[str, int] = {}
            fallback_line = 1
            for step in spec.steps:
                line_index = step.params.get("_code_line_index")
                if isinstance(line_index, int) and line_index >= 1:
                    line_map[step.step_id] = line_index
                    fallback_line = max(fallback_line, line_index + 1)
                else:
                    line_map[step.step_id] = fallback_line
                    fallback_line += 1
            return list(spec.raw_code_lines), line_map

        lines: list[str] = []
        line_map: dict[str, int] = {}
        for step in spec.steps:
            line_map[step.step_id] = len(lines) + 1
            lines.extend(self._python_for_step(step))
        return lines, line_map

    def _python_for_step(self, step: PipelineStep) -> list[str]:
        if step.operator == OperatorType.SOURCE_TABLE:
            return [f"{step.output} = {step.params['source_table']}.copy()"]
        if step.operator == OperatorType.RENAME:
            return [f"{step.output} = {step.inputs[0]}.rename(columns={step.params.get('mapping', {})!r})"]
        if step.operator == OperatorType.DATE_FORMATTING:
            column = step.params["column"]
            source_column = step.params.get("source_column", column)
            input_format = step.params.get("input_format")
            format_arg = f", format={input_format!r}" if input_format else ""
            return [
                f"{step.output} = {step.inputs[0]}.copy()",
                f"{step.output}[{column!r}] = pd.to_datetime({step.output}[{source_column!r}], errors='coerce'{format_arg}).dt.strftime({step.params.get('output_format', '%Y-%m-%d')!r})",
            ]
        if step.operator == OperatorType.GROUPBY:
            agg_spec = {
                item["output"]: (item["source"], item.get("func", "sum"))
                for item in step.params["aggregations"]
            }
            return [f"{step.output} = {step.inputs[0]}.groupby({step.params['keys']!r}, dropna=False).agg(**{agg_spec!r}).reset_index()"]
        if step.operator == OperatorType.JOIN:
            return [
                f"{step.output} = {step.inputs[0]}.merge({step.inputs[1]}, how={step.params.get('how', 'left')!r}, left_on={step.params.get('left_on')!r}, right_on={step.params.get('right_on')!r}, suffixes=('_left', '_right'))"
            ]
        if step.operator == OperatorType.UNION:
            return [f"{step.output} = pd.concat([{', '.join(step.inputs)}], ignore_index=True)"]
        if step.operator == OperatorType.UNPIVOT:
            return [
                f"{step.output} = {step.inputs[0]}.melt(id_vars={step.params.get('id_vars')!r}, value_vars={step.params.get('value_vars')!r}, var_name={step.params.get('var_name', 'variable')!r}, value_name={step.params.get('value_name', 'value')!r})"
            ]
        if step.operator == OperatorType.PIVOT:
            return [
                f"{step.output} = {step.inputs[0]}.pivot_table(index={step.params.get('index')!r}, columns={step.params.get('columns')!r}, values={step.params.get('values')!r}, aggfunc={step.params.get('aggfunc', 'first')!r}).reset_index()"
            ]
        if step.operator == OperatorType.COLUMN_ARITHMETIC:
            return [
                f"{step.output} = {step.inputs[0]}.copy()",
                f"{step.output}[{step.params['output_column']!r}] = {step.output}.eval({step.params['expression']!r})",
            ]
        if step.operator == OperatorType.ADD_COLUMNS:
            lines = [f"{step.output} = {step.inputs[0]}.copy()"]
            for mapping in step.params.get("mappings", []):
                kind = mapping.get("kind", "constant")
                output = mapping["output"]
                if kind == "constant":
                    lines.append(f"{step.output}[{output!r}] = {mapping.get('value')!r}")
                elif kind == "copy":
                    lines.append(f"{step.output}[{output!r}] = {step.output}[{mapping['source']!r}]")
                elif kind == "template":
                    lines.append(
                        f"{step.output}[{output!r}] = {step.output}.apply(lambda row: {mapping['template']!r}.format(**row.to_dict()), axis=1)"
                    )
            return lines
        if step.operator == OperatorType.DROP_COLUMNS:
            if "keep" in step.params:
                keep = step.params["keep"]
                return [f"{step.output} = {step.inputs[0]}[[col for col in {keep!r} if col in {step.inputs[0]}.columns]].copy()"]
            return [f"{step.output} = {step.inputs[0]}.drop(columns={step.params.get('drop', [])!r})"]
        raise ValueError(f"Unsupported operator: {step.operator}")

    def clone(self, spec: PipelineSpec) -> PipelineSpec:
        return PipelineSpec.model_validate(copy.deepcopy(spec.model_dump()))

    def extract_last_variable(self, code_str: str) -> str | None:
        tree = ast.parse(code_str)
        last_var = None
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for target in reversed(node.targets):
                    if isinstance(target, ast.Name):
                        last_var = target.id
                        break
        return last_var
