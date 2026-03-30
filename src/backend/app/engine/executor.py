from __future__ import annotations

import io
from concurrent.futures import ProcessPoolExecutor
from typing import Any

import pandas as pd

from app.engine.compiler import PipelineCompiler
from app.engine.utils import guess_preview_rows, runtime_source_name_map
from app.models import PipelineSpec, PipelineStep, Session, StepPreview


def _execute_raw_with_previews(spec_payload: dict[str, Any], source_map: dict[str, str], preview_rows: int) -> dict[str, Any]:
    compiler = PipelineCompiler()
    tables: dict[str, pd.DataFrame] = {name: pd.read_csv(path) for name, path in source_map.items()}
    env = {"pd": pd, **{name: frame.copy() for name, frame in tables.items()}}
    snapshots: dict[int, dict[str, pd.DataFrame]] = {}

    for index, line in enumerate(spec_payload.get("raw_code_lines", []), start=1):
        line = line.strip()
        if not line:
            continue
        exec(line, env)
        snapshots[index] = {key: value.copy() for key, value in env.items() if isinstance(value, pd.DataFrame)}

    previews: list[dict[str, Any]] = []
    previous_by_output: dict[str, pd.DataFrame] = {}
    for raw_step in spec_payload["steps"]:
        line_index = raw_step.get("params", {}).get("_code_line_index")
        output_table = raw_step["output"]
        snapshot = snapshots.get(line_index, {})
        result = snapshot.get(output_table)
        if result is None:
            continue
        before_columns = previous_by_output.get(output_table, pd.DataFrame()).columns.tolist()
        after_columns = result.columns.tolist()
        previews.append(
            {
                "step_id": raw_step["step_id"],
                "title": raw_step["title"],
                "operator": raw_step["operator"],
                "output_table": output_table,
                "row_count": int(len(result.index)),
                "columns": after_columns,
                "preview_rows": guess_preview_rows(result, preview_rows),
                "added_columns": [col for col in after_columns if col not in before_columns],
                "removed_columns": [col for col in before_columns if col not in after_columns],
                "renamed_columns": raw_step.get("params", {}).get("mapping", {}),
                "notes": raw_step.get("notes", ""),
                "warnings": [],
            }
        )
        previous_by_output[output_table] = result.copy()

    final_df = compiler.execute_raw_code(spec_payload.get("raw_code_lines", []), tables)
    return {
        "step_previews": previews,
        "final_preview_rows": guess_preview_rows(final_df, preview_rows),
        "final_csv": final_df.to_csv(index=False),
    }


def _run_spec(spec_payload: dict[str, Any], source_map: dict[str, str], preview_rows: int) -> dict[str, Any]:
    if spec_payload.get("raw_code_lines"):
        return _execute_raw_with_previews(spec_payload, source_map, preview_rows)

    compiler = PipelineCompiler()
    tables: dict[str, pd.DataFrame] = {
        table_name: pd.read_csv(path) for table_name, path in source_map.items()
    }
    previews: list[dict[str, Any]] = []

    for raw_step in spec_payload["steps"]:
        before_columns: list[str] = []
        if raw_step["inputs"]:
            input_name = raw_step["inputs"][0]
            if input_name in tables:
                before_columns = tables[input_name].columns.tolist()

        step = PipelineStep.model_validate(raw_step)
        result = compiler.apply_step(step, tables)
        after_columns = result.columns.tolist()

        previews.append(
            {
                "step_id": raw_step["step_id"],
                "title": raw_step["title"],
                "operator": raw_step["operator"],
                "output_table": raw_step["output"],
                "row_count": int(len(result.index)),
                "columns": after_columns,
                "preview_rows": guess_preview_rows(result, preview_rows),
                "added_columns": [col for col in after_columns if col not in before_columns],
                "removed_columns": [col for col in before_columns if col not in after_columns],
                "renamed_columns": raw_step.get("params", {}).get("mapping", {}) if raw_step["operator"] == "rename" else {},
                "notes": raw_step.get("notes", ""),
                "warnings": [],
            }
        )

    final_df = tables[spec_payload["final_table"]]
    return {
        "step_previews": previews,
        "final_preview_rows": guess_preview_rows(final_df, preview_rows),
        "final_csv": final_df.to_csv(index=False),
    }


class IntermediateExecutor:
    def __init__(self) -> None:
        self.pool: ProcessPoolExecutor | None = None

    def _get_pool(self) -> ProcessPoolExecutor | None:
        if self.pool is not None:
            return self.pool
        try:
            self.pool = ProcessPoolExecutor(max_workers=2)
        except Exception:
            self.pool = None
        return self.pool

    def execute(self, session: Session, spec: PipelineSpec) -> tuple[list[StepPreview], list[dict[str, Any]], pd.DataFrame]:
        source_map = runtime_source_name_map(session.source_tables)
        pool = self._get_pool()
        if pool is None:
            payload = _run_spec(spec.model_dump(), source_map, session.settings.preview_rows)
        else:
            try:
                future = pool.submit(_run_spec, spec.model_dump(), source_map, session.settings.preview_rows)
                payload = future.result(timeout=30)
            except Exception:
                payload = _run_spec(spec.model_dump(), source_map, session.settings.preview_rows)
        step_previews = [StepPreview.model_validate(item) for item in payload["step_previews"]]
        final_df = pd.read_csv(io.StringIO(payload["final_csv"]))
        return step_previews, payload["final_preview_rows"], final_df
