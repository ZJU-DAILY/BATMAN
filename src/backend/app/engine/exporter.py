from __future__ import annotations
import zipfile
from pathlib import Path

import pandas as pd

from app.engine.compiler import PipelineCompiler
from app.engine.utils import runtime_source_name_map
from app.models import CandidatePipeline, Session


class Exporter:
    def __init__(self) -> None:
        self.compiler = PipelineCompiler()

    def build_final_output(self, session: Session, candidate: CandidatePipeline) -> pd.DataFrame:
        source_filenames = runtime_source_name_map(session.source_tables)
        tables = {name: pd.read_csv(path) for name, path in source_filenames.items()}
        if candidate.pipeline_spec.raw_code_lines:
            return self.compiler.execute_raw_code(candidate.pipeline_spec.raw_code_lines, tables)
        for step in candidate.pipeline_spec.steps:
            self.compiler.apply_step(step, tables)
        return tables[candidate.pipeline_spec.final_table]

    def build_target_table_frames(self, session: Session, candidate: CandidatePipeline) -> tuple[pd.DataFrame, pd.DataFrame]:
        before_df = pd.DataFrame(session.target_samples)
        final_df = self.build_final_output(session, candidate)
        ordered_columns = [field.name for field in session.target_schema]
        extra_columns = [column for column in final_df.columns if column not in ordered_columns]
        merged_columns = ordered_columns + extra_columns

        if before_df.empty:
            before_df = pd.DataFrame(columns=merged_columns)
        else:
            before_df = before_df.reindex(columns=merged_columns)
        generated_df = final_df.reindex(columns=merged_columns)
        after_df = pd.concat([before_df, generated_df], ignore_index=True)
        return before_df, after_df

    def export(self, session: Session, candidate: CandidatePipeline, export_dir: Path) -> dict[str, Path]:
        export_dir.mkdir(parents=True, exist_ok=True)
        source_filenames = runtime_source_name_map(session.source_tables)
        final_df = self.build_final_output(session, candidate)
        before_df, target_table_df = self.build_target_table_frames(session, candidate)

        csv_path = export_dir / "transformed_rows.csv"
        py_path = export_dir / "pipeline.py"
        target_table_path = export_dir / "target_table_with_new_rows.csv"
        zip_path = export_dir / "bat_export_bundle.zip"

        final_df.to_csv(csv_path, index=False)
        py_path.write_text(
            self.compiler.compile_python(candidate.pipeline_spec, source_filenames),
            encoding="utf-8",
        )
        target_table_df.to_csv(target_table_path, index=False)
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as bundle:
            bundle.write(csv_path, arcname=csv_path.name)
            bundle.write(py_path, arcname=py_path.name)
            bundle.write(target_table_path, arcname=target_table_path.name)
        return {"csv": csv_path, "python": py_path, "target_table": target_table_path, "all": zip_path}


exporter = Exporter()
