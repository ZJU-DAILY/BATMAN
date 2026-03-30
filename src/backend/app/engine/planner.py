from __future__ import annotations

from dataclasses import dataclass

from app.engine.bat_search import BatSearchPlanner
from app.engine.utils import (
    SYNONYM_GROUPS,
    choose_main_table,
    detect_join_keys,
    normalize_name,
    similar_fields,
)
from app.models import OperatorType, PipelineSpec, PipelineStep, Session


@dataclass(slots=True)
class MappingRef:
    table: str
    column: str


class HeuristicPipelinePlanner:
    def generate_pipeline_specs(self, session: Session) -> list[PipelineSpec]:
        if not session.source_tables or not session.target_schema:
            return []
        return self._heuristic_candidates(session)

    def _heuristic_candidates(self, session: Session) -> list[PipelineSpec]:
        source_columns = {table.name: table.columns for table in session.source_tables}
        main_table = choose_main_table(session.target_schema, source_columns)
        mappings = self._map_target_fields(session)
        aggregate_fields = [
            field.name
            for field in session.target_schema
            if "total" in normalize_name(field.name) or "sum" in normalize_name(field.description)
        ]
        group_keys = self._infer_group_keys(session)

        steps: list[PipelineStep] = [
            PipelineStep(
                step_id="step_1",
                operator=OperatorType.SOURCE_TABLE,
                title=f"Load {main_table}",
                inputs=[main_table],
                output="main_source",
                params={"source_table": main_table},
                notes="The primary source table is selected from the uploaded inputs.",
            )
        ]

        rename_mapping = {
            ref.column: field_name
            for field_name, ref in mappings.items()
            if ref.table == main_table and ref.column != field_name
        }
        current_main = "main_source"
        if rename_mapping:
            steps.append(
                PipelineStep(
                    step_id=f"step_{len(steps)+1}",
                    operator=OperatorType.RENAME,
                    title="Align source field names",
                    inputs=[current_main],
                    output="renamed_main",
                    params={"mapping": rename_mapping},
                    notes="Field names are standardized to match the target schema.",
                )
            )
            current_main = "renamed_main"

        date_field = self._find_date_field(session)
        if date_field:
            steps.append(
                PipelineStep(
                    step_id=f"step_{len(steps)+1}",
                    operator=OperatorType.DATE_FORMATTING,
                    title="Normalize date values",
                    inputs=[current_main],
                    output="dated_main",
                    params={"column": date_field, "output_format": "%Y-%m-%d"},
                    notes="Date values are normalized for export and validation.",
                )
            )
            current_main = "dated_main"

        auxiliary_outputs: list[str] = []
        for table in session.source_tables:
            if table.name == main_table:
                continue
            base_output = f"{table.name}_base"
            steps.append(
                PipelineStep(
                    step_id=f"step_{len(steps)+1}",
                    operator=OperatorType.SOURCE_TABLE,
                    title=f"Load {table.name}",
                    inputs=[table.name],
                    output=base_output,
                    params={"source_table": table.name},
                    notes=f"{table.name} contributes additional target fields.",
                )
            )
            aux_mapping = {
                ref.column: field_name
                for field_name, ref in mappings.items()
                if ref.table == table.name and ref.column != field_name
            }
            prepared_output = base_output
            if aux_mapping:
                prepared_output = f"{table.name}_prepared"
                steps.append(
                    PipelineStep(
                        step_id=f"step_{len(steps)+1}",
                        operator=OperatorType.RENAME,
                        title=f"Align {table.name} fields",
                        inputs=[base_output],
                        output=prepared_output,
                        params={"mapping": aux_mapping},
                        notes="Shared fields are renamed before joining.",
                    )
                )
            auxiliary_outputs.append(prepared_output)

        final_base = current_main
        if aggregate_fields:
            measure_column = self._find_measure_column(session, main_table)
            if measure_column:
                normalized_measure = rename_mapping.get(measure_column, measure_column)
                steps.append(
                    PipelineStep(
                        step_id=f"step_{len(steps)+1}",
                        operator=OperatorType.GROUPBY,
                        title="Compute aggregate metrics",
                        inputs=[current_main],
                        output="aggregated_metrics",
                        params={
                            "keys": group_keys,
                            "aggregations": [{"source": normalized_measure, "func": "sum", "output": aggregate_fields[0]}],
                        },
                        notes="The target aggregate field is computed from the primary measure column.",
                    )
                )
                if self._should_keep_detail_rows(session, group_keys):
                    steps.append(
                        PipelineStep(
                            step_id=f"step_{len(steps)+1}",
                            operator=OperatorType.JOIN,
                            title="Attach aggregate metrics back to detail rows",
                            inputs=[current_main, "aggregated_metrics"],
                            output="detail_with_metrics",
                            params={"left_on": group_keys, "right_on": group_keys, "how": "left"},
                            notes="The aggregate is joined back so category-level rows remain visible.",
                        )
                    )
                    final_base = "detail_with_metrics"
                else:
                    final_base = "aggregated_metrics"

        for prepared in auxiliary_outputs:
            table_name = prepared.replace("_prepared", "").replace("_base", "")
            left_columns = next(table.columns for table in session.source_tables if table.name == main_table)
            right_columns = next(table.columns for table in session.source_tables if table.name == table_name)
            join_keys = detect_join_keys(left_columns, right_columns)
            if not join_keys:
                continue
            left_on = [rename_mapping.get(left, left) for left, _ in join_keys]
            right_mapping = {
                ref.column: field_name
                for field_name, ref in mappings.items()
                if ref.table == table_name
            }
            right_on = [right_mapping.get(right, right) for _, right in join_keys]
            joined_output = f"{table_name}_joined"
            steps.append(
                PipelineStep(
                    step_id=f"step_{len(steps)+1}",
                    operator=OperatorType.JOIN,
                    title=f"Join {table_name}",
                    inputs=[final_base, prepared],
                    output=joined_output,
                    params={"left_on": left_on, "right_on": right_on, "how": "left"},
                    notes=f"Fields from {table_name} are added to the working table.",
                )
            )
            final_base = joined_output

        keep_columns = [field.name for field in session.target_schema]
        steps.append(
            PipelineStep(
                step_id=f"step_{len(steps)+1}",
                operator=OperatorType.DROP_COLUMNS,
                title="Keep only the target schema columns",
                inputs=[final_base],
                output="final_output",
                params={"keep": keep_columns},
                notes="Only the fields required by the target schema are kept for export.",
            )
        )

        primary = PipelineSpec(
            steps=steps,
            final_table="final_output",
            rationale="Heuristic candidate built from target-schema overlap, aggregate detection, and join-key discovery.",
        )
        secondary = PipelineSpec.model_validate(primary.model_dump())
        secondary.rationale = "Alternative candidate that prioritizes compact output over detail preservation."
        secondary.warnings.append("This alternative candidate may remove detail-level rows.")
        removed_outputs: dict[str, str] = {}
        retained_steps: list[PipelineStep] = []
        for step in secondary.steps:
            if step.operator == OperatorType.JOIN and "category-level rows remain visible" in step.notes:
                replacement_input = step.inputs[1] if len(step.inputs) > 1 else (step.inputs[0] if step.inputs else "")
                if replacement_input:
                    removed_outputs[step.output] = replacement_input
                continue
            step.inputs = [removed_outputs.get(input_name, input_name) for input_name in step.inputs]
            if secondary.final_table == step.output and step.output in removed_outputs:
                secondary.final_table = removed_outputs[step.output]
            retained_steps.append(step)
        secondary.steps = retained_steps
        for step in reversed(secondary.steps):
            if step.operator == OperatorType.DROP_COLUMNS:
                if "Product_category" in step.params.get("keep", []):
                    step.params["keep"] = [col for col in step.params["keep"] if col != "Product_category"]
                break
        return [primary, secondary]

    def _map_target_fields(self, session: Session) -> dict[str, MappingRef]:
        mapping: dict[str, MappingRef] = {}
        for field in session.target_schema:
            for table in session.source_tables:
                found = next((column for column in table.columns if similar_fields(field.name, column)), None)
                if found:
                    mapping[field.name] = MappingRef(table=table.name, column=found)
                    break
        return mapping

    def _find_date_field(self, session: Session) -> str | None:
        for field in session.target_schema:
            normalized = normalize_name(field.name)
            if normalized in SYNONYM_GROUPS["date"] or field.type.value in {"DATE", "DATETIME"}:
                return field.name
        return None

    def _find_measure_column(self, session: Session, table_name: str) -> str | None:
        table = next((table for table in session.source_tables if table.name == table_name), None)
        if not table:
            return None
        for column in table.columns:
            if normalize_name(column) in SYNONYM_GROUPS["sales"]:
                return column
        for column, dtype in table.inferred_types.items():
            if dtype.value in {"INTEGER", "FLOAT"}:
                return column
        return None

    def _infer_group_keys(self, session: Session) -> list[str]:
        keys = []
        for field in session.target_schema:
            normalized = normalize_name(field.name)
            if normalized in SYNONYM_GROUPS["shop_id"] or normalized in SYNONYM_GROUPS["date"]:
                keys.append(field.name)
        return keys or [field.name for field in session.target_schema[:2]]

    def _should_keep_detail_rows(self, session: Session, group_keys: list[str]) -> bool:
        if session.target_samples and any("Product_category" in row for row in session.target_samples):
            return True
        target_fields = {field.name for field in session.target_schema}
        return any(field for field in target_fields if field not in set(group_keys) and "total" not in normalize_name(field))


class PipelinePlanner:
    def __init__(self) -> None:
        self.bat_search = BatSearchPlanner()

    async def generate_pipeline_specs(self, session: Session) -> list[PipelineSpec]:
        return await self.bat_search.generate(session)
