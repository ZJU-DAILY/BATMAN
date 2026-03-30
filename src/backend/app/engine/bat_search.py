from __future__ import annotations

import json
import math
import random
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import pandas as pd

from app.config import settings
from app.engine.bat_prompts import (
    build_table_schema_dict,
    get_identify_function_prompt,
    get_schema_match_prompt,
    get_transformation_prompt,
    get_transformation_revision_prompt,
)
from app.engine.compiler import PipelineCompiler
from app.engine.normalizer import CodeToPipelineNormalizer
from app.engine.structure_validator import structure_validator
from app.engine.utils import runtime_source_name_map
from app.engine.validator import PipelineValidator
from app.models import PipelineSpec, Session
from app.services.llm_client import llm_client


class BatActionType(str, Enum):
    ROOT = "root"
    SCHEMA_MAPPING = "schema_mapping"
    OPERATOR_DISCOVERY = "operator_discovery"
    CODE_SYNTHESIS = "code_synthesis"
    CODE_REFINEMENT = "code_refinement"
    TERMINATION = "termination"


@dataclass(slots=True)
class BatState:
    schema_mapping: Any = None
    operator_plan: str | None = None
    pipeline_spec: PipelineSpec | None = None
    validation_payload: dict[str, Any] | None = None


@dataclass(slots=True)
class SearchNode:
    action_type: BatActionType
    state: BatState
    parent: SearchNode | None = None
    children: list["SearchNode"] = field(default_factory=list)
    visits: int = 0
    reward: float = 0.0
    depth: int = 0

    def is_terminal(self) -> bool:
        return self.action_type == BatActionType.TERMINATION


class BatSearchPlanner:
    def __init__(self) -> None:
        self.validator = PipelineValidator()
        self.compiler = PipelineCompiler()
        self.normalizer = CodeToPipelineNormalizer()
        self.reward_cache: dict[str, float] = {}
        self.validation_cache: dict[str, dict[str, Any]] = {}
        self.action_cache: dict[str, list[str]] = {}
        self.state_cache: set[str] = set()
        self.invalid_candidate_errors: list[str] = []

    async def generate(self, session: Session) -> list[PipelineSpec]:
        self._reset_search_state()
        if not llm_client.enabled:
            raise ValueError("BAT generation is unavailable because the model service is not configured.")

        config = self._mode_config(session)
        root = SearchNode(action_type=BatActionType.ROOT, state=BatState(), depth=0)
        terminal_nodes: list[SearchNode] = []

        for _ in range(config["rollouts"]):
            leaf = self._select(root, config["exploration_constant"])
            if leaf.is_terminal():
                self._backpropagate(leaf, leaf.reward)
                continue
            expanded = await self._expand(session, leaf, config)
            if not expanded:
                break
            chosen = random.choice(expanded)
            end_node = await self._simulate(session, chosen, config)
            reward = await self._reward_for_node(session, end_node)
            end_node.reward = reward
            terminal_nodes.append(end_node)
            self._backpropagate(end_node, reward)
            if len([node for node in terminal_nodes if node.reward >= config["early_stop_reward"]]) >= config["early_stop_count"]:
                break

        ranked: list[SearchNode] = []
        for node in terminal_nodes:
            if node.state.pipeline_spec is None:
                continue
            payload = node.state.validation_payload or await self._validation_payload(session, node.state.pipeline_spec)
            node.state.validation_payload = payload
            if not payload.get("executable"):
                reason = self._validation_failure_message(payload)
                self._remember_invalid_candidate(f"BAT generated invalid code: {reason}")
                continue
            ranked.append(node)
        ranked.sort(key=lambda item: item.reward, reverse=True)
        unique: list[PipelineSpec] = []
        seen = set()
        for node in ranked:
            assert node.state.pipeline_spec is not None
            key = node.state.pipeline_spec.model_dump_json()
            if key in seen:
                continue
            seen.add(key)
            unique.append(node.state.pipeline_spec)
            if len(unique) >= config["return_candidates"]:
                break
        if unique:
            return unique
        raise ValueError(self._invalid_generation_error())

    def _mode_config(self, session: Session) -> dict[str, Any]:
        return {
            "rollouts": session.settings.bat_max_rollout_steps,
            "branching": 1,
            "max_depth": session.settings.bat_max_depth,
            "exploration_constant": session.settings.bat_exploration_constant,
            "return_candidates": 2,
            "early_stop_reward": 1.0,
            "early_stop_count": 2,
            "action_strategy": "schema_operator_codegen_refine",
            "temperature": session.settings.bat_temperature,
            "top_p": session.settings.bat_top_p,
        }

    def _valid_actions(self, node: SearchNode, action_strategy: str) -> list[BatActionType]:
        if action_strategy == "schema_then_codegen":
            if node.action_type == BatActionType.ROOT:
                return [BatActionType.SCHEMA_MAPPING]
            if node.action_type == BatActionType.SCHEMA_MAPPING:
                return [BatActionType.CODE_SYNTHESIS]
            if node.action_type == BatActionType.CODE_SYNTHESIS:
                return [BatActionType.TERMINATION]
            return []

        if action_strategy == "schema_operator_codegen":
            if node.action_type == BatActionType.ROOT:
                return [BatActionType.SCHEMA_MAPPING]
            if node.action_type == BatActionType.SCHEMA_MAPPING:
                return [BatActionType.OPERATOR_DISCOVERY]
            if node.action_type == BatActionType.OPERATOR_DISCOVERY:
                return [BatActionType.CODE_SYNTHESIS]
            if node.action_type == BatActionType.CODE_SYNTHESIS:
                return [BatActionType.TERMINATION]
            return []

        if node.action_type == BatActionType.ROOT:
            return [BatActionType.SCHEMA_MAPPING]
        if node.action_type == BatActionType.SCHEMA_MAPPING:
            return [BatActionType.OPERATOR_DISCOVERY]
        if node.action_type == BatActionType.OPERATOR_DISCOVERY:
            return [BatActionType.CODE_SYNTHESIS]
        if node.action_type == BatActionType.CODE_SYNTHESIS:
            return [BatActionType.TERMINATION, BatActionType.CODE_REFINEMENT]
        if node.action_type == BatActionType.CODE_REFINEMENT:
            return [BatActionType.TERMINATION]
        return []

    def _select(self, root: SearchNode, exploration_constant: float) -> SearchNode:
        current = root
        while current.children and not current.is_terminal():
            unvisited = [child for child in current.children if child.visits == 0]
            if unvisited:
                return unvisited[0]
            current = max(
                current.children,
                key=lambda child: (child.reward / child.visits) + exploration_constant * math.sqrt(math.log(current.visits + 1) / child.visits),
            )
        return current

    async def _expand(self, session: Session, node: SearchNode, config: dict[str, Any]) -> list[SearchNode]:
        if node.children:
            return node.children
        children: list[SearchNode] = []
        for action in self._valid_actions(node, config.get("action_strategy", "schema_operator_codegen")):
            children.extend(await self._apply_action(session, node, action, config))
        node.children = children
        return children

    async def _simulate(self, session: Session, node: SearchNode, config: dict[str, Any]) -> SearchNode:
        current = node
        while not current.is_terminal() and current.depth < config["max_depth"]:
            children = await self._expand(session, current, config)
            if not children:
                if current.state.pipeline_spec:
                    terminal = SearchNode(
                        action_type=BatActionType.TERMINATION,
                        state=current.state,
                        parent=current,
                        depth=current.depth + 1,
                    )
                    current.children.append(terminal)
                    return terminal
                return current
            current = random.choice(children)
        return current

    def _backpropagate(self, node: SearchNode, reward: float) -> None:
        current = node
        while current is not None:
            current.visits += 1
            current.reward += reward
            current = current.parent

    async def _apply_action(self, session: Session, node: SearchNode, action: BatActionType, config: dict[str, Any]) -> list[SearchNode]:
        table_schema_dict = build_table_schema_dict(session)
        branching = config["branching"]

        if action == BatActionType.TERMINATION:
            if node.state.pipeline_spec is None:
                return []
            return [SearchNode(action_type=BatActionType.TERMINATION, state=node.state, parent=node, depth=node.depth + 1)]

        if action == BatActionType.SCHEMA_MAPPING:
            hint = ""
            if node.state.operator_plan:
                hint = f"\n\nHere are my previous thoughts:\nPossible column functions: {node.state.operator_plan}\n"
            prompt = get_schema_match_prompt(table_schema_dict, hint)
            outputs = await self._cached_texts(
                cache_key=f"{action}:{prompt}",
                prompt=prompt,
                temperature=config["temperature"],
                top_p=config["top_p"],
                n=branching,
                system_prompt=self._schema_system_prompt(),
            )
            children = []
            for output in outputs:
                mapping = self._extract_schema_mapping(output)
                if mapping is None:
                    continue
                child = SearchNode(
                    action_type=BatActionType.SCHEMA_MAPPING,
                    state=BatState(
                        schema_mapping=mapping,
                        operator_plan=node.state.operator_plan,
                        pipeline_spec=node.state.pipeline_spec,
                        validation_payload=node.state.validation_payload,
                    ),
                    parent=node,
                    depth=node.depth + 1,
                )
                if self._should_keep_child(child):
                    children.append(child)
            return children

        if action == BatActionType.OPERATOR_DISCOVERY:
            hint = ""
            if node.state.schema_mapping is not None:
                hint = f"\n\nHere are my previous thoughts:\nPossible schema match info: {json.dumps(node.state.schema_mapping, ensure_ascii=False)}\n"
            if len(session.source_tables) == 1 and not session.target_samples:
                hint += (
                    "\nMetadata-only guidance: prefer a single-table pipeline that renames fields, normalizes dates, "
                    "aggregates to the target grain when needed, and keeps only the target-ready columns.\n"
                )
            extra_hint = self._code_generation_hint(session)
            if extra_hint:
                hint += f"\n{extra_hint}\n"
            prompt = get_identify_function_prompt(table_schema_dict, hint)
            outputs = await self._cached_texts(
                cache_key=f"{action}:{prompt}",
                prompt=prompt,
                temperature=config["temperature"],
                top_p=config["top_p"],
                n=branching,
                system_prompt=self._operator_system_prompt(),
            )
            children = []
            for output in outputs:
                if not output.strip():
                    continue
                child = SearchNode(
                    action_type=BatActionType.OPERATOR_DISCOVERY,
                    state=BatState(
                        schema_mapping=node.state.schema_mapping,
                        operator_plan=output.strip(),
                        pipeline_spec=node.state.pipeline_spec,
                        validation_payload=node.state.validation_payload,
                    ),
                    parent=node,
                    depth=node.depth + 1,
                )
                if self._should_keep_child(child):
                    children.append(child)
            return children

        if action == BatActionType.CODE_SYNTHESIS:
            hint = ""
            if node.state.operator_plan:
                hint += f"Possible column functions: {node.state.operator_plan}\n"
            if len(session.source_tables) == 1 and not session.target_samples:
                hint += (
                    "Metadata-only guidance: this is a single-source task. Avoid unnecessary joins or helper tables; "
                    "prefer rename -> date formatting -> groupby -> keep-columns when that matches the schema.\n"
                )
            extra_hint = self._code_generation_hint(session)
            if extra_hint:
                hint += f"{extra_hint}\n"
            prompt_hint = f"\n\nHere are my previous thoughts:\n{hint}" if hint else ""
            prompt = get_transformation_prompt(table_schema_dict, prompt_hint)
            outputs = await self._cached_texts(
                cache_key=f"{action}:{prompt}",
                prompt=prompt,
                temperature=config["temperature"],
                top_p=config["top_p"],
                n=branching,
                system_prompt=self._code_system_prompt(),
            )
            nodes: list[SearchNode] = []
            for output in outputs:
                code_lines = self._extract_code_lines(output)
                if not code_lines:
                    continue
                spec = await self._code_to_pipeline_spec_with_repair(
                    session,
                    code_lines,
                    origin="BAT generated code",
                    table_schema_dict=table_schema_dict,
                    operator_plan=node.state.operator_plan,
                    config=config,
                )
                if spec is None:
                    continue
                validation_payload = await self._validation_payload(session, spec)
                if not validation_payload.get("executable"):
                    reason = self._validation_failure_message(validation_payload)
                    self._remember_invalid_candidate(f"BAT generated invalid code: {reason}")
                    continue
                child = SearchNode(
                    action_type=BatActionType.CODE_SYNTHESIS,
                    state=BatState(
                        schema_mapping=node.state.schema_mapping,
                        operator_plan=node.state.operator_plan,
                        pipeline_spec=spec,
                        validation_payload=validation_payload,
                    ),
                    parent=node,
                    depth=node.depth + 1,
                )
                if self._should_keep_child(child):
                    nodes.append(child)
            return nodes

        if action == BatActionType.CODE_REFINEMENT and node.state.pipeline_spec is not None:
            validation_payload = await self._validation_payload(session, node.state.pipeline_spec)
            previous_code = node.state.pipeline_spec.raw_code_lines
            refinement_hint = node.state.operator_plan or ""
            extra_hint = self._code_generation_hint(session)
            if extra_hint:
                refinement_hint = f"{refinement_hint}\n{extra_hint}".strip()
            prompt = get_transformation_revision_prompt(
                table_schema_dict=table_schema_dict,
                hint=f"\n\nHere are my previous thoughts:\n{refinement_hint}" if refinement_hint else "",
                original_code=previous_code,
                error_message="\n".join(validation_payload.get("warnings", [])),
                exec_result=json.dumps(validation_payload, ensure_ascii=False),
            )
            outputs = await self._cached_texts(
                cache_key=f"{action}:{prompt}",
                prompt=prompt,
                temperature=config["temperature"],
                top_p=config["top_p"],
                n=branching,
                system_prompt=self._code_system_prompt(),
            )
            nodes: list[SearchNode] = []
            for output in outputs:
                code_lines = self._extract_code_lines(output)
                if not code_lines:
                    continue
                spec = await self._code_to_pipeline_spec_with_repair(
                    session,
                    code_lines,
                    origin="BAT refined code",
                    table_schema_dict=table_schema_dict,
                    operator_plan=node.state.operator_plan,
                    config=config,
                )
                if spec is None:
                    continue
                refined_validation_payload = await self._validation_payload(session, spec)
                if not refined_validation_payload.get("executable"):
                    reason = self._validation_failure_message(refined_validation_payload)
                    self._remember_invalid_candidate(f"BAT generated invalid code: {reason}")
                    continue
                child = SearchNode(
                    action_type=BatActionType.CODE_REFINEMENT,
                    state=BatState(
                        schema_mapping=node.state.schema_mapping,
                        operator_plan=node.state.operator_plan,
                        pipeline_spec=spec,
                        validation_payload=refined_validation_payload,
                    ),
                    parent=node,
                    depth=node.depth + 1,
                )
                if self._should_keep_child(child):
                    nodes.append(child)
            return nodes

        return []

    async def _reward_for_node(self, session: Session, node: SearchNode) -> float:
        if node.state.pipeline_spec is None:
            return 0.0
        key = node.state.pipeline_spec.model_dump_json()
        if key in self.reward_cache:
            return self.reward_cache[key]
        payload = await self._validation_payload(session, node.state.pipeline_spec)
        score = 0.0
        if payload["executable"]:
            score += 0.45
        if payload["column_match"]:
            score += 0.2
        if payload["required_fields_met"]:
            score += 0.15
        score += 0.1 * payload["type_compatibility"]
        score += 0.1 * (payload["example_similarity"] or 0.0)
        reward = max(0.0, min(1.0, round(score, 4)))
        self.reward_cache[key] = reward
        return reward

    async def _validation_payload(self, session: Session, spec: PipelineSpec) -> dict[str, Any]:
        key = spec.model_dump_json()
        if key in self.validation_cache:
            return self.validation_cache[key]
        try:
            tables = {name: pd.read_csv(path) for name, path in runtime_source_name_map(session.source_tables).items()}
            if spec.raw_code_lines:
                final_df = self.compiler.execute_raw_code(spec.raw_code_lines, tables)
            else:
                for step in spec.steps:
                    self.compiler.apply_step(step, tables)
                final_df = tables[spec.final_table]
            validation = self.validator.validate(final_df, session.target_schema, session.target_samples, warnings=list(spec.warnings))
            payload = validation.model_dump()
        except Exception as exc:
            payload = {
                "executable": False,
                "column_match": False,
                "required_fields_met": False,
                "type_compatibility": 0.0,
                "sample_consistency": 0.0 if session.target_samples else None,
                "example_similarity": 0.0 if session.target_samples else None,
                "pipeline_correct": False,
                "warnings": [str(exc)],
            }
        self.validation_cache[key] = payload
        return payload

    def _extract_json_block(self, output: str) -> str | None:
        text = output.strip()
        match = re.search(r"```json\s*(.*?)\s*```", text, re.DOTALL)
        if match:
            return match.group(1).strip()
        if text.startswith("```") and text.endswith("```"):
            return text[3:-3].strip()
        return text if text.startswith("{") or text.startswith("[") else None

    def _extract_schema_mapping(self, output: str) -> Any:
        block = self._extract_json_block(output)
        if not block:
            return None
        try:
            return json.loads(block)
        except Exception:
            return None

    def _extract_code_lines(self, output: str) -> list[str]:
        block = self._extract_json_block(output)
        if not block:
            return []
        try:
            data = json.loads(block)
        except Exception:
            return []
        code = data.get("code", [])
        if not isinstance(code, list):
            return []
        return [line for line in code if isinstance(line, str) and line.strip()]

    async def _code_to_pipeline_spec_with_repair(
        self,
        session: Session,
        code_lines: list[str],
        *,
        origin: str,
        table_schema_dict: str,
        operator_plan: str | None,
        config: dict[str, Any],
    ) -> PipelineSpec | None:
        spec, error_message = self._code_to_pipeline_spec_or_error(session, code_lines, origin=origin)
        if spec is not None:
            return spec
        if not error_message:
            return None

        repair_hint = operator_plan or ""
        extra_hint = self._code_generation_hint(session)
        if extra_hint:
            repair_hint = f"{repair_hint}\n{extra_hint}".strip()
        repair_prompt = get_transformation_revision_prompt(
            table_schema_dict=table_schema_dict,
            hint=f"\n\nHere are my previous thoughts:\n{repair_hint}" if repair_hint else "",
            original_code=code_lines,
            error_message=error_message,
            exec_result="Structure validation failed before execution. Revise the code so every join uses existing keys and every dataframe mutation follows the required .copy() scaffold.",
        )
        repaired_outputs = await self._cached_texts(
            cache_key=f"{origin}:repair:{repair_prompt}",
            prompt=repair_prompt,
            temperature=config["temperature"],
            top_p=config["top_p"],
            n=1,
            system_prompt=self._code_system_prompt(),
        )
        for repaired_output in repaired_outputs:
            repaired_lines = self._extract_code_lines(repaired_output)
            if not repaired_lines:
                continue
            repaired_spec, _ = self._code_to_pipeline_spec_or_error(
                session,
                repaired_lines,
                origin=f"{origin} repair",
            )
            if repaired_spec is not None:
                return repaired_spec
        return None

    def _code_to_pipeline_spec_or_error(
        self,
        session: Session,
        code_lines: list[str],
        *,
        origin: str,
    ) -> tuple[PipelineSpec | None, str | None]:
        try:
            structure_validator.validate_new_code(session, code_lines, origin=origin)
            spec = self.normalizer.normalize(code_lines)
            structure_validator.validate_spec(session, spec, origin=origin)
            return spec, None
        except Exception as exc:
            message = str(exc)
            self._remember_invalid_candidate(message)
            return None, message

    async def _cached_texts(
        self,
        cache_key: str,
        prompt: str,
        temperature: float,
        top_p: float,
        n: int,
        system_prompt: str | None = None,
    ) -> list[str]:
        full_cache_key = f"{system_prompt or ''}\n{cache_key}"
        cached = self.action_cache.get(full_cache_key, [])
        if len(cached) >= n:
            return cached[:n]
        outputs = await llm_client.complete_texts(
            prompt,
            model=settings.generation_model,
            temperature=temperature,
            top_p=top_p,
            n=n,
            system_prompt=system_prompt,
        )
        unique = []
        for item in cached + outputs:
            if item and item not in unique:
                unique.append(item)
        self.action_cache[full_cache_key] = unique
        return unique[:n]

    def _schema_system_prompt(self) -> str:
        return (
            "You map target schema columns to the real source tables for a data-preparation app. "
            "Return one JSON block only, use the real source table names exactly, and do not add commentary."
        )

    def _operator_system_prompt(self) -> str:
        return (
            "You identify a concise, implementation-ready sequence of dataframe transformations. "
            "Prefer the shortest valid plan and use the real source table names."
        )

    def _code_system_prompt(self) -> str:
        return (
            "You write pandas code for a strict pipeline normalizer. "
            "Use only normalizer-safe dataframe patterns, keep source dataframes read-only, "
            "create a fresh dataframe variable for every table-level step, mutate columns only after an explicit .copy() scaffold, "
            "use named aggregation tuples for groupby, and assign final_output exactly once as the final line. "
            "Return JSON only."
        )

    def _code_generation_hint(self, session: Session) -> str:
        target_columns = [field.name for field in session.target_schema]
        broadcast_hint = self._single_row_broadcast_hint(session)
        if broadcast_hint:
            return broadcast_hint
        if len(session.source_tables) == 1:
            source_name = session.source_tables[0].name
            return (
                "Normalizer-safe skeleton example:\n"
                f"- {source_name}_base = {source_name}.copy()\n"
                f"- renamed_df = {source_name}_base.rename(columns={{...}})\n"
                "- dated_df = renamed_df.copy()\n"
                "- dated_df['Date'] = pd.to_datetime(dated_df['Date'], errors='coerce', format='...').dt.strftime('%Y-%m-%d')\n"
                "- grouped_df = dated_df.groupby([...], dropna=False).agg(**{'OutputColumn': ('SourceColumn', 'sum')}).reset_index()\n"
                f"- final_output = grouped_df[[col for col in {target_columns!r} if col in grouped_df.columns]].copy()\n"
                "Do not mutate renamed_df directly. Do not use agg({'col': 'sum'}) or groupby()['col'].sum().reset_index(name='...')."
            )

        source_names = [table.name for table in session.source_tables[:2]]
        primary_name = source_names[0] if source_names else "primary_source"
        secondary_name = source_names[1] if len(source_names) > 1 else "secondary_source"
        return (
            "Normalizer-safe multi-source skeleton example:\n"
            f"- {primary_name}_base = {primary_name}.copy()\n"
            f"- {secondary_name}_base = {secondary_name}.copy()\n"
            f"- primary_named = {primary_name}_base.rename(columns={{...}})\n"
            "- primary_dated = primary_named.copy()\n"
            "- primary_dated['Date'] = pd.to_datetime(primary_dated['Date'], errors='coerce', format='...').dt.strftime('%Y-%m-%d')\n"
            f"- secondary_named = {secondary_name}_base.rename(columns={{...}})\n"
            "- joined_output = primary_dated.merge(secondary_named, how='left', left_on=[...], right_on=[...], suffixes=('_left', '_right'))\n"
            f"- final_output = joined_output[[col for col in {target_columns!r} if col in joined_output.columns]].copy()\n"
            "Rename join keys before merge, and never rename columns after final_output has been created."
        )

    def _single_row_broadcast_hint(self, session: Session) -> str:
        if len(session.source_tables) < 2:
            return ""

        single_row_tables = [table for table in session.source_tables if table.row_count == 1]
        if not single_row_tables:
            return ""

        primary_table = max(session.source_tables, key=lambda table: table.row_count)
        target_columns = {field.name for field in session.target_schema}

        for helper_table in single_row_tables:
            if helper_table.name == primary_table.name:
                continue

            helper_columns = set(helper_table.columns)
            direct_target_columns = sorted(target_columns & helper_columns)
            needs_shop_id_alias = (
                "Shop_id" in target_columns
                and "Shop_id" not in primary_table.columns
                and "Shop_id" not in helper_columns
                and "Store_id" in helper_columns
            )
            if len(direct_target_columns) < 2 and not needs_shop_id_alias:
                continue

            if needs_shop_id_alias:
                helper_scaffold = (
                    f"- {helper_table.name}_named = {helper_table.name}_base.rename(columns={{'Store_id': 'Shop_id'}})\n"
                    f"- {helper_table.name}_join_ready = {helper_table.name}_named.copy()\n"
                    f"- {helper_table.name}_join_ready['Join_key'] = 1"
                )
            else:
                helper_scaffold = (
                    f"- {helper_table.name}_join_ready = {helper_table.name}_base.copy()\n"
                    f"- {helper_table.name}_join_ready['Join_key'] = 1"
                )

            return (
                "Detected a single-row metadata-table pattern.\n"
                f"- {helper_table.name} has one row and should be broadcast across every row of {primary_table.name}.\n"
                "- Do not join on Shop_id or another metadata key before that key exists in the main dataframe.\n"
                "- Instead, add a constant Join_key = 1 to copied versions of both dataframes and merge on Join_key.\n"
                "Normalizer-safe broadcast skeleton example:\n"
                f"- {primary_table.name}_base = {primary_table.name}.copy()\n"
                f"- {primary_table.name}_join_ready = {primary_table.name}_base.copy()\n"
                f"- {primary_table.name}_join_ready['Join_key'] = 1\n"
                f"- {helper_table.name}_base = {helper_table.name}.copy()\n"
                f"{helper_scaffold}\n"
                f"- broadcast_joined = {primary_table.name}_join_ready.merge({helper_table.name}_join_ready, how='left', left_on=['Join_key'], right_on=['Join_key'], suffixes=('_left', '_right'))\n"
                "- After the broadcast join, aggregate to the target grain and keep only the target-ready columns."
            )

        return ""

    def _should_keep_child(self, child: SearchNode) -> bool:
        signature = json.dumps(
            {
                "action_type": child.action_type.value,
                "schema_mapping": child.state.schema_mapping,
                "operator_plan": child.state.operator_plan,
                "pipeline_spec": child.state.pipeline_spec.model_dump() if child.state.pipeline_spec else None,
            },
            sort_keys=True,
            ensure_ascii=False,
            default=str,
        )
        if signature in self.state_cache:
            return False
        self.state_cache.add(signature)
        return True

    def _reset_search_state(self) -> None:
        self.reward_cache.clear()
        self.validation_cache.clear()
        self.action_cache.clear()
        self.state_cache.clear()
        self.invalid_candidate_errors = []

    def _remember_invalid_candidate(self, message: str) -> None:
        if not message:
            return
        if message not in self.invalid_candidate_errors:
            self.invalid_candidate_errors.append(message)

    def _invalid_generation_error(self) -> str:
        if self.invalid_candidate_errors:
            return self.invalid_candidate_errors[0]
        return "BAT did not produce a valid executable pipeline."

    def _validation_failure_message(self, payload: dict[str, Any]) -> str:
        warnings = payload.get("warnings") or []
        if warnings:
            return str(warnings[0])
        return "the candidate did not pass execution validation."
