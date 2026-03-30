from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from app.models import DataType, SourceTableSpec, TargetFieldSpec


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def normalize_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


RAW_SYNONYM_GROUPS: dict[str, set[str]] = {
    "shop_id": {"shopid", "shop_id", "storeid", "store_id", "branchid", "branch_id"},
    "date": {"date", "sale_date", "transactiondate", "transaction_date", "rawdate", "day"},
    "product_category": {"productcategory", "product_category", "category", "product", "itemcategory"},
    "sales": {"sales", "netsales", "amount", "revenue", "value", "load"},
    "region": {"region", "store_region", "market", "zone"},
    "total_store_sales": {"totalstoresales", "total_store_sales", "dailytotal", "daily_total", "storetotal"},
}


SYNONYM_GROUPS: dict[str, set[str]] = {
    group_name: {normalize_name(item) for item in items}
    for group_name, items in RAW_SYNONYM_GROUPS.items()
}


def stem_name(filename: str) -> str:
    return Path(filename).stem.replace(" ", "_")


def infer_dtype(series: pd.Series) -> DataType:
    if pd.api.types.is_bool_dtype(series):
        return DataType.BOOLEAN
    if pd.api.types.is_integer_dtype(series):
        return DataType.INTEGER
    if pd.api.types.is_float_dtype(series):
        return DataType.FLOAT
    if pd.api.types.is_datetime64_any_dtype(series):
        return DataType.DATETIME

    non_null = series.dropna()
    if non_null.empty:
        return DataType.STRING

    sample = non_null.astype(str).head(10).tolist()
    date_hits = 0
    for value in sample:
        try:
            pd.to_datetime(value)
            date_hits += 1
        except Exception:
            continue
    if sample and date_hits >= max(1, len(sample) - 2):
        return DataType.DATE
    return DataType.STRING


def json_safe_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        if value.hour == 0 and value.minute == 0 and value.second == 0 and value.microsecond == 0 and value.nanosecond == 0:
            return value.date().isoformat()
        return value.isoformat()
    if value is pd.NaT:
        return None
    if hasattr(value, "item"):
        try:
            return json_safe_value(value.item())
        except Exception:
            pass
    if isinstance(value, dict):
        return {str(key): json_safe_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe_value(item) for item in value]
    return value


def guess_preview_rows(df: pd.DataFrame, limit: int = 5) -> list[dict[str, Any]]:
    preview = df.head(limit).copy()
    preview = preview.where(pd.notnull(preview), None)
    return [json_safe_value(row) for row in preview.to_dict(orient="records")]


def source_signature(columns: list[str]) -> dict[str, str]:
    return {normalize_name(col): col for col in columns}


def similar_fields(a: str, b: str) -> bool:
    na = normalize_name(a)
    nb = normalize_name(b)
    if na == nb:
        return True
    for group in SYNONYM_GROUPS.values():
        if na in group and nb in group:
            return True
    return False


def choose_main_table(target_schema: list[TargetFieldSpec], table_columns: dict[str, list[str]]) -> str:
    target_names = [normalize_name(field.name) for field in target_schema]
    best_score = -1
    best_table = next(iter(table_columns))
    for table_name, columns in table_columns.items():
        score = 0
        normalized_cols = {normalize_name(c) for c in columns}
        for target in target_names:
            if target in normalized_cols:
                score += 2
            else:
                for group in SYNONYM_GROUPS.values():
                    if target in group and normalized_cols.intersection(group):
                        score += 1
                        break
        if any(normalize_name(c) in SYNONYM_GROUPS["sales"] for c in columns):
            score += 2
        if score > best_score:
            best_score = score
            best_table = table_name
    return best_table


def detect_join_keys(left_columns: list[str], right_columns: list[str]) -> list[tuple[str, str]]:
    keys: list[tuple[str, str]] = []
    for left in left_columns:
        for right in right_columns:
            if similar_fields(left, right):
                keys.append((left, right))
    preferred = []
    for left, right in keys:
        normalized = normalize_name(left)
        if normalized in SYNONYM_GROUPS["shop_id"] or normalized in SYNONYM_GROUPS["date"]:
            preferred.append((left, right))
    return preferred or keys[:2]


def target_mode_from_samples(samples: list[dict[str, Any]]) -> str:
    return "sample_assisted" if samples else "metadata_only"


def safe_type_name(value: Any) -> str:
    if value is None:
        return "None"
    return type(value).__name__


def source_alias(index: int) -> str:
    return f"test_{index}"


def runtime_source_name_map(source_tables: list[SourceTableSpec]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for index, table in enumerate(source_tables):
        alias = source_alias(index)
        mapping[alias] = table.path
        mapping[table.name] = table.path
    return mapping
