from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from io import StringIO

from app.models import TargetFieldSpec
from app.target_catalog import build_target_database, get_target_table


@dataclass(slots=True)
class PresetDefinition:
    id: str
    name: str
    description: str
    source_files: dict[str, str]
    target_table_id: str
    target_table_description: str | None
    clear_all_target_rows: bool
    target_schema: list[TargetFieldSpec]
    target_samples: list[dict[str, object]]


STORE_PROFILE_ROW = {
    "Store_id": "S001",
    "Region": "East",
    "Store_format": "Urban",
    "Open_date": "2019-08-15",
}


def _csv_from_rows(fieldnames: list[str], rows: list[dict[str, object]]) -> str:
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().rstrip("\n")


def _store_transactions_rows() -> list[dict[str, object]]:
    day_entries = [
        ("04/10/2024", [("P100", "InStore", 12), ("P101", "InStore", 7), ("P102", "Delivery", 15), ("P103", "Mobile", 9), ("P104", "InStore", 11), ("P105", "Delivery", 13), ("P106", "InStore", 12)]),
        ("04/11/2024", [("P107", "Mobile", 14), ("P108", "InStore", 10), ("P109", "Delivery", 8), ("P110", "InStore", 16), ("P111", "Mobile", 9), ("P112", "InStore", 11), ("P113", "Delivery", 13)]),
        ("04/12/2024", [("P100", "InStore", 11), ("P102", "Delivery", 12), ("P104", "Mobile", 9), ("P106", "InStore", 14), ("P108", "Delivery", 10), ("P110", "InStore", 13), ("P112", "Mobile", 11)]),
        ("04/13/2024", [("P101", "InStore", 15), ("P103", "Delivery", 12), ("P105", "Mobile", 11), ("P107", "InStore", 10), ("P109", "Delivery", 14), ("P111", "Mobile", 9), ("P113", "InStore", 17)]),
        ("04/14/2024", [("P100", "Delivery", 10), ("P103", "InStore", 8), ("P106", "Mobile", 12), ("P109", "InStore", 11), ("P112", "Delivery", 9), ("P104", "InStore", 13), ("P107", "Mobile", 13)]),
        ("04/15/2024", [("P102", "InStore", 13), ("P105", "Delivery", 12), ("P108", "Mobile", 10), ("P111", "InStore", 14), ("P100", "Delivery", 9), ("P113", "InStore", 11), ("P106", "Mobile", 15)]),
        ("04/16/2024", [("P101", "InStore", 16), ("P104", "Delivery", 11), ("P107", "Mobile", 13), ("P110", "InStore", 12), ("P113", "Delivery", 10), ("P102", "InStore", 14), ("P109", "Mobile", 15)]),
    ]
    rows: list[dict[str, object]] = []
    receipt_number = 1101
    for day, entries in day_entries:
        for product_id, channel, net_sales in entries:
            rows.append(
                {
                    "Transaction_date": day,
                    "Product_id": product_id,
                    "Channel": channel,
                    "Net_sales": net_sales,
                    "Receipt_id": f"R{receipt_number}",
                }
            )
            receipt_number += 1
    return rows


def _store_product_catalog_rows() -> list[dict[str, object]]:
    return [
        {"Product_id": "P100", "Product_name": "Sparkling Water", "Product_category": "Beverages"},
        {"Product_id": "P101", "Product_name": "Blueberry Muffin", "Product_category": "Bakery"},
        {"Product_id": "P102", "Product_name": "Frozen Dumplings", "Product_category": "Frozen"},
        {"Product_id": "P103", "Product_name": "Pocket Notebook", "Product_category": "Stationery"},
        {"Product_id": "P104", "Product_name": "Granola Bar", "Product_category": "Snacks"},
        {"Product_id": "P105", "Product_name": "Dish Soap", "Product_category": "Household"},
        {"Product_id": "P106", "Product_name": "Greek Yogurt", "Product_category": "Dairy"},
        {"Product_id": "P107", "Product_name": "Orange Juice", "Product_category": "Beverages"},
        {"Product_id": "P108", "Product_name": "Sourdough Roll", "Product_category": "Bakery"},
        {"Product_id": "P109", "Product_name": "Ice Cream Pint", "Product_category": "Frozen"},
        {"Product_id": "P110", "Product_name": "Gel Pen Set", "Product_category": "Stationery"},
        {"Product_id": "P111", "Product_name": "Trail Mix", "Product_category": "Snacks"},
        {"Product_id": "P112", "Product_name": "Paper Towels", "Product_category": "Household"},
        {"Product_id": "P113", "Product_name": "Cheddar Cheese", "Product_category": "Dairy"},
    ]


def _store_staff_schedule_rows() -> list[dict[str, object]]:
    assignments = [
        ("2024-04-10", "Ava", "08:00", "16:00"),
        ("2024-04-10", "Noah", "12:00", "20:00"),
        ("2024-04-11", "Mia", "08:00", "16:00"),
        ("2024-04-11", "Liam", "12:00", "20:00"),
        ("2024-04-12", "Ella", "08:00", "16:00"),
        ("2024-04-12", "Ethan", "12:00", "20:00"),
        ("2024-04-13", "Grace", "09:00", "17:00"),
        ("2024-04-13", "Lucas", "13:00", "21:00"),
        ("2024-04-14", "Chloe", "09:00", "17:00"),
        ("2024-04-14", "Mason", "13:00", "21:00"),
        ("2024-04-15", "Zoe", "08:00", "16:00"),
        ("2024-04-15", "Owen", "12:00", "20:00"),
        ("2024-04-16", "Ruby", "08:00", "16:00"),
        ("2024-04-16", "Levi", "12:00", "20:00"),
    ]
    return [
        {
            "Store_id": STORE_PROFILE_ROW["Store_id"],
            "Work_date": work_date,
            "Associate_name": associate_name,
            "Shift_start": shift_start,
            "Shift_end": shift_end,
        }
        for work_date, associate_name, shift_start, shift_end in assignments
    ]


def _store_sales_metrics_rows() -> list[dict[str, object]]:
    day_entries = [
        ("2024/04/10", [(48, 43, 40), (37, 33, 33), (41, 36, 32)]),
        ("2024/04/11", [(52, 46, 43), (39, 35, 35), (40, 36, 31)]),
        ("2024/04/12", [(50, 45, 42), (36, 32, 32), (42, 37, 34)]),
        ("2024/04/13", [(54, 48, 44), (38, 34, 34), (44, 39, 35)]),
        ("2024/04/14", [(46, 41, 38), (35, 31, 31), (43, 38, 34)]),
        ("2024/04/15", [(55, 49, 45), (41, 36, 36), (44, 39, 35)]),
        ("2024/04/16", [(51, 46, 42), (39, 35, 35), (43, 38, 34)]),
    ]
    rows: list[dict[str, object]] = []
    receipt_number = 3001
    for day, entries in day_entries:
        for gross_sales, net_sales, paid_sales in entries:
            rows.append(
                {
                    "Store_id": STORE_PROFILE_ROW["Store_id"],
                    "Transaction_date": day,
                    "Receipt_id": f"R{receipt_number}",
                    "Gross_sales": gross_sales,
                    "Net_sales": net_sales,
                    "Paid_sales": paid_sales,
                }
            )
            receipt_number += 1
    return rows


def _daily_total_rows_from_transactions() -> list[dict[str, object]]:
    totals_by_day: dict[str, int] = {}
    for row in _store_transactions_rows():
        normalized_date = datetime.strptime(str(row["Transaction_date"]), "%m/%d/%Y").strftime("%Y-%m-%d")
        totals_by_day[normalized_date] = totals_by_day.get(normalized_date, 0) + int(row["Net_sales"])
    return [
        {
            "Shop_id": "S001",
            "Date": day,
            "Region": "East",
            "Store_format": "Urban",
            "Total_store_sales": total,
        }
        for day, total in sorted(totals_by_day.items())
    ]


def _store_demo_source_files() -> dict[str, str]:
    return {
        "store_transactions.csv": _csv_from_rows(
            ["Transaction_date", "Product_id", "Channel", "Net_sales", "Receipt_id"],
            _store_transactions_rows(),
        ),
        "store_product_catalog.csv": _csv_from_rows(
            ["Product_id", "Product_name", "Product_category"],
            _store_product_catalog_rows(),
        ),
        "store_profile.csv": _csv_from_rows(
            ["Store_id", "Region", "Store_format", "Open_date"],
            [STORE_PROFILE_ROW],
        ),
        "store_staff_schedule.csv": _csv_from_rows(
            ["Store_id", "Work_date", "Associate_name", "Shift_start", "Shift_end"],
            _store_staff_schedule_rows(),
        ),
    }


def _issue_correction_source_files() -> dict[str, str]:
    return {
        "store_sales_metrics.csv": _csv_from_rows(
            ["Store_id", "Transaction_date", "Receipt_id", "Gross_sales", "Net_sales", "Paid_sales"],
            _store_sales_metrics_rows(),
        ),
        "store_profile.csv": _csv_from_rows(
            ["Store_id", "Region", "Store_format", "Open_date"],
            [STORE_PROFILE_ROW],
        ),
    }


def _target_fixture(table_id: str) -> tuple[list[TargetFieldSpec], list[dict[str, object]]]:
    table = get_target_table(build_target_database(), table_id)
    if table is None:
        raise ValueError(f"Unknown target table fixture: {table_id}")
    schema = [TargetFieldSpec.model_validate(field.model_dump()) for field in table.schema]
    samples = [dict(row) for row in table.existing_rows]
    return schema, samples


def _preset_definition(
    preset_id: str,
    name: str,
    description: str,
    target_table_id: str,
    target_table_description: str | None = None,
    *,
    include_samples: bool = True,
    clear_all_target_rows: bool = False,
    source_files: dict[str, str] | None = None,
    target_samples_override: list[dict[str, object]] | None = None,
) -> PresetDefinition:
    target_schema, target_samples = _target_fixture(target_table_id)
    if target_samples_override is not None:
        target_samples = [dict(row) for row in target_samples_override]
    if not include_samples:
        target_samples = []
    return PresetDefinition(
        id=preset_id,
        name=name,
        description=description,
        source_files=source_files or _store_demo_source_files(),
        target_table_id=target_table_id,
        target_table_description=target_table_description,
        clear_all_target_rows=clear_all_target_rows,
        target_schema=target_schema,
        target_samples=target_samples,
    )


PRESETS: dict[str, PresetDefinition] = {
    "fex_example": _preset_definition(
        preset_id="fex_example",
        name="Instance-visible",
        description="A single-store retail upload example with sample target rows for the warehouse store-day totals table.",
        target_table_id="store_daily_totals",
        target_table_description="Warehouse store-day sales totals for one retail store, with existing rows available as guidance.",
        target_samples_override=_daily_total_rows_from_transactions(),
    ),
    "metadata_only": _preset_definition(
        preset_id="metadata_only",
        name="Metadata-only",
        description="The same retail upload example without target rows, using only source tables and the target schema.",
        target_table_id="store_daily_totals",
        target_table_description="Warehouse store-day sales totals for one retail store, without any existing target rows.",
        include_samples=False,
        clear_all_target_rows=True,
        target_samples_override=_daily_total_rows_from_transactions(),
    ),
    "sales_metric_ambiguity": _preset_definition(
        preset_id="sales_metric_ambiguity",
        name="Issue correction",
        description="A metadata-only retail example where several sales metrics remain plausible and the pipeline needs human confirmation.",
        target_table_id="store_daily_sales_report",
        target_table_description="Store-day warehouse sales report for one retail store, created without target sample rows.",
        include_samples=False,
        clear_all_target_rows=True,
        source_files=_issue_correction_source_files(),
    ),
}


def list_presets() -> list[dict[str, str]]:
    return [{"id": preset.id, "name": preset.name, "description": preset.description} for preset in PRESETS.values()]
