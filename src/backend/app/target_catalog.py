from __future__ import annotations

from app.models import DataType, Session, TargetDatabaseSpec, TargetFieldSpec, TargetTableSpec


def _store_daily_totals_schema() -> list[TargetFieldSpec]:
    return [
        TargetFieldSpec(name="Shop_id", type=DataType.STRING, description="Warehouse store identifier"),
        TargetFieldSpec(name="Date", type=DataType.DATE, description="Business date in YYYY-MM-DD"),
        TargetFieldSpec(name="Region", type=DataType.STRING, description="Region assigned in the warehouse"),
        TargetFieldSpec(name="Store_format", type=DataType.STRING, description="Warehouse store format"),
        TargetFieldSpec(name="Total_store_sales", type=DataType.INTEGER, description="Total net sales for the store-day grain"),
    ]


DEFAULT_TARGET_DATABASE = TargetDatabaseSpec(
    id="builtin_demo_targets",
    name="BAT Demo Target Database",
    description="Read-only target tables used by the demo.",
    tables=[
        TargetTableSpec(
            id="store_daily_totals",
            name="store_daily_totals",
            description="Central warehouse store-day sales totals with sample rows from multiple stores.",
            schema=_store_daily_totals_schema(),
            existing_rows=[
                {
                    "Shop_id": "S001",
                    "Date": "2024-04-09",
                    "Region": "East",
                    "Store_format": "Urban",
                    "Total_store_sales": 74,
                },
                {
                    "Shop_id": "N204",
                    "Date": "2024-04-09",
                    "Region": "North",
                    "Store_format": "Neighborhood",
                    "Total_store_sales": 63,
                },
            ],
        ),
        TargetTableSpec(
            id="store_category_daily_totals",
            name="store_category_daily_totals",
            description="Central warehouse category-level store-day totals built from the same retail domain.",
            schema=[
                TargetFieldSpec(name="Shop_id", type=DataType.STRING, description="Warehouse store identifier"),
                TargetFieldSpec(name="Date", type=DataType.DATE, description="Business date in YYYY-MM-DD"),
                TargetFieldSpec(name="Product_category", type=DataType.STRING, description="Rolled-up product category"),
                TargetFieldSpec(name="Region", type=DataType.STRING, description="Region assigned in the warehouse"),
                TargetFieldSpec(name="Store_format", type=DataType.STRING, description="Warehouse store format"),
                TargetFieldSpec(name="Total_store_sales", type=DataType.INTEGER, description="Category-level store-day net sales"),
            ],
            existing_rows=[
                {
                    "Shop_id": "S001",
                    "Date": "2024-04-09",
                    "Product_category": "Beverages",
                    "Region": "East",
                    "Store_format": "Urban",
                    "Total_store_sales": 20,
                },
                {
                    "Shop_id": "W305",
                    "Date": "2024-04-09",
                    "Product_category": "Snacks",
                    "Region": "West",
                    "Store_format": "Outlet",
                    "Total_store_sales": 15,
                },
            ],
        ),
        TargetTableSpec(
            id="store_daily_sales_report",
            name="store_daily_sales_report",
            description="Central warehouse store-day sales report built from local store exports.",
            schema=[
                TargetFieldSpec(name="Shop_id", type=DataType.STRING, description="Warehouse store identifier"),
                TargetFieldSpec(name="Date", type=DataType.DATE, description="Business date in YYYY-MM-DD"),
                TargetFieldSpec(name="Region", type=DataType.STRING, description="Region assigned in the warehouse"),
                TargetFieldSpec(name="Store_format", type=DataType.STRING, description="Warehouse store format"),
                TargetFieldSpec(
                    name="Total_store_sales",
                    type=DataType.INTEGER,
                    description="Daily sales amount reported for each store and date.",
                ),
            ],
            existing_rows=[
                {
                    "Shop_id": "S110",
                    "Date": "2024-05-18",
                    "Region": "East",
                    "Store_format": "Urban",
                    "Total_store_sales": 221,
                },
                {
                    "Shop_id": "S205",
                    "Date": "2024-05-18",
                    "Region": "West",
                    "Store_format": "Outlet",
                    "Total_store_sales": 184,
                },
            ],
        ),
    ],
)


def build_empty_target_database() -> TargetDatabaseSpec:
    return TargetDatabaseSpec(
        id="session_targets",
        name="Target Database",
        description="Session-specific target tables.",
        tables=[],
    )


def build_target_database() -> TargetDatabaseSpec:
    return TargetDatabaseSpec.model_validate(DEFAULT_TARGET_DATABASE.model_dump())


def get_target_table(database: TargetDatabaseSpec, table_id: str) -> TargetTableSpec | None:
    return next((table for table in database.tables if table.id == table_id), None)


def default_target_table(database: TargetDatabaseSpec) -> TargetTableSpec:
    table = database.tables[0] if database.tables else None
    if table is None:
        raise ValueError("The target database fixture has no target tables.")
    return table


def apply_target_table(session: Session, table_id: str | None = None) -> TargetTableSpec:
    database = session.target_database or build_empty_target_database()
    session.target_database = database
    selected = get_target_table(database, table_id or session.selected_target_table_id or "") if table_id or session.selected_target_table_id else None
    table = selected or default_target_table(database)
    session.selected_target_table_id = table.id
    session.target_schema = [TargetFieldSpec.model_validate(field.model_dump()) for field in table.schema]
    session.target_samples = [dict(row) for row in table.existing_rows]
    return table
