from __future__ import annotations

import json

from app.models import Session


NORMALIZER_COMPATIBILITY_RULES = """
You must return code that is compatible with a strict pipeline normalizer.

Hard requirements:
- Use the real source DataFrame names exactly as provided below.
- Do not invent placeholder names such as test_0, test_1, df, left_df, right_df, source_df, or target_df.
- Treat every real source DataFrame as read-only.
- Every table-level transformation must assign to a fresh dataframe variable name.
- The only allowed multi-line scaffold is:
  1. fresh_df = previous_df.copy()
  2. one or more fresh_df["column"] = ... statements
- Never mutate a dataframe that was created by rename, merge, groupby, concat, melt, or pivot. If you need to update columns after one of those steps, first create a new .copy() dataframe.
- Do not reassign a source dataframe name.
- Do not reuse the same dataframe variable as the output of multiple table-level steps.
- The last dataframe assignment must be to final_output, and final_output must be assigned exactly once.
- Do not add imports, helper functions, file I/O, print statements, comments, markdown, or explanatory prose.
- Each code array item must be exactly one complete Python statement with no embedded line breaks.

Allowed table-step patterns:
- fresh_df = source_df.copy()
- renamed_df = input_df.rename(columns={...})
- joined_df = left_df.merge(right_df, how='left', left_on=[...], right_on=[...], suffixes=('_left', '_right'))
- grouped_df = input_df.groupby([...], dropna=False).agg(**{'OutputColumn': ('SourceColumn', 'sum')}).reset_index()
- union_df = pd.concat([...], ignore_index=True)
- melted_df = input_df.melt(...)
- pivot_df = input_df.pivot_table(...).reset_index()
- final_output = input_df[[col for col in [... ] if col in input_df.columns]].copy()

Allowed column-update pattern:
- dated_df = renamed_df.copy()
- dated_df['Date'] = pd.to_datetime(dated_df['Date'], errors='coerce', format='%Y/%m/%d').dt.strftime('%Y-%m-%d')

Forbidden patterns:
- renamed_df['Date'] = ... immediately after renamed_df = input_df.rename(...)
- merged_df['Region'] = ... immediately after merged_df = left_df.merge(...)
- df = df.rename(...)
- final_output = final_output.rename(...)
- grouped_df = input_df.groupby(...).agg({'Sales': 'sum'}).reset_index()
- grouped_df = input_df.groupby(...).agg(**{'Total_sales': 'sum'}).reset_index()
- grouped_df = input_df.groupby(... )['Sales'].sum().reset_index(name='Total_sales')

When grouping rows, you must use named aggregation tuples exactly like:
- grouped_df = input_df.groupby(['Shop_id', 'Date'], dropna=False).agg(**{'Total_store_sales': ('Daily_sales', 'sum')}).reset_index()

When selecting the final columns, rename fields before the final_output line. Do not rename columns after final_output is created.
""".strip()


STRICT_JSON_RULE = """
Return JSON only, inside one ```json``` block, with no extra commentary before or after it.
""".strip()


def _prompt_footer(table_schema_dict: str, hint: str) -> str:
    return f"""
Table Schema:
{table_schema_dict}

Hint:
{hint}
""".strip()


def build_table_schema_dict(session: Session) -> str:
    source_tables = []
    for table in session.source_tables:
        caption = f"**DataFrame Name:** {table.name}"
        columns = "**Columns:**\n" + "\n".join(f"- {column}" for column in table.columns)
        rows = []
        for index, row in enumerate(table.preview_rows[:3], start=1):
            row_str = " | ".join(str(row.get(column, "")) for column in table.columns)
            rows.append(f"{index}. | {row_str} |")
        rows_str = "**Preview Rows:**\n" + "\n".join(rows) if rows else "**Preview Rows:**\n- No preview rows provided"
        source_tables.append(
            f"{caption}\n**Original File:** {table.filename}\n{columns}\n{rows_str}".strip()
        )
    source_tables_block = "\n".join(source_tables)

    target_columns = "**Columns:**\n" + "\n".join(f"- {field.name}" for field in session.target_schema)
    target_table = f"**DataFrame Name:** target\n{target_columns}"
    target_description = "\n".join(
        f"- {field.name}: {field.description or 'No description provided'}"
        for field in session.target_schema
    )
    sample_block = ""
    if session.target_samples:
        sample_block = "\nTarget Sample Rows:\n" + json.dumps(
            session.target_samples[:3], ensure_ascii=False, indent=2, default=str
        )

    return (
        f"Source Tables:\n{source_tables_block}\n\n"
        f"Target Table:\n{target_table}\n"
        f"Target Data Description:\n{target_description}{sample_block}"
    )


def get_schema_match_prompt(table_schema_dict: str, hint: str) -> str:
    return f"""
You are a meticulous schema matcher for data preparation workflows.

Analyze the source tables and the target table, then identify how each target column maps back to source columns.
The target table may be derived using join, union, groupby, pivot, unpivot, rename, column arithmetic, date formatting, add_columns, or drop_columns.

Return your result as JSON in the format below:

```json
[
  {{
    "target_column": "target_column_name",
    "sources": {{
      "source_table_name": ["column_a", "column_b"]
    }}
  }}
]
```

Do not explain your answer outside the JSON block.
If a target column is derived from multiple source columns, include all of them.
Use the real source table names exactly as provided.

{STRICT_JSON_RULE}

{_prompt_footer(table_schema_dict, hint)}
"""


def get_identify_function_prompt(table_schema_dict: str, hint: str) -> str:
    return f"""
You're an assistant that identifies which transformation functions are likely needed to turn the source tables into the target table.

The available functions are:
- join
- union
- groupby
- pivot
- unpivot
- rename
- column arithmetic
- date formatting
- adding columns
- dropping columns

Example:
Source Tables:
**DataFrame Name:** employees
**Columns:**
- EmployeeID
- Name
- DepartmentID
**Preview Rows:**
1. | 1 | Alice | 101 |
2. | 2 | Bob | 102 |
**DataFrame Name:** departments
**Columns:**
- DepartmentID
- DepartmentName
**Preview Rows:**
1. | 101 | Finance |
2. | 102 | Marketing |
Target Table:
**DataFrame Name:** target
**Columns:**
- EmployeeID
- EmployeeName
- DepartmentName

Answer:
The likely functions are:

Rename:
Rename Name to EmployeeName in employees.

Join:
Join employees with departments on DepartmentID to bring DepartmentName into the final output.

Now answer the real question in the same style.

Keep the answer concise and operational. Prefer a valid step order such as rename -> date formatting -> groupby -> join -> keep columns.

{_prompt_footer(table_schema_dict, hint)}

Answer:
"""


def get_transformation_prompt(table_schema_dict: str, hint: str) -> str:
    return f"""
You are a data transformation expert specializing in Python and pandas.

Write Python code that transforms the already-loaded source DataFrames into the target table.

{NORMALIZER_COMPATIBILITY_RULES}

If the task needs rename + date formatting, you must use this exact scaffold shape:
1. renamed_df = source_df.rename(columns={{...}})
2. dated_df = renamed_df.copy()
3. dated_df['Date'] = pd.to_datetime(dated_df['Date'], errors='coerce', format='...').dt.strftime('...')

If the task needs an aggregation, you must aggregate from an existing input column into a new output column name:
- grouped_df = dated_df.groupby(['Shop_id', 'Date'], dropna=False).agg(**{{'Total_store_sales': ('Daily_sales', 'sum')}}).reset_index()

If the task needs a join, rename the join keys before the merge. Do not wait until final_output to rename columns.

Return JSON only in this shape:

```json
{{
  "chain_of_thought_reasoning": "Brief reasoning about the pipeline steps",
  "code": [
    "python statement 1",
    "python statement 2"
  ]
}}
```

{STRICT_JSON_RULE}

{_prompt_footer(table_schema_dict, hint)}
"""


def get_transformation_revision_prompt(
    table_schema_dict: str,
    hint: str,
    original_code: list[str] | str,
    error_message: str,
    exec_result: str,
) -> str:
    return f"""
You are revising a pandas transformation pipeline after a failed attempt.

Your task:
1. Read the current code, the observed execution result, and the error.
2. Produce a corrected full code listing.
3. Keep the code compatible with a structured pipeline normalizer.

{NORMALIZER_COMPATIBILITY_RULES}

The corrected code must strictly follow the normalizer-safe shapes above.
If the previous attempt used forbidden patterns, replace them with the allowed equivalents instead of preserving them.

Return JSON only in this shape:

```json
{{
  "chain_of_thought_reasoning": "Brief reasoning about the fix",
  "code": [
    "python statement 1",
    "python statement 2"
  ]
}}
```

{STRICT_JSON_RULE}

Original Code:
{original_code}

Execution Result:
{exec_result}

Error Message:
{error_message}

Table Schema:
{table_schema_dict}

Hint:
{hint}
"""
