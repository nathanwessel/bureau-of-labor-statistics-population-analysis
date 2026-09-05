# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Export Assignment Results
# MAGIC
# MAGIC Generates one reviewer-facing Markdown file containing the complete
# MAGIC outputs for all three assignment questions.
# MAGIC
# MAGIC Output:
# MAGIC `results/assignment_results.md`
# MAGIC
# MAGIC The Gold tables remain the source of truth. This notebook is only a
# MAGIC publishing step so the assignment answers can be reviewed directly
# MAGIC in the Git repository.

# COMMAND ----------

from pathlib import Path
from pyspark.sql import DataFrame

# COMMAND ----------

# Gold tables used to answer the assignment questions.
QUESTION_1_TABLE = "rearc.gold.population_summary_stats"
QUESTION_2_TABLE = "rearc.gold.bls_series_best_year"
QUESTION_3_TABLE = "rearc.gold.bls_prs30006032_q01_population_by_year"

# More general Gold tables that support reuse beyond the assignment-specific
# outputs.
POPULATION_BY_YEAR_TABLE = "rearc.gold.population_by_year"
BLS_SERIES_PERIOD_POPULATION_BY_YEAR_TABLE = (
    "rearc.gold.bls_series_period_population_by_year"
)


QUESTION_1 = (
    "What are the mean and standard deviation of the annual US population "
    "across 2013-2018 inclusive?"
)

QUESTION_2 = (
    "For every `series_id` in the BLS data, what's the **best year**, meaning "
    "the year with the largest sum of `value` across all its quarters? "
    "(e.g. if `PRS30006011` has values 1, 2 in 1995 and 3, 4 in 1996, its "
    "best year is 1996 with a summed value of 7.) Include enough of a "
    "human-readable label for each series that someone unfamiliar with BLS "
    "series codes could understand at a glance what's actually being measured."
)

QUESTION_3 = (
    "For `series_id = PRS30006032` and `period = Q01`, what was the `value` "
    "each year, joined with that year's `population` where available?"
)

# COMMAND ----------

def sort_if_present(df: DataFrame, preferred_columns: list[str]) -> DataFrame:
    """Sort by the requested columns that are actually present."""
    columns_to_sort = [
        column_name
        for column_name in preferred_columns
        if column_name in df.columns
    ]

    if not columns_to_sort:
        return df

    return df.orderBy(*columns_to_sort)


def markdown_cell(value) -> str:
    """Convert a Python value into a safe Markdown-table cell."""
    if value is None:
        return ""

    text = str(value)
    text = text.replace("\\", "\\\\")
    text = text.replace("|", r"\|")
    text = text.replace("\r\n", "<br>")
    text = text.replace("\n", "<br>")
    text = text.replace("\r", "<br>")

    return text


def dataframe_to_markdown(df: DataFrame) -> str:
    """
    Render a small Spark DataFrame as a Markdown table.

    The assignment outputs are intentionally small (the largest is about
    288 rows), so collecting them to the driver is appropriate here.
    """
    columns = df.columns
    rows = df.collect()

    header = "| " + " | ".join(markdown_cell(column) for column in columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"

    body = [
        "| "
        + " | ".join(markdown_cell(row[column]) for column in columns)
        + " |"
        for row in rows
    ]

    return "\n".join([header, separator, *body])


def repo_root_from_notebook() -> Path:
    """
    Resolve the repository root when this notebook lives either at the repo
    root or inside a top-level `notebooks/` folder.
    """
    current_directory = Path.cwd()

    if current_directory.name == "notebooks":
        return current_directory.parent

    return current_directory

# COMMAND ----------

question_1_df = spark.table(QUESTION_1_TABLE)

question_2_df = sort_if_present(
    spark.table(QUESTION_2_TABLE),
    ["series_id", "best_year", "year"],
)

question_3_df = sort_if_present(
    spark.table(QUESTION_3_TABLE),
    ["year"],
)

# COMMAND ----------

assignment_results_markdown = f"""# Assignment Results

This document contains the complete outputs for all three analytical questions
in the assignment.

The results are generated from Gold tables produced by the Spark Declarative
Pipeline. The Gold tables remain the source of truth; this file is a representation of those results for the Git repository.

---

## Question 1

> {QUESTION_1}

**Gold table:** `{QUESTION_1_TABLE}`

A more general population table is also available at
`{POPULATION_BY_YEAR_TABLE}`.

{dataframe_to_markdown(question_1_df)}

---

## Question 2

> {QUESTION_2}

**Gold table:** `{QUESTION_2_TABLE}`

{dataframe_to_markdown(question_2_df)}

---

## Question 3

> {QUESTION_3}

**Gold table:** `{QUESTION_3_TABLE}`

A more general Gold table containing all BLS series and periods joined to
population (where available) by year is also available at
`{BLS_SERIES_PERIOD_POPULATION_BY_YEAR_TABLE}`.

{dataframe_to_markdown(question_3_df)}
"""

# COMMAND ----------

repo_root = repo_root_from_notebook()
results_directory = repo_root / "results"
output_path = results_directory / "assignment_results.md"

results_directory.mkdir(parents=True, exist_ok=True)
output_path.write_text(assignment_results_markdown, encoding="utf-8")

print(f"Wrote assignment results to: {output_path}")
print(f"Question 1 rows: {question_1_df.count()}")
print(f"Question 2 rows: {question_2_df.count()}")
print(f"Question 3 rows: {question_3_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional main README link
# MAGIC
# MAGIC Add this to the repository's main `README.md`:
# MAGIC
# MAGIC ```markdown
# MAGIC ## Assignment Results
# MAGIC
# MAGIC [View the outputs for all three analytical questions](results/assignment_results.md)
# MAGIC ```