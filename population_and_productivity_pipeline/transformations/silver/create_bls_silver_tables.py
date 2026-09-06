# BLS Silver - Spark Declarative Pipeline

# Official BLS file-format reference:
# https://download.bls.gov/pub/time.series/pr/pr.txt
# BLS documents field names, field lengths, examples, and semantics
#
# Silver remains normalized:
# - two fact tables remain separate
# - each BLS mapping/series file becomes its own dimension
# - no descriptive dimension attributes are joined into facts in Silver
# - Gold performs the joins required by each analytical question

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from decimal import Decimal

from utilities.config import BRONZE_SCHEMA, SILVER_SCHEMA

# Column helpers
STRING = StringType()
INTEGER = IntegerType()

# BLS documents data values as a 12-character field. It also states that
# indexes are stored to three decimal places and percent changes to one.
VALUE_DECIMAL = DecimalType(18, 3)


# Source-to-Silver mapping
#
# Every mapping below is explicit:
# source Bronze column -> Silver column -> Spark representation
#
# Codes remain strings even when they contain digits because they are
# identifiers, not quantities. This also preserves leading zeroes such as
# measure_code = "01".
#
# "selectable" remains a string because BLS defines the source values as
# the codes "T" and "F"; Silver does not reinterpret that.
#
# pr.series source column "seasonal" is renamed to "seasonal_code" so its
# relationship to dim_bls_pr_seasonal is explicit.


# list of Bronze-to-Silver table mappings for BLS data
# TABLE_CONFIGS is a very large mapping that is looped through at the end of this code
# to construct the BLS-related silver tables.
TABLE_CONFIGS = [
    # map bls current data from bronze to silver
    {
        "source": f"{BRONZE_SCHEMA}.bls_pr_data_0_current",
        "target": f"{SILVER_SCHEMA}.fct_bls_pr_data_0_current",
        "description": "Current year-to-date BLS productivity observations.",
        "key_columns": ["series_id", "year", "period"],
        "required_columns": ["series_id", "year", "period", "value"],
        "columns": [
            ("series_id", "series_id", STRING),
            ("year", "year", INTEGER),
            ("period", "period", STRING),
            ("value", "value", VALUE_DECIMAL),
            ("footnote_codes", "footnote_codes", STRING),
        ],
    },
    # map bls all data from bronze to silver
    {
        "source": f"{BRONZE_SCHEMA}.bls_pr_data_1_alldata",
        "target": f"{SILVER_SCHEMA}.fct_bls_pr_data_1_alldata",
        "description": "Complete historical BLS productivity observations.",
        "key_columns": ["series_id", "year", "period"],
        "required_columns": ["series_id", "year", "period", "value"],
        "columns": [
            ("series_id", "series_id", STRING),
            ("year", "year", INTEGER),
            ("period", "period", STRING),
            ("value", "value", VALUE_DECIMAL),
            ("footnote_codes", "footnote_codes", STRING),
        ],
    },
    # map bls class data from bronze to silver
    {
        "source": f"{BRONZE_SCHEMA}.bls_pr_class",
        "target": f"{SILVER_SCHEMA}.dim_bls_pr_class",
        "description": "BLS productivity class-code dimension.",
        "key_columns": ["class_code"],
        "required_columns": ["class_code"],
        "columns": [
            ("class_code", "class_code", STRING),
            ("class_text", "class_text", STRING),
            ("display_level", "display_level", INTEGER),
            ("selectable", "selectable", STRING),
            ("sort_sequence", "sort_sequence", INTEGER),
        ],
    },
    # map bls duration dimension from bronze to silver
    {
        "source": f"{BRONZE_SCHEMA}.bls_pr_duration",
        "target": f"{SILVER_SCHEMA}.dim_bls_pr_duration",
        "description": "BLS productivity duration-code dimension.",
        "key_columns": ["duration_code"],
        "required_columns": ["duration_code"],
        "columns": [
            ("duration_code", "duration_code", STRING),
            ("duration_text", "duration_text", STRING),
            ("display_level", "display_level", INTEGER),
            ("selectable", "selectable", STRING),
            ("sort_sequence", "sort_sequence", INTEGER),
        ],
    },
    # map bls footnote dimension from bronze to silver
    {
        "source": f"{BRONZE_SCHEMA}.bls_pr_footnote",
        "target": f"{SILVER_SCHEMA}.dim_bls_pr_footnote",
        "description": "BLS productivity footnote-code dimension.",
        "key_columns": ["footnote_code"],
        "required_columns": ["footnote_code"],
        "columns": [
            ("footnote_code", "footnote_code", STRING),
            ("footnote_text", "footnote_text", STRING),
        ],
    },
    # map bls measure-code dimension from bronze to silver
    {
        "source": f"{BRONZE_SCHEMA}.bls_pr_measure",
        "target": f"{SILVER_SCHEMA}.dim_bls_pr_measure",
        "description": "BLS productivity measure-code dimension.",
        "key_columns": ["measure_code"],
        "required_columns": ["measure_code"],
        "columns": [
            ("measure_code", "measure_code", STRING),
            ("measure_text", "measure_text", STRING),
            ("display_level", "display_level", INTEGER),
            ("selectable", "selectable", STRING),
            ("sort_sequence", "sort_sequence", INTEGER),
        ],
    },
    # map bls period dimension from bronze to silver
    {
        "source": f"{BRONZE_SCHEMA}.bls_pr_period",
        "target": f"{SILVER_SCHEMA}.dim_bls_pr_period",
        "description": "BLS productivity observation-period dimension.",
        "key_columns": ["period"],
        "required_columns": ["period"],
        "columns": [
            ("period", "period", STRING),
            ("period_abbr", "period_abbr", STRING),
            ("period_name", "period_name", STRING),
        ],
    },
    # map bls seasonal dimension from bronze to silver
    {
        "source": f"{BRONZE_SCHEMA}.bls_pr_seasonal",
        "target": f"{SILVER_SCHEMA}.dim_bls_pr_seasonal",
        "description": "BLS productivity seasonal-adjustment dimension.",
        "key_columns": ["seasonal_code"],
        "required_columns": ["seasonal_code"],
        "columns": [
            ("seasonal_code", "seasonal_code", STRING),
            ("seasonal_text", "seasonal_text", STRING),
        ],
    },
    # map bls sector dimension from bronze to silver
    {
        "source": f"{BRONZE_SCHEMA}.bls_pr_sector",
        "target": f"{SILVER_SCHEMA}.dim_bls_pr_sector",
        "description": "BLS productivity economic-sector dimension.",
        "key_columns": ["sector_code"],
        "required_columns": ["sector_code"],
        "columns": [
            ("sector_code", "sector_code", STRING),
            ("sector_name", "sector_name", STRING),
            ("display_level", "display_level", INTEGER),
            ("selectable", "selectable", STRING),
            ("sort_sequence", "sort_sequence", INTEGER),
        ],
    },
    # map bls series dimension from bronze to silver
    {
        "source": f"{BRONZE_SCHEMA}.bls_pr_series",
        "target": f"{SILVER_SCHEMA}.dim_bls_pr_series",
        "description": "BLS productivity series dimension and its dimensional codes.",
        "key_columns": ["series_id"],
        "required_columns": ["series_id"],
        "columns": [
            ("series_id", "series_id", STRING),
            ("sector_code", "sector_code", STRING),
            ("class_code", "class_code", STRING),
            ("measure_code", "measure_code", STRING),
            ("duration_code", "duration_code", STRING),
            ("seasonal", "seasonal_code", STRING),
            # BLS permits "-" for base_year, so it intentionally remains text.
            ("base_year", "base_year", STRING),
            ("footnote_codes", "footnote_codes", STRING),
            ("begin_year", "begin_year", INTEGER),
            ("begin_period", "begin_period", STRING),
            ("end_year", "end_year", INTEGER),
            ("end_period", "end_period", STRING),
        ],
    },
]


# Transformation helpers
def cast_and_rename_columns(df: DataFrame, column_mappings: list[tuple]) -> DataFrame:
    # Silver converts the raw string representation into analytical types while
    # keeping code fields as strings.
    selected_columns = []
    
    # Each mapping defines:
    #   1. Bronze source column name
    #   2. Silver target column name
    #   3. Silver target data type
    for source_column, target_column, target_type in column_mappings:
        # Bronze values are strings, so trim surrounding whitespace
        # before casting them to their correct Silver data type.
        transformed_value = (
            F.trim(F.col(source_column))
            .cast(target_type)
        )
        
        # rename to column name for corresponding silver table
        selected_columns.append(
            transformed_value.alias(target_column)
        )

    return df.select(
        *selected_columns,
        F.col("_source_file"),
        F.col("_source_path"),
        F.col("_bronze_refreshed_at_utc"),
    )


def add_natural_key_uniqueness_flag(df: DataFrame, key_columns: list[str]) -> DataFrame:
    # Preserve duplicate source records and make the quality problem explicit
    # instead of silently deduplicating them in Silver.
    natural_key_window = Window.partitionBy(*key_columns)

    return df.withColumn(
        "_natural_key_unique",
        F.count(F.lit(1)).over(natural_key_window) == 1,
    )


def build_expectations(required_columns: list[str]) -> dict[str, str]:
    # Required fact fields and dimension keys fail the update when missing
    # after casting. This also surfaces invalid numeric source values because
    # a failed cast becomes null.
    expectations = {
        f"{column_name}_not_null": f"{column_name} IS NOT NULL"
        for column_name in required_columns
    }

    expectations["unique_natural_key"] = "_natural_key_unique"

    return expectations


# Dataset factory:
def create_silver_table(table_config: dict) -> None:
    # build the data-quality expectations for this table
    expectations = build_expectations(
        table_config["required_columns"]
    )

    # create the materialized view in silver based on table_config
    @dp.materialized_view(
        name=table_config["target"],
        comment=table_config["description"],
    )

    # Fail the pipeline update if any required-column expectation fails.
    @dp.expect_all_or_fail(expectations)
    def silver_table() -> DataFrame:
        return (
            # Read from the configured Bronze source table.
            spark.read.table(table_config["source"])

            # Trim, cast, and rename Bronze columns into their
            # Silver analytical representation.
            .transform(
                lambda df: cast_and_rename_columns(
                    df,
                    table_config["columns"],
                )
            )

            # Add a flag identifying duplicate rows based on the
            # configured natural key for this dataset.
            .transform(
                lambda df: add_natural_key_uniqueness_flag(
                    df,
                    table_config["key_columns"],
                )
            )
        )


# Register all BLS silver facts and dimensions:
# The loop defines independent silver datasets. Because no dimensions are
# joined into the facts here, these tables can be refreshed independently.
# Gold will join only the dimensions needed for each analytical question.

# register every silver fact and dimension defined above:
for table_config in TABLE_CONFIGS:
    create_silver_table(table_config)
