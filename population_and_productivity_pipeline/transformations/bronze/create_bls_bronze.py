# BLS Bronze
#
# notes:
# - each current BLS source file becomes its own Bronze materialized view
# - new files are discovered automatically on the next pipeline update
#
# Intentionally ignored:
# - _directory_listing.html
# - pr.contacts
# - pr.txt

import os
import re

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

from utilities.config import BRONZE_SCHEMA

# Configuration
BLS_SOURCE_PATH = (
    "/Volumes/rearc/bronze/raw_source_data/"
    "bls/productivity/"
)

IGNORED_FILES = [
    "_directory_listing.html",
    "pr.contacts",
    "pr.txt",
]

# BLS source contracts
#
# BLS documents these files as tab-delimited text with a header row
# Keep all Bronze fields as strings; do type conversion in Silver

def string_schema(*column_names: str) -> StructType:
    return StructType(
        [
            StructField(column_name, StringType(), True)
            for column_name in column_names
        ]
    )

# see: https://download.bls.gov/pub/time.series/pr/pr.txt
BLS_CONTRACTS = {
    "pr.class": {
        "schema": string_schema(
            "class_code",
            "class_text",
            "display_level",
            "selectable",
            "sort_sequence",
        ),
        "keys": ["class_code"],
    },
    "pr.data.0.Current": {
        "schema": string_schema(
            "series_id",
            "year",
            "period",
            "value",
            "footnote_codes",
        ),
        "keys": ["series_id", "year", "period"],
    },
    "pr.data.1.AllData": {
        "schema": string_schema(
            "series_id",
            "year",
            "period",
            "value",
            "footnote_codes",
        ),
        "keys": ["series_id", "year", "period"],
    },
    "pr.duration": {
        "schema": string_schema(
            "duration_code",
            "duration_text",
            "display_level",
            "selectable",
            "sort_sequence",
        ),
        "keys": ["duration_code"],
    },
    "pr.footnote": {
        "schema": string_schema(
            "footnote_code",
            "footnote_text",
        ),
        "keys": ["footnote_code"],
    },
    "pr.measure": {
        "schema": string_schema(
            "measure_code",
            "measure_text",
            "display_level",
            "selectable",
            "sort_sequence",
        ),
        "keys": ["measure_code"],
    },
    "pr.period": {
        "schema": string_schema(
            "period",
            "period_abbr",
            "period_name",
        ),
        "keys": ["period"],
    },
    "pr.seasonal": {
        "schema": string_schema(
            "seasonal_code",
            "seasonal_text",
        ),
        "keys": ["seasonal_code"],
    },
    "pr.sector": {
        "schema": string_schema(
            "sector_code",
            "sector_name",
            "display_level",
            "selectable",
            "sort_sequence",
        ),
        "keys": ["sector_code"],
    },
    "pr.series": {
        "schema": string_schema(
            "series_id",
            "sector_code",
            "class_code",
            "measure_code",
            "duration_code",
            "seasonal",
            "base_year",
            "footnote_codes",
            "begin_year",
            "begin_period",
            "end_year",
            "end_period",
        ),
        "keys": ["series_id"],
    },
}

# Source discovery:
def discover_bls_files() -> list[str]:
    # Discover files at pipeline planning time so future BLS files can become
    # Bronze datasets without requiring another hardcoded table definition.
    return sorted(
        file_name for file_name in os.listdir(BLS_SOURCE_PATH)
        if file_name not in IGNORED_FILES
        and os.path.isfile(os.path.join(BLS_SOURCE_PATH, file_name))
    )

def bronze_table_name(source_file: str) -> str:
    # Normalize arbitrary future BLS filenames into safe Unity Catalog names
    # and make the BLS source system explicit in every Bronze table name.
    normalized_name = re.sub(
        r"[^a-z0-9]+",
        "_",
        source_file.lower(),
    ).strip("_")

    return f"{BRONZE_SCHEMA}.bls_{normalized_name}"


# Bronze transformation helpers:
def read_tab_delimited_file(source_file: str) -> DataFrame:
    source_path = os.path.join(BLS_SOURCE_PATH, source_file)

    # The Volume is the lossless raw layer. Bronze normalizes only whitespace
    # while intentionally leaving every business field as a string.
    df = (
        spark.read
        .option("header", "true")
        .option("sep", "\t")
        .option("inferSchema", "false")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .csv(source_path)
    )

    # Normalize header whitespace because the BLS ASCII files use padded fields.
    # also normalize capitalization to all lowercase for consistency
    return df.toDF(
        *[column_name.strip().lower() for column_name in df.columns]
    )


def add_quality_flags(df: DataFrame, source_file: str) -> DataFrame:
    # get the contract for the given source file from BLS
    contract = BLS_CONTRACTS.get(source_file)

    # Unknown future files are still exposed automatically, but are visibly
    # flagged until an explicit BLS schema/key contract is added here.
    if contract is None:
        return (
            df
            .withColumn("_expected_schema_valid", F.lit(False))
            .withColumn("_non_null_key_valid", F.lit(False))
        )

    expected_columns = contract["schema"].fieldNames()
    key_columns = contract["keys"]

    actual_columns = df.columns
    schema_is_valid = actual_columns == expected_columns

    key_is_valid = F.lit(True)

    for key_column in key_columns:
        if key_column not in actual_columns:
            key_is_valid = F.lit(False)
            break

        key_is_valid = (
            key_is_valid
            & F.col(key_column).isNotNull()
            & (F.trim(F.col(key_column)) != "")
        )

    return (
        df.withColumn("_expected_schema_valid", F.lit(schema_is_valid)) \
        .withColumn("_non_null_key_valid", key_is_valid)
    )

def add_bronze_metadata(df: DataFrame, source_file: str) -> DataFrame:
    # Preserve enough lineage to trace every Bronze row back to the current
    # authoritative file in the governed Volume.
    return (
        df
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_source_path", F.lit(os.path.join(BLS_SOURCE_PATH, source_file)))
        .withColumn("_bronze_refreshed_at_utc", F.current_timestamp())
    )

# Dynamic Bronze dataset factory (for every file present after ingestion pipeline run)
def create_bls_bronze_table(source_file: str) -> None:
    table_name = bronze_table_name(source_file)

    # Databricks recommends @dp.materialized_view for datasets defined from batch sources
    @dp.materialized_view(
        name=table_name,
        comment=(
            f"Current-state Bronze representation of BLS source file "
            f"{source_file}. All source fields are retained as strings."
        ),
    )
    @dp.expect(
        "expected_schema",
        "_expected_schema_valid",
    )
    @dp.expect(
        "non_null_key",
        "_non_null_key_valid",
    )
    def bls_bronze() -> DataFrame:
        return (
            read_tab_delimited_file(source_file)
            .transform(lambda df: add_quality_flags(df, source_file))
            .transform(lambda df: add_bronze_metadata(df, source_file))
        )


# Register one Bronze dataset per current BLS source file
# The loop runs during pipeline planning. Independent Bronze datasets can then
# refresh in parallel when the pipeline executes.
for bls_source_file in discover_bls_files():
    create_bls_bronze_table(bls_source_file)