# Population Silver

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

from utilities.config import BRONZE_SCHEMA, SILVER_SCHEMA

# Configuration
POPULATION_BRONZE_TABLE = f"{BRONZE_SCHEMA}.population_raw"
POPULATION_SILVER_TABLE = f"{SILVER_SCHEMA}.fct_population"

# Source contract:
# The datausa.io API used by this project contains these fields 
# in each record under the top-level "data" array:
#
# Nation ID   -> string
# Nation      -> string
# Year        -> integer
# Population  -> numeric value represented with decimal notation

# (top-level JSON objects such as annotations, page, and columns are ignored)

POPULATION_RECORD_SCHEMA = StructType(
    [
        StructField("Nation ID", StringType(), True),
        StructField("Nation", StringType(), True),
        StructField("Year", LongType(), True),
        StructField("Population", DoubleType(), True),
    ]
)

POPULATION_RESPONSE_SCHEMA = StructType(
    [
        StructField(
            "data",
            ArrayType(POPULATION_RECORD_SCHEMA),
            True,
        )
    ]
)


# Transformation helpers:
def read_latest_population_snapshot() -> DataFrame:
    # Silver uses the literal newest source snapshot from Bronze. It does not
    # silently fall back to an older snapshot if the newest payload is malformed.
    population_bronze_df = spark.read.table(POPULATION_BRONZE_TABLE)

    # your partition is the whole table in this case
    all_rows_window = Window.partitionBy()

    return (
        population_bronze_df
        .withColumn(
            "_latest_fetched_at_utc",
            F.max("fetched_at_utc").over(all_rows_window),
        )
        .where(
            F.col("fetched_at_utc") == F.col("_latest_fetched_at_utc")
        )
        .drop("_latest_fetched_at_utc")
    )

def parse_and_explode_population(df: DataFrame) -> DataFrame:
    # raw_json starts as one full API response per Bronze row.
    # _parsed_payload converts that JSON string into a structured Spark object.
    # explode_outer creates one Silver row for each record in _parsed_payload.data.
    # The original snapshot-level columns remain attached to each exploded row.
    return (
        df.withColumn("_parsed_payload",
            F.from_json(
                F.col("raw_json"),
                POPULATION_RESPONSE_SCHEMA,
            ),
        )
        .withColumn(
            "_population_record",
            F.explode_outer(F.col("_parsed_payload.data")),
        )
    )

def select_population_columns(df: DataFrame) -> DataFrame:
    # Normalize the source field names while retaining every field present in
    # each exploded population record and the source snapshot lineage.
    return df.select(
        F.col("_population_record.`Nation ID`").alias("nation_id"),
        F.col("_population_record.Nation").alias("nation"),
        F.col("_population_record.Year").alias("year"),
        F.col("_population_record.Population").alias("population"),
        F.col("source_file"),
        F.col("fetched_at_utc"),
    )

def add_uniqueness_flag(df: DataFrame) -> DataFrame:
    # Preserve unexpected duplicates so the pipeline fails visibly instead of
    # silently choosing or removing one of the source records.
    natural_key_window = Window.partitionBy(
        "nation_id",
        "year",
    )

    return df.withColumn(
        "_natural_key_unique",
        F.count(F.lit(1)).over(natural_key_window) == 1,
    )


# Silver materialized view

@dp.materialized_view(
    name=POPULATION_SILVER_TABLE,
    comment=(
        "Latest datausa.io population snapshot exploded to one typed row per "
        "nation and year, with source snapshot lineage retained."
    ),
)
@dp.expect_or_fail(
    "nation_id_not_null",
    "nation_id IS NOT NULL",
)
@dp.expect_or_fail(
    "year_not_null",
    "year IS NOT NULL",
)
@dp.expect_or_fail(
    "population_not_null",
    "population IS NOT NULL",
)
@dp.expect_or_fail(
    "unique_nation_year",
    "_natural_key_unique",
)
def population() -> DataFrame:
    return (
        read_latest_population_snapshot()
        .transform(parse_and_explode_population)
        .transform(select_population_columns)
        .transform(add_uniqueness_flag)
    )
