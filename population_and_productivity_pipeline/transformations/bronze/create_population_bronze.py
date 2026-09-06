# Population Bronze

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

from utilities.config import BRONZE_SCHEMA

# Configuration
POPULATION_SOURCE_PATH = (
    "/Volumes/rearc/bronze/raw_source_data/"
    "data_usa_io/population/"
)

POPULATION_BRONZE_TABLE = f"{BRONZE_SCHEMA}.population_raw"

POPULATION_FILE_PATTERN = "population__fetched_*.json"
POPULATION_FILE_PREFIX = "population__fetched_"
POPULATION_FILE_SUFFIX = "_UTC.json"
POPULATION_TIMESTAMP_FORMAT = "yyyy-MM-dd__HH_mm_ssXXX"


# Source contract:
# Bronze preserves the raw JSON unchanged. This schema exists only 
# to validate that each snapshot still resembles the 
# population API contract expected by downstream transformations (in silver and gold).
POPULATION_RECORD_SCHEMA = StructType(
    [
        StructField("ID Nation", StringType()),
        StructField("Nation", StringType()),
        StructField("ID Year", LongType()),
        StructField("Year", StringType()),
        StructField("Population", LongType()),
        StructField("Slug Nation", StringType()),
    ]
)

POPULATION_RESPONSE_SCHEMA = StructType(
    [
        StructField(
            "data",
            ArrayType(POPULATION_RECORD_SCHEMA),
        )
    ]
)


# Data quality expectations:
# Snapshot-level failures stop the update because an unidentifiable or
# unorderable raw snapshot is not safe to publish to Bronze.
SNAPSHOT_EXPECTATIONS = {
    "source_file_not_null": "source_file IS NOT NULL",
    "fetched_at_utc_not_null": "fetched_at_utc IS NOT NULL",
    "raw_json_not_null": "raw_json IS NOT NULL",
}

# Payload issues are recorded without discarding the raw source artifact.
# This keeps Bronze auditable if the upstream API changes unexpectedly.
PAYLOAD_EXPECTATIONS = {
    "expected_population_schema": "payload_schema_valid",
    "population_data_not_empty": "population_data_not_empty",
}

# Transformation helpers:
def read_population_snapshots() -> DataFrame:
    """Incrementally discover immutable population snapshot files."""
    return (
        # use autoloader to only load what hasn't been loaded yet
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", POPULATION_FILE_PATTERN)
        .load(POPULATION_SOURCE_PATH)
    )

def add_snapshot_metadata(df: DataFrame) -> DataFrame:
    """Derive stable snapshot identity and source fetch time from the filename."""
    return (
        df
        .withColumn(
            "source_file", 
            F.substring_index(F.col("path"), "/", -1)
        )
        # The raw timestamp substring extracted from the filename, e.g. "2025-01-15__14_30_00"
        .withColumn(
            "fetched_at_text",
            F.substring_index(
                F.substring_index(
                    F.col("source_file"),
                    POPULATION_FILE_PREFIX,
                    -1,
                ),
                POPULATION_FILE_SUFFIX,
                1,
            ),
        )
        # Parsed UTC timestamp derived from fetched_at_text; serves as the stable snapshot ordering key
        .withColumn(
            "fetched_at_utc",
            F.to_timestamp(
                F.concat(
                    F.col("fetched_at_text"),
                    F.lit("+00:00"),
                ),
                POPULATION_TIMESTAMP_FORMAT,
            ),
        )
        # the actual JSON response
        .withColumn(
            "raw_json",
            F.decode(F.col("content"), "UTF-8"),
        )
    )


def add_payload_quality_flags(df: DataFrame) -> DataFrame:
    """Validate the API shape without replacing or mutating the raw payload."""
    return (
        df
        .withColumn(
            "_parsed_payload",
            F.from_json(
                F.col("raw_json"),
                POPULATION_RESPONSE_SCHEMA,
            ),
        )
        .withColumn(
            "payload_schema_valid",
            F.col("_parsed_payload").isNotNull()
            & F.col("_parsed_payload.data").isNotNull(),
        )
        .withColumn(
            "population_data_not_empty",
            F.size(F.col("_parsed_payload.data")) > 0,
        )
    )


def select_bronze_columns(df: DataFrame) -> DataFrame:
    """Publish only raw-source, lineage, ordering, and quality metadata."""
    return df.select(
        "source_file",
        "fetched_at_utc",
        F.current_timestamp().alias("bronze_ingested_at_utc"),
        "raw_json",
        F.col("path").alias("source_path"),
        F.col("modificationTime").alias("source_file_modified_at"),
        F.col("length").alias("source_file_size_bytes"),
        "payload_schema_valid",
        "population_data_not_empty",
    )

# Auto-Loader tracks incremental file state so only newly discovered files are processed on each update. Because the population source notebook writes a new immutable timestamped file per fetch, previously ingested files are automatically skipped during normal incremental pipeline updates — they are neither deleted from the source directory nor re-read into Bronze.
@dp.table(
    name=POPULATION_BRONZE_TABLE,
    comment=(
        "Immutable raw snapshots from the Data USA population API, "
        "with source lineage and basic payload-quality indicators."
    ),
)

# JSON payloads can be a little bit dirty,
# as bronze should capture EVERYTHING, 
# but (source_file, fetched_at_utc, raw_json) cannot be NULL though
@dp.expect_all_or_fail(SNAPSHOT_EXPECTATIONS)
@dp.expect_all(PAYLOAD_EXPECTATIONS)
def population_raw() -> DataFrame:
    return (
        read_population_snapshots()
        .transform(add_snapshot_metadata)
        .transform(add_payload_quality_flags)
        .transform(select_bronze_columns)
    )
