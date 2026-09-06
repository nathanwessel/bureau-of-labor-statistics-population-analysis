# BLS + Population Gold

# Primary implementation (PySpark) for analytical question 3:
# For series_id = PRS30006032 and period = Q01, what was the value each year,
# joined with that year's population where available?

# Design:
# - Build one reusable Gold table containing every BLS series/period/year.
# - LEFT JOIN annual population by year so all BLS observations are retained.
# - Build the assignment-specific filtered Gold table from that reusable table.

# The Spark SQL alternative is implemented separately in:
# gold_bls_population_by_year_sql_alternative.sql

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utilities.config import SILVER_SCHEMA, GOLD_SCHEMA

# Configuration:
BLS_FACT_TABLE = f"{SILVER_SCHEMA}.fct_bls_pr_data_1_alldata"
# annual population table used to join population onto BLS rows by year
POPULATION_BY_YEAR_TABLE = f"{GOLD_SCHEMA}.population_by_year"

# for question 3 (extra gold table)
BLS_SERIES_PERIOD_POPULATION_BY_YEAR_TABLE = f"{GOLD_SCHEMA}.bls_series_period_population_by_year"
# for question 3 (main table)
BLS_SERIES_PERIOD_FILTERED_POPULATION_TABLE = (
    f"{GOLD_SCHEMA}.bls_prs30006032_q01_population_by_year"
)

# series_id and period for question 3
TARGET_SERIES_ID = "PRS30006032"
TARGET_PERIOD = "Q01"

# Reusable Gold table:
@dp.materialized_view(
    name=BLS_SERIES_PERIOD_POPULATION_BY_YEAR_TABLE,
    comment=(
        "All historical BLS observations joined to annual US population "
        "by year where population is available."
    ),
)
def bls_population_by_year() -> DataFrame:
    # BLS is the primary side of the join because the assignment asks for
    # each BLS observation enriched with population where available.
    bls_series_productivity_df = (
        spark.read.table(BLS_FACT_TABLE)
        .select(
            "series_id",
            "year",
            "period",
            "value",
        )
        .alias("bls")
    )

    population_df = (
        spark.read.table(POPULATION_BY_YEAR_TABLE)
        .select(
            "year",
            "population",
        )
        .alias("population")
    )

    return (
        bls_series_productivity_df
        .join(
            population_df,
            F.col("bls.year") == F.col("population.year"),
            "left",
        )
        .select(
            F.col("bls.series_id").alias("series_id"),
            F.col("bls.period").alias("period"),
            F.col("bls.year").alias("year"),
            F.col("bls.value").alias("value"),
            F.col("population.population").alias("population"),
        )
    )


# Assignment-specific Gold table

@dp.materialized_view(
    name=BLS_SERIES_PERIOD_FILTERED_POPULATION_TABLE,
    comment=(
        "Historical values for BLS series PRS30006032 in period Q01, "
        "joined to annual US population where available."
    ),
)
def bls_prs30006032_q01_population_by_year() -> DataFrame:
    # Keep the assignment-specific result intentionally simple and derive it
    # from the reusable Gold table so the join logic is defined only once.
    return (
        spark.read.table(BLS_SERIES_PERIOD_POPULATION_BY_YEAR_TABLE)
        .where(
            (F.col("series_id") == TARGET_SERIES_ID)
            & (F.col("period") == TARGET_PERIOD)
        )
        .select(
            "series_id",
            "period",
            "year",
            "value",
            "population",
        )
    )
