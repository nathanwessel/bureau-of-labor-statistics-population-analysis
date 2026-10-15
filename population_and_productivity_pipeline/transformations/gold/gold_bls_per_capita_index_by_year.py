# BLS Per-Capita Index - gold table

# Exploratory question beyond the assignment:
# "Has the US economy grown faster than its population?"
#
# BLS publishes volume measures such as employment, hours worked, and real
# output as indexes (e.g. "Index (2017=100)"), never as per-person levels.
# This table rebases annual US population to the same base year as each
# index so the two can be compared directly:
#
#   population_index = population(year) / population(base_year) * 100
#   per_capita_index = measure_index / population_index * 100
#
# A per_capita_index above 100 means the measure has grown faster than the
# population since the base year (e.g. more real output per US resident).

# Project rules / assumptions:
# - Only index series are used. Percent-change series cannot be rebased.
# - The index base year is parsed from duration_text ("Index (2017=100)"),
#   so a future BLS rebase is picked up without a code change.
# - Only volume measures are scaled by population. Ratios (productivity,
#   hourly compensation, labor share) and prices are excluded because
#   dividing them by population has no economic meaning.
# - The annual measure_index prefers Q05, the BLS annual average. If Q05 is
#   not published (e.g. the in-progress year), the mean of the available
#   quarters is used and flagged through annual_value_method/quarter_count.
#   This intentionally differs from bls_series_best_year, which sums quarters
#   because question 2 defines the yearly value as the quarterly sum.
# - Only years with population are kept (inner join), because a per-capita
#   value cannot exist without a population.
# - Year-over-year change is only calculated between consecutive years, so
#   a gap in population data (ACS published no 2020 1-year estimate) does
#   not silently produce a two-year change labelled as one year.

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utilities.config import SILVER_SCHEMA, GOLD_SCHEMA


# Configuration

# silver fact table
BLS_PRODUCTIVITY_FACT_TABLE = f"{SILVER_SCHEMA}.fct_bls_pr_data_1_alldata"

# silver dim tables
BLS_SERIES_TABLE = f"{SILVER_SCHEMA}.dim_bls_pr_series"
BLS_SECTOR_TABLE = f"{SILVER_SCHEMA}.dim_bls_pr_sector"
BLS_CLASS_TABLE = f"{SILVER_SCHEMA}.dim_bls_pr_class"
BLS_MEASURE_TABLE = f"{SILVER_SCHEMA}.dim_bls_pr_measure"
BLS_DURATION_TABLE = f"{SILVER_SCHEMA}.dim_bls_pr_duration"
BLS_SEASONAL_TABLE = f"{SILVER_SCHEMA}.dim_bls_pr_seasonal"

# reusable annual population gold table
POPULATION_BY_YEAR_TABLE = f"{GOLD_SCHEMA}.population_by_year"

# gold table to create
BLS_PER_CAPITA_INDEX_TABLE = f"{GOLD_SCHEMA}.bls_per_capita_index_by_year"

QUARTER_PERIODS = ["Q01", "Q02", "Q03", "Q04"]
# Q5 represents the average for the whole year
ANNUAL_PERIOD = "Q05"

# dim_bls_pr_measure.measure_text values that are economy-wide volumes and
# therefore meaningful to express per US resident
POPULATION_SCALED_MEASURES = [
    "Employment",
    "Hours worked",
    "Real value-added output",
    "Real sectoral output",
]

# "Index (2017=100)" -> "2017"
INDEX_BASE_YEAR_PATTERN = r"^Index \((\d{4})=100\)$"


# Series selection
def select_population_scaled_index_series() -> DataFrame:
    # Keep only index series for the configured volume measures, with enough
    # descriptive text for a reader who does not know BLS series codes.
    series_df = spark.read.table(BLS_SERIES_TABLE).alias("series")
    sector_df = spark.read.table(BLS_SECTOR_TABLE).alias("sector")
    class_dim_df = spark.read.table(BLS_CLASS_TABLE).alias("class_dim")
    measure_df = spark.read.table(BLS_MEASURE_TABLE).alias("measure")
    duration_df = spark.read.table(BLS_DURATION_TABLE).alias("duration")
    seasonal_df = spark.read.table(BLS_SEASONAL_TABLE).alias("seasonal")

    return (
        series_df
        # measure and duration drive the filter, so they are inner joins
        .join(
            measure_df,
            F.col("series.measure_code") == F.col("measure.measure_code"),
            "inner",
        )
        .join(
            duration_df,
            F.col("series.duration_code") == F.col("duration.duration_code"),
            "inner",
        )
        .join(
            sector_df,
            F.col("series.sector_code") == F.col("sector.sector_code"),
            "left",
        )
        .join(
            class_dim_df,
            F.col("series.class_code") == F.col("class_dim.class_code"),
            "left",
        )
        .join(
            seasonal_df,
            F.col("series.seasonal_code") == F.col("seasonal.seasonal_code"),
            "left",
        )
        .where(F.col("measure.measure_text").isin(POPULATION_SCALED_MEASURES))
        .where(F.col("duration.duration_text").startswith("Index"))
        .select(
            F.col("series.series_id").alias("series_id"),
            F.col("sector.sector_name").alias("sector_name"),
            F.col("measure.measure_text").alias("measure_text"),
            F.col("class_dim.class_text").alias("class_text"),
            F.col("seasonal.seasonal_text").alias("seasonal_text"),
            F.col("duration.duration_text").alias("duration_text"),
            # regexp_extract returns "" on no match; keep that as null so the
            # pipeline expectation can catch an unrecognized index format
            F.nullif(
                F.regexp_extract(
                    F.col("duration.duration_text"),
                    INDEX_BASE_YEAR_PATTERN,
                    1,
                ),
                F.lit(""),
            )
            .cast("int")
            .alias("index_base_year"),
        )
    )


# Year-level calculation
def aggregate_annual_index_values(series_df: DataFrame) -> DataFrame:
    # Collapse each selected series/year to one annual index value.
    fct_productivity_series_all_data_df = (
        spark.read.table(BLS_PRODUCTIVITY_FACT_TABLE)
        .join(series_df.select("series_id"), "series_id", "left_semi")
    )

    has_q05 = F.col("_q05_value").isNotNull()
    has_quarters = F.col("quarter_count") > 0

    return (
        fct_productivity_series_all_data_df
        .groupBy("series_id", "year")
        .agg(
            # the published BLS annual average
            F.max(
                F.when(F.col("period") == ANNUAL_PERIOD, F.col("value"))
            ).alias("_q05_value"),
            # fallback when Q05 is not yet published
            F.avg(
                F.when(F.col("period").isin(QUARTER_PERIODS), F.col("value"))
            ).alias("_quarterly_mean"),
            # count the number of quarters
            F.countDistinct(
                F.when(F.col("period").isin(QUARTER_PERIODS), F.col("period"))
            ).alias("quarter_count"),
        )
        .withColumn(
            "measure_index",
            F.when(has_q05, F.col("_q05_value"))
            .when(has_quarters, F.col("_quarterly_mean"))
            .cast("double"),
        )
        # Record how measure_index was calculated so Gold output
        # remains easy to interpret and audit.
        .withColumn(
            "annual_value_method",
            F.when(has_q05, F.lit("annual_average_q05"))
            .when(has_quarters, F.lit("mean_available_quarters")),
        )
        .join(series_df, "series_id", "inner")
    )


# Population rebasing
def add_population_index(df: DataFrame) -> DataFrame:
    # Rebase population to each series' own index base year so both sides of
    # the per-capita ratio are expressed on the same scale.
    population_df = (
        spark.read.table(POPULATION_BY_YEAR_TABLE)
        .select("year", "population")
    )

    base_year_population_df = population_df.select(
        F.col("year").alias("index_base_year"),
        F.col("population").alias("_base_year_population"),
    )

    population_index = (
        F.col("population") / F.col("_base_year_population") * 100
    )

    return (
        df
        .join(population_df, "year", "inner")
        .join(base_year_population_df, "index_base_year", "left")
        .withColumn("population_index", population_index)
        .withColumn(
            "per_capita_index",
            F.col("measure_index") / F.col("population_index") * 100,
        )
        .withColumn(
            "grew_faster_than_population",
            F.col("measure_index") > F.col("population_index"),
        )
    )


def add_year_over_year_change(df: DataFrame) -> DataFrame:
    # Only compare against the immediately preceding calendar year.
    series_window = Window.partitionBy("series_id").orderBy("year")

    previous_year = F.lag("year").over(series_window)
    previous_per_capita_index = F.lag("per_capita_index").over(series_window)

    return df.withColumn(
        "per_capita_index_yoy_pct_change",
        F.when(
            previous_year == F.col("year") - 1,
            (F.col("per_capita_index") / previous_per_capita_index - 1) * 100,
        ),
    )


@dp.temporary_view(
    name="bls_per_capita_index_validated",
    comment=(
        "Annual BLS volume indexes joined to US population rebased to the "
        "same index base year."
    ),
)
@dp.expect_or_fail(
    "index_base_year_recognized",
    "index_base_year IS NOT NULL",
)
@dp.expect_or_fail(
    "usable_annual_index_value",
    "measure_index IS NOT NULL",
)
@dp.expect(
    "population_available_for_index_base_year",
    "_base_year_population IS NOT NULL",
)
@dp.expect(
    "full_year_of_index_values",
    "annual_value_method = 'annual_average_q05'",
)
def bls_per_capita_index_validated() -> DataFrame:
    return (
        aggregate_annual_index_values(select_population_scaled_index_series())
        .transform(add_population_index)
        .transform(add_year_over_year_change)
    )


# Gold output
@dp.materialized_view(
    name=BLS_PER_CAPITA_INDEX_TABLE,
    comment=(
        "Annual BLS employment, hours worked, and real output indexes "
        "expressed per US resident by rebasing population to the same index "
        "base year. per_capita_index > 100 means the measure grew faster "
        "than the population since the base year."
    ),
)
def bls_per_capita_index_by_year() -> DataFrame:
    # BLS stores indexes to three decimal places; match that precision.
    return (
        spark.read.table("bls_per_capita_index_validated")
        .select(
            "series_id",
            "sector_name",
            "measure_text",
            "class_text",
            "seasonal_text",
            "index_base_year",

            "year",
            "annual_value_method",
            "quarter_count",

            F.round("measure_index", 3).alias("measure_index"),
            "population",
            F.round("population_index", 3).alias("population_index"),
            F.round("per_capita_index", 3).alias("per_capita_index"),
            F.round("per_capita_index_yoy_pct_change", 2)
            .alias("per_capita_index_yoy_pct_change"),
            "grew_faster_than_population",
        )
        .orderBy("series_id", "year")
    )
