# BLS Best Year - gold table

# Primary implementation (PySpark) for analytical question 2:
# For every BLS series_id, find the year with the largest yearly value.

# Official BLS PR reference:
# https://download.bls.gov/pub/time.series/pr/pr.txt
# Period rule from the BLS documentation:
# - Q01-Q04 are quarterly observations.
# - Q05 is the annual average.
# - Some manufacturing series are available only as annual averages.

# Project rules / assumptions:
# - If any Q01-Q04 observations exist, yearly_value is the sum of the
#   quarterly observations that are present. Q05 is ignored.
# - If no Q01-Q04 values exist and Q05 exists, yearly_value = Q05.
# - If only some of Q01-Q04 exist, keep the year and flag the incomplete
#   quarter set with a non-failing expectation.
# - If multiple years tie for the maximum yearly value, retain every tie.

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utilities.config import SILVER_SCHEMA, GOLD_SCHEMA


# Configuration

# silver fact table
# use alldata as we must compare years across the entire history
BLS_PRODUCTIVITY_FACT_TABLE = f"{SILVER_SCHEMA}.fct_bls_pr_data_1_alldata"

# silver dim tables
BLS_SERIES_TABLE = f"{SILVER_SCHEMA}.dim_bls_pr_series"
BLS_SECTOR_TABLE = f"{SILVER_SCHEMA}.dim_bls_pr_sector"
BLS_CLASS_TABLE = f"{SILVER_SCHEMA}.dim_bls_pr_class"
BLS_MEASURE_TABLE = f"{SILVER_SCHEMA}.dim_bls_pr_measure"
BLS_DURATION_TABLE = f"{SILVER_SCHEMA}.dim_bls_pr_duration"
BLS_SEASONAL_TABLE = f"{SILVER_SCHEMA}.dim_bls_pr_seasonal"

# gold table to create
BLS_BEST_YEAR_TABLE = f"{GOLD_SCHEMA}.bls_series_best_year"

# Q5 represents the average for the whole year
QUARTER_PERIODS = ["Q01", "Q02", "Q03", "Q04"]
ALLOWED_PERIODS = QUARTER_PERIODS + ["Q05"]

# Year-level calculation
def aggregate_series_years() -> DataFrame:
    # Collapse each series/year to the information needed to distinguish a
    # complete quarterly year from a genuinely annual-only Q05 observation.
    fct_productivity_series_all_data_df = spark.read.table(BLS_PRODUCTIVITY_FACT_TABLE)

    return (
        fct_productivity_series_all_data_df
        .groupBy("series_id", "year")
        .agg(
            # count the number of quarters
            F.countDistinct(
                F.when(F.col("period").isin(QUARTER_PERIODS), F.col("period"))
            ).alias("quarter_count"),
            # sum the quarterly observations
            F.sum(
                F.when(F.col("period").isin(QUARTER_PERIODS), F.col("value"))
            ).alias("_quarterly_sum"),
            # whether or not q5 is present
            F.sum(
                F.when(F.col("period") == "Q05", F.lit(1),)\
                .otherwise(F.lit(0))
            ).alias("_q05_count"),
            # get the q5 value
            F.max(
                F.when(F.col("period") == "Q05", F.col("value"))
            ).alias("_q05_value"),
            # count the number of unexpected periods
            F.sum(
                F.when(~F.col("period").isin(ALLOWED_PERIODS), F.lit(1)).otherwise(F.lit(0))
            ).alias("_unexpected_period_count"),
        )
    )

def apply_yearly_value_rule(df: DataFrame) -> DataFrame:
    # Quarterly observations take precedence over Q05.
    # Partial-quarter years stay eligible but are visibly flagged.

    # A year has quarterly data if at least one Q01-Q04 observation exists.
    has_quarters = F.col("quarter_count") > 0

    # Treat a year as annual-only when it has no quarterly observations
    # and exactly one Q05 annual-average observation.
    annual_only_year = (
        (F.col("quarter_count") == 0)
        & (F.col("_q05_count") == 1)
    )

    return (
        df
        # Flag whether the year contains a complete set of 4 quarters
        .withColumn(
            "has_all_four_quarters",
            F.col("quarter_count") == 4,
        )

        # Used by the pipeline expectation to ensure the source contained
        # only the BLS period codes that this calculation knows how to handle.
        .withColumn(
            "_recognized_periods_only",
            F.col("_unexpected_period_count") == 0,
        )

        # A usable yearly value requires either at least one quarterly
        # observation or a valid annual-only Q05 observation.
        .withColumn(
            "_has_usable_yearly_value",
            has_quarters | annual_only_year,
        )

        # Quality flag:
        # - quarterly series should ideally contain all four quarters
        # - annual-only series should contain one Q05 value
        #
        # Partial-quarter years fail this condition but are not removed,
        # because the corresponding pipeline expectation is non-failing.
        .withColumn(
            "_all_four_quarters_or_annual_only",
            F.when(
                has_quarters,
                F.col("quarter_count") == 4,
            ).otherwise(annual_only_year),
        )

        # Calculate the value that will be used to compare years.
        # If quarterly observations exist, use their sum and ignore Q05.
        # Otherwise, for a genuinely annual-only year, use the Q05 value.
        .withColumn(
            "yearly_value",
            F.when(
                has_quarters,
                F.col("_quarterly_sum"),
            ).when(
                annual_only_year,
                F.col("_q05_value"),
            ),
        )

        # Record how yearly_value was calculated so Gold output
        # remains easy to interpret and audit.
        .withColumn(
            "value_method",
            F.when(
                has_quarters,
                F.lit("sum_available_quarters"),
            ).when(
                annual_only_year,
                F.lit("annual_average_q05"),
            ),
        )
    )

@dp.temporary_view(
    name="bls_series_yearly_values_question_2",
    comment=(
        "Validated yearly BLS values for question 2. Quarterly years sum "
        "available Q01-Q04 observations; annual-only years use Q05."
    ),
)
@dp.expect_or_fail(
    "recognized_period_codes_only",
    "_recognized_periods_only",
)
@dp.expect_or_fail(
    "usable_yearly_value",
    "_has_usable_yearly_value",
)
@dp.expect(
    "all_four_quarters_when_quarterly",
    "_all_four_quarters_or_annual_only",
)
def bls_series_yearly_values_question_2() -> DataFrame:
    return (
        aggregate_series_years()
        .transform(apply_yearly_value_rule)
    )


# Best-year selection
def select_best_years(df: DataFrame) -> DataFrame:
    # within the given series_id, take the maximum yearly value, 
    # and retain all matching years so ties are preserved
    series_window = Window.partitionBy("series_id")

    return (
        df
        .withColumn(
            "_max_yearly_value",
            F.max("yearly_value").over(series_window),
        )
        .where(
            F.col("yearly_value") == F.col("_max_yearly_value")
        )
        .select(
            "series_id",
            F.col("year").alias("best_year"),
            "yearly_value",
            "value_method",
            "quarter_count",
            "has_all_four_quarters",
        )
    )

'''
Human-readable BLS series mapping

+----------------------------+-------------------------------+-----------------------------+
| Series column              | Human-readable column         | Meaning                     |
+----------------------------+-------------------------------+-----------------------------+
| dim_bls_pr_series.         | dim_bls_pr_sector.            | Economic sector for the     |
| sector_code                | sector_name                   | observation                 |
+----------------------------+-------------------------------+-----------------------------+
| dim_bls_pr_series.         | dim_bls_pr_class.             | Employee group to which     |
| class_code                 | class_text                    | the data pertain            |
+----------------------------+-------------------------------+-----------------------------+
| dim_bls_pr_series.         | dim_bls_pr_measure.           | Specific factor being       |
| measure_code               | measure_text                  | measured                    |
+----------------------------+-------------------------------+-----------------------------+
| dim_bls_pr_series.         | dim_bls_pr_duration.          | Index or type of change     |
| duration_code              | duration_text                 | represented                 |
+----------------------------+-------------------------------+-----------------------------+
| dim_bls_pr_series.         | dim_bls_pr_seasonal.          | Seasonal-adjustment         |
| seasonal_code              | seasonal_text                 | status                      |
+----------------------------+-------------------------------+-----------------------------+

Gold joins these coded series fields to their descriptive dimension tables
so readers can understand each series without interpreting the series_id.
'''
# Gold retains each descriptive field separately and also combines them into
# human_readable_label for an at-a-glance description of the series.

def add_human_readable_series_labels(df: DataFrame) -> DataFrame:
    series_df = spark.read.table(BLS_SERIES_TABLE).alias("series")
    sector_df = spark.read.table(BLS_SECTOR_TABLE).alias("sector")
    class_dim_df = spark.read.table(BLS_CLASS_TABLE).alias("class_dim")
    measure_df = spark.read.table(BLS_MEASURE_TABLE).alias("measure")
    duration_df = spark.read.table(BLS_DURATION_TABLE).alias("duration")
    seasonal_df = spark.read.table(BLS_SEASONAL_TABLE).alias("seasonal")

    labeled_series_df = (
        series_df
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
            measure_df,
            F.col("series.measure_code") == F.col("measure.measure_code"),
            "left",
        )
        .join(
            duration_df,
            F.col("series.duration_code") == F.col("duration.duration_code"),
            "left",
        )
        .join(
            seasonal_df,
            F.col("series.seasonal_code") == F.col("seasonal.seasonal_code"),
            "left",
        )
        .select(
            F.col("series.series_id").alias("series_id"),
            F.col("sector.sector_name").alias("sector_name"),
            F.col("class_dim.class_text").alias("class_text"),
            F.col("measure.measure_text").alias("measure_text"),
            F.col("duration.duration_text").alias("duration_text"),
            F.col("seasonal.seasonal_text").alias("seasonal_text"),
        )
        .withColumn(
            "_label_lookup_complete",
            F.col("sector_name").isNotNull()
            & F.col("class_text").isNotNull()
            & F.col("measure_text").isNotNull()
            & F.col("duration_text").isNotNull()
            & F.col("seasonal_text").isNotNull(),
        )
        .withColumn(
            "human_readable_label",
            F.concat_ws(
                " | ",
                "sector_name",
                "measure_text",
                "class_text",
                "duration_text",
                "seasonal_text",
            ),
        )
    )

    return df.join(
        labeled_series_df,
        "series_id",
        "left",
    )


# Gold output
@dp.materialized_view(
    name=BLS_BEST_YEAR_TABLE,
    comment=(
        "Best year or tied best years for every BLS series, using available "
        "quarterly observations or Q05 for genuinely annual-only series."
    ),
)
@dp.expect(
    "human_readable_label_lookup_complete",
    "_label_lookup_complete",
)
def bls_series_best_year() -> DataFrame:
    # select_best_years() will return its resulting dataframe to feed into 
    # the call to add_human_readable_series_labels()
    return (
        spark.read.table("bls_series_yearly_values_question_2")
        .transform(select_best_years)
        .transform(add_human_readable_series_labels)
        .select(
            "series_id",
            "sector_name",
            "class_text",
            "seasonal_text",
            "measure_text",
            "duration_text",
            
            "value_method",
            "best_year",
            "yearly_value",
            
            # SOON: maybe get rid of these
            "human_readable_label",
            "_label_lookup_complete",
        )
        .orderBy("series_id")
    )