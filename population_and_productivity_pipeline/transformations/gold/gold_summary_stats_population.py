# Population Gold tables

# Primary implementation (PySpark) for analytical question 1:
# "What are the mean and standard deviation of the annual US population
# across 2013-2018 inclusive?"

# PySpark is the primary implementation feeding Gold.
# A separate Spark SQL alternative is provided in:
# 04_gold_population_sql_alternative.sql

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utilities.config import SILVER_SCHEMA, GOLD_SCHEMA

# Configuration
POPULATION_SILVER_TABLE = f"{SILVER_SCHEMA}.fct_population"
POPULATION_BY_YEAR_TABLE = f"{GOLD_SCHEMA}.population_by_year"
POPULATION_SUMMARY_TABLE = f"{GOLD_SCHEMA}.population_summary_stats"
SUMMARY_START_YEAR = 2013
SUMMARY_END_YEAR = 2018

# reusable yearly population gold table
@dp.materialized_view(
    name=POPULATION_BY_YEAR_TABLE,
    comment=(
        "Annual US population for every year available in the latest "
        "validated population snapshot."
    ),
)
def population_by_year() -> DataFrame:
    # Keep all available years in Gold so the dataset remains reusable beyond
    # the specific 2013-2018 validation question in the assignment.
    return (
        spark.read.table(POPULATION_SILVER_TABLE)
        .select(
            "nation_id",
            "nation",
            "year",
            "population",
        )
    )


# one-row answer for the required 2013-2018 metric gold table
@dp.materialized_view(
    name=POPULATION_SUMMARY_TABLE,
    comment=(
        "One-row summary answering the assignment's 2013-2018 population "
        "mean and standard deviation question."
    ),
)
def population_summary() -> DataFrame:
    # The assignment asks for the statistics only across 2013-2018 inclusive,
    # while population_by_year intentionally retains every available year.
    population_2013_2018_df = (
        spark.read.table(POPULATION_BY_YEAR_TABLE)
        .where(F.col("year").between(SUMMARY_START_YEAR, SUMMARY_END_YEAR))
    )

    return (
        population_2013_2018_df.agg(
            F.max("nation_id").alias("nation_id"),
            F.max("nation").alias("nation"),
            F.avg("population").alias("mean_population"),
            F.stddev("population").alias("stddev_population"),
        )
        .withColumn("start_year", F.lit(SUMMARY_START_YEAR))
        .withColumn("end_year", F.lit(SUMMARY_END_YEAR))
        .select(
            "nation_id",
            "nation",
            "start_year",
            "end_year",
            "mean_population",
            "stddev_population",
        )
    )
