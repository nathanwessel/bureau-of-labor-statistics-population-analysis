-- BLS + Population Gold - Spark SQL Alternative

-- Alternative Spark SQL implementation for analytical question 3:
--
-- For series_id = PRS30006032 and period = Q01, what was the value each year,
-- joined with that year's population where available?
--
-- The PySpark implementation remains the primary version feeding:
-- rearc.gold.bls_prs30006032_q01_population_by_year
--
-- This SQL version independently performs the required filtering and
-- BLS LEFT OUTER JOIN population logic as an SDP materialized view.

CREATE OR REFRESH MATERIALIZED VIEW
    rearc.gold.bls_prs30006032_q01_population_by_year_sql_alternative
COMMENT "Spark SQL alternative for PRS30006032 / Q01 values left-outer joined to annual US population."
AS
SELECT
    bls.series_id,
    bls.period,
    bls.year,
    bls.value,
    population.population
FROM rearc.silver.fct_bls_pr_data_1_alldata AS bls
LEFT OUTER JOIN rearc.gold.population_by_year AS population
    ON bls.year = population.year
WHERE bls.series_id = 'PRS30006032'
  AND bls.period = 'Q01';