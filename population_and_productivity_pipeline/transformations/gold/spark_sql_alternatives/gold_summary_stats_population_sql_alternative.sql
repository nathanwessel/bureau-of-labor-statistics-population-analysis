-- Population Gold - Spark SQL Alternative
--
-- Alternative implementation for analytical question 1:
-- "What are the mean and standard deviation of the annual US population
-- across 2013-2018 inclusive?"
--
-- The PySpark implementation remains the primary version feeding:
-- rearc.gold.population_summary

CREATE OR REFRESH MATERIALIZED VIEW rearc.gold.population_summary_sql_alternative
COMMENT "Spark SQL alternative for the 2013-2018 US population mean and standard deviation"
AS
SELECT
    nation_id as nation_id,
    nation as nation,
    2013 AS start_year,
    2018 AS end_year,
    AVG(population) AS mean_population,
    STDDEV(population) AS stddev_population,
    COUNT(*) AS year_count
FROM rearc.silver.fct_population
WHERE year BETWEEN 2013 AND 2018
GROUP BY 
    nation_id,
    nation,
    start_year,
    end_year;