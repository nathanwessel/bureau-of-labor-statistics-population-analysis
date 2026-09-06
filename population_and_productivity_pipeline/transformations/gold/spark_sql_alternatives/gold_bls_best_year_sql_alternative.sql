-- BLS Best Year Gold - Spark SQL Alternative
--
-- Alternative Spark SQL implementation for analytical question 2.
-- The PySpark implementation remains the primary version feeding:
-- rearc.gold.bls_series_best_year
--
-- Official BLS PR reference:
-- https://download.bls.gov/pub/time.series/pr/pr.txt
--
-- Period rule:
-- - Complete Q01-Q04 year -> sum the four quarters and ignore Q05.
-- - No quarterly observations + Q05 -> use Q05 as an annual-only value.
-- - Partial Q01-Q04 year -> fail the update.
-- - Return all years tied for the maximum yearly value.

CREATE TEMPORARY VIEW bls_series_yearly_values_q2_sql (
    CONSTRAINT recognized_period_codes_only
        EXPECT (_recognized_periods_only)
        ON VIOLATION FAIL UPDATE,
    CONSTRAINT complete_quarters_or_annual_only
        EXPECT (_complete_quarters_or_annual_only)
        ON VIOLATION FAIL UPDATE
)
COMMENT "Validated SQL yearly BLS values for analytical question 2."
AS
WITH yearly_periods AS (
    SELECT
        series_id,
        year,

        COUNT(
            -- only count valid quarters
            DISTINCT CASE
                WHEN period IN ('Q01', 'Q02', 'Q03', 'Q04')
                THEN period
            END
        ) AS _quarter_count,

        SUM(
            -- only sum valid quarters
            CASE
                WHEN period IN ('Q01', 'Q02', 'Q03', 'Q04')
                THEN value
            END
        ) AS _quarterly_sum,

        SUM(
            CASE
                WHEN period = 'Q05' THEN 1
                ELSE 0
            END
        ) AS _q05_count,

        MAX(
            CASE
                WHEN period = 'Q05'
                THEN value
            END
        ) AS _q05_value,

        SUM(
            -- sum up the number of unexpected periods in the data
            CASE
                WHEN period NOT IN ('Q01', 'Q02', 'Q03', 'Q04', 'Q05')
                THEN 1
                ELSE 0
            END
        ) AS _unexpected_period_count

    FROM rearc.silver.fct_bls_pr_data_1_alldata
    GROUP BY
        series_id,
        year
)

SELECT
    series_id,
    year,

    CASE
        WHEN _quarter_count = 4
            THEN _quarterly_sum
        WHEN _quarter_count = 0 AND _q05_count = 1
            THEN _q05_value
    END AS yearly_value,

    CASE
        WHEN _quarter_count = 4
            THEN 'sum_q01_q04'
        WHEN _quarter_count = 0 AND _q05_count = 1
            THEN 'annual_average_q05'
    END AS value_method,

    _unexpected_period_count = 0
        AS _recognized_periods_only,

    (
        _quarter_count = 4
        OR (_quarter_count = 0 AND _q05_count = 1)
    ) AS _complete_quarters_or_annual_only

FROM yearly_periods;


CREATE OR REFRESH MATERIALIZED VIEW rearc.gold.bls_series_best_year_sql_alternative (
    CONSTRAINT human_readable_label_lookup_complete
        EXPECT (_label_lookup_complete)
)
COMMENT "Spark SQL alternative for the best-year-by-BLS-series analytical question."
AS
WITH best_values AS (
    SELECT
        series_id,
        year AS best_year,
        yearly_value,
        value_method,
        MAX(yearly_value) OVER (
            PARTITION BY series_id
        ) AS _max_yearly_value
    FROM bls_series_yearly_values_q2_sql
),

best_years AS (
    SELECT
        series_id,
        best_year,
        yearly_value,
        value_method
    FROM best_values
    WHERE yearly_value = _max_yearly_value
),

labeled_series AS (
    SELECT
        series.series_id,
        sector.sector_name,
        class_dim.class_text,
        measure.measure_text,
        duration.duration_text,
        seasonal.seasonal_text,

        (
            sector.sector_name IS NOT NULL
            AND class_dim.class_text IS NOT NULL
            AND measure.measure_text IS NOT NULL
            AND duration.duration_text IS NOT NULL
            AND seasonal.seasonal_text IS NOT NULL
        ) AS _label_lookup_complete,

        CONCAT_WS(
            ' | ',
            sector.sector_name,
            measure.measure_text,
            class_dim.class_text,
            duration.duration_text,
            seasonal.seasonal_text
        ) AS human_readable_label

    FROM rearc.silver.dim_bls_pr_series AS series

    LEFT JOIN rearc.silver.dim_bls_pr_sector AS sector
        ON series.sector_code = sector.sector_code

    LEFT JOIN rearc.silver.dim_bls_pr_class AS class_dim
        ON series.class_code = class_dim.class_code

    LEFT JOIN rearc.silver.dim_bls_pr_measure AS measure
        ON series.measure_code = measure.measure_code

    LEFT JOIN rearc.silver.dim_bls_pr_duration AS duration
        ON series.duration_code = duration.duration_code

    LEFT JOIN rearc.silver.dim_bls_pr_seasonal AS seasonal
        ON series.seasonal_code = seasonal.seasonal_code
)

SELECT
    best_years.series_id,
    best_years.best_year,
    best_years.yearly_value,
    best_years.value_method,
    labeled_series.sector_name,
    labeled_series.measure_text,
    labeled_series.class_text,
    labeled_series.duration_text,
    labeled_series.seasonal_text,
    labeled_series.human_readable_label,
    labeled_series._label_lookup_complete

FROM best_years

LEFT JOIN labeled_series
    ON best_years.series_id = labeled_series.series_id;