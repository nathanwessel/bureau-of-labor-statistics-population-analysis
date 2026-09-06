WITH year_components AS (
    SELECT
        series_id,
        year,

        -- Sum any true quarterly observations that exist.
        SUM(
            CASE
                WHEN period IN ('Q01', 'Q02', 'Q03', 'Q04')
                THEN value
            END
        ) AS quarterly_sum,

        COUNT(
            CASE
                WHEN period IN ('Q01', 'Q02', 'Q03', 'Q04')
                THEN 1
            END
        ) AS quarter_count,

        -- Q05 is the annual observation.
        MAX(
            CASE
                WHEN period = 'Q05'
                THEN value
            END
        ) AS q05_value

    FROM rearc.silver.fct_bls_pr_data_1_alldata
    GROUP BY
        series_id,
        year
),

annual_values AS (
    SELECT
        series_id,
        year,

        CASE
            -- If any actual quarters exist, sum the available quarters.
            WHEN quarter_count > 0
                THEN quarterly_sum

            -- Otherwise fall back to Q05.
            WHEN q05_value IS NOT NULL
                THEN q05_value
        END AS yearly_value

    FROM year_components
),

ranked AS (
    SELECT
        series_id,
        year,
        yearly_value,

        -- DENSE_RANK preserves ties.
        DENSE_RANK() OVER (
            PARTITION BY series_id
            ORDER BY yearly_value DESC
        ) AS best_year_rank

    FROM annual_values
    WHERE yearly_value IS NOT NULL
),

expected AS (
    SELECT
        series_id,
        year AS best_year,
        yearly_value
    FROM ranked
    WHERE best_year_rank = 1
),

actual AS (
    SELECT
        series_id,
        best_year,
        yearly_value
    FROM rearc.gold.bls_series_best_year
)

SELECT
    'missing_from_gold' AS mismatch_type,
    *
FROM (
    SELECT * FROM expected
    EXCEPT
    SELECT * FROM actual
)

UNION ALL

SELECT
    'unexpected_in_gold' AS mismatch_type,
    *
FROM (
    SELECT * FROM actual
    EXCEPT
    SELECT * FROM expected
);