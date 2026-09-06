-- Question 2 manual trace: Bronze -> Silver -> Gold
-- Test cases:
--   Normal quarterly: PRS30006011
--   Incomplete year:  PRS30006032
--   Q05 only:         PRS30006081
--   Tied best years:  PRS30006221


-- ============================================================
-- 1. BRONZE: roll source rows up to series/year
-- ============================================================

WITH test_cases AS (
    SELECT * FROM VALUES
        ('normal_quarterly', 'PRS30006011', 2022),
        ('incomplete_year',  'PRS30006032', 1994),
        ('incomplete_year',  'PRS30006032', 2026),
        ('q05_only',         'PRS30006081', 2021),
        ('q05_only',         'PRS30006081', 2022),
        ('tied_best_years',  'PRS30006221', 2021),
        ('tied_best_years',  'PRS30006221', 2022)
    AS t(test_case, series_id, year)
)

SELECT
    t.test_case,
    b.series_id,
    CAST(b.year AS INT) AS year,
    COUNT(CASE
        WHEN b.period IN ('Q01', 'Q02', 'Q03', 'Q04') THEN 1
    END) AS quarter_count,
    SUM(CASE
        WHEN b.period IN ('Q01', 'Q02', 'Q03', 'Q04')
        THEN CAST(b.value AS DOUBLE)
    END) AS quarterly_sum,
    MAX(CASE
        WHEN b.period = 'Q05'
        THEN CAST(b.value AS DOUBLE)
    END) AS q05_value
FROM rearc.bronze.bls_pr_data_1_alldata b
JOIN test_cases t
    ON b.series_id = t.series_id
   AND CAST(b.year AS INT) = t.year
GROUP BY
    t.test_case,
    b.series_id,
    CAST(b.year AS INT)
ORDER BY
    t.test_case,
    year;


-- ============================================================
-- 2. SILVER: same rollup; numbers should match Bronze
-- ============================================================

WITH test_cases AS (
    SELECT * FROM VALUES
        ('normal_quarterly', 'PRS30006011', 2022),
        ('incomplete_year',  'PRS30006032', 1994),
        ('incomplete_year',  'PRS30006032', 2026),
        ('q05_only',         'PRS30006081', 2021),
        ('q05_only',         'PRS30006081', 2022),
        ('tied_best_years',  'PRS30006221', 2021),
        ('tied_best_years',  'PRS30006221', 2022)
    AS t(test_case, series_id, year)
)

SELECT
    t.test_case,
    s.series_id,
    s.year,
    COUNT(CASE
        WHEN s.period IN ('Q01', 'Q02', 'Q03', 'Q04') THEN 1
    END) AS quarter_count,
    SUM(CASE
        WHEN s.period IN ('Q01', 'Q02', 'Q03', 'Q04')
        THEN s.value
    END) AS quarterly_sum,
    MAX(CASE
        WHEN s.period = 'Q05'
        THEN s.value
    END) AS q05_value
FROM rearc.silver.fct_bls_pr_data_1_alldata s
JOIN test_cases t
    ON s.series_id = t.series_id
   AND s.year = t.year
GROUP BY
    t.test_case,
    s.series_id,
    s.year
ORDER BY
    t.test_case,
    s.year;


-- ============================================================
-- 3. GOLD: inspect final winning rows
-- ============================================================

WITH test_cases AS (
    SELECT * FROM VALUES
        ('normal_quarterly', 'PRS30006011'),
        ('incomplete_year',  'PRS30006032'),
        ('q05_only',         'PRS30006081'),
        ('tied_best_years',  'PRS30006221')
    AS t(test_case, series_id)
)

SELECT
    t.test_case,
    g.series_id,
    g.value_method,
    g.best_year,
    g.yearly_value,
    g.human_readable_label
FROM rearc.gold.bls_series_best_year g
JOIN test_cases t
    ON g.series_id = t.series_id
ORDER BY
    t.test_case,
    g.best_year;

-- ============================================================
-- 4. BRONZE vs SILVER vs GOLD: compare year-level values
-- ============================================================

WITH test_cases AS (
    SELECT * FROM VALUES
        ('normal_quarterly', 'PRS30006011', 2022),
        ('incomplete_year',  'PRS30006032', 1994),
        ('incomplete_year',  'PRS30006032', 2026),
        ('q05_only',         'PRS30006081', 2021),
        ('q05_only',         'PRS30006081', 2022),
        ('tied_best_years',  'PRS30006221', 2021),
        ('tied_best_years',  'PRS30006221', 2022)
    AS t(test_case, series_id, year)
),

bronze_rollup AS (
    SELECT
        t.test_case,
        b.series_id,
        CAST(b.year AS INT) AS year,
        COUNT(CASE
            WHEN b.period IN ('Q01', 'Q02', 'Q03', 'Q04') THEN 1
        END) AS quarter_count,
        SUM(CASE
            WHEN b.period IN ('Q01', 'Q02', 'Q03', 'Q04')
            THEN CAST(b.value AS DOUBLE)
        END) AS quarterly_sum,
        MAX(CASE
            WHEN b.period = 'Q05'
            THEN CAST(b.value AS DOUBLE)
        END) AS q05_value
    FROM rearc.bronze.bls_pr_data_1_alldata b
    JOIN test_cases t
        ON b.series_id = t.series_id
       AND CAST(b.year AS INT) = t.year
    GROUP BY
        t.test_case,
        b.series_id,
        CAST(b.year AS INT)
),

bronze AS (
    SELECT
        *,
        CASE
            WHEN quarter_count = 4 THEN quarterly_sum
            WHEN quarter_count = 0 AND q05_value IS NOT NULL THEN q05_value
        END AS yearly_value
    FROM bronze_rollup
),

silver_rollup AS (
    SELECT
        t.test_case,
        s.series_id,
        s.year,
        COUNT(CASE
            WHEN s.period IN ('Q01', 'Q02', 'Q03', 'Q04') THEN 1
        END) AS quarter_count,
        SUM(CASE
            WHEN s.period IN ('Q01', 'Q02', 'Q03', 'Q04')
            THEN s.value
        END) AS quarterly_sum,
        MAX(CASE
            WHEN s.period = 'Q05'
            THEN s.value
        END) AS q05_value
    FROM rearc.silver.fct_bls_pr_data_1_alldata s
    JOIN test_cases t
        ON s.series_id = t.series_id
       AND s.year = t.year
    GROUP BY
        t.test_case,
        s.series_id,
        s.year
),

silver AS (
    SELECT
        *,
        CASE
            WHEN quarter_count = 4 THEN quarterly_sum
            WHEN quarter_count = 0 AND q05_value IS NOT NULL THEN q05_value
        END AS yearly_value
    FROM silver_rollup
),

gold AS (
    SELECT
        series_id,
        best_year AS year,
        yearly_value
    FROM rearc.gold.bls_series_best_year
    WHERE series_id IN (
        'PRS30006011',
        'PRS30006032',
        'PRS30006081',
        'PRS30006221'
    )
)

SELECT
    b.test_case,
    b.series_id,
    b.year,

    b.quarter_count AS bronze_quarter_count,
    b.yearly_value  AS bronze_yearly_value,

    s.quarter_count AS silver_quarter_count,
    s.yearly_value  AS silver_yearly_value,

    g.yearly_value  AS gold_yearly_value,

    ROUND(ABS(b.yearly_value - s.yearly_value), 3) AS bronze_vs_silver_diff,
    ROUND(ABS(s.yearly_value - g.yearly_value), 3) AS silver_vs_gold_diff,
    ROUND(ABS(b.yearly_value - g.yearly_value), 3) AS bronze_vs_gold_diff

FROM bronze b
JOIN silver s
    ON b.series_id = s.series_id
   AND b.year = s.year
LEFT JOIN gold g
    ON b.series_id = g.series_id
   AND b.year = g.year

ORDER BY
    b.test_case,
    b.year;