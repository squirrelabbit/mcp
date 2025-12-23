CREATE OR REPLACE VIEW v_insight_candidate_norm AS
WITH base AS (
    SELECT
        spatial_label,
        date,
        foot_traffic,
        sales
    FROM v_activity_monthly_trend_norm
),
baseline AS (
    SELECT
        *,
        LAG(foot_traffic) OVER (PARTITION BY spatial_label ORDER BY date) AS ft_prev,
        LAG(sales) OVER (PARTITION BY spatial_label ORDER BY date) AS sales_prev,
        LAG(foot_traffic, 12) OVER (PARTITION BY spatial_label ORDER BY date) AS ft_prev_year,
        LAG(sales, 12) OVER (PARTITION BY spatial_label ORDER BY date) AS sales_prev_year,
        AVG(foot_traffic) OVER (PARTITION BY spatial_label) AS ft_avg,
        STDDEV_SAMP(foot_traffic) OVER (PARTITION BY spatial_label) AS ft_std,
        AVG(sales) OVER (PARTITION BY spatial_label) AS sales_avg,
        STDDEV_SAMP(sales) OVER (PARTITION BY spatial_label) AS sales_std,
        AVG(foot_traffic) OVER (PARTITION BY date) AS ft_avg_date,
        AVG(sales) OVER (PARTITION BY date) AS sales_avg_date,
        RANK() OVER (PARTITION BY date ORDER BY foot_traffic DESC NULLS LAST) AS ft_rank,
        RANK() OVER (PARTITION BY date ORDER BY sales DESC NULLS LAST) AS sales_rank
    FROM base
),
demo_base AS (
    SELECT
        d.spatial_label,
        g.date,
        g.sex,
        g.age_group,
        SUM(g.value) AS value
    FROM gold_demographics g
    JOIN dim_spatial d ON d.spatial_key = g.spatial_key
    WHERE g.granularity = 'month'
    GROUP BY d.spatial_label, g.date, g.sex, g.age_group
),
demo_rank AS (
    SELECT
        spatial_label,
        date,
        sex,
        age_group,
        value,
        SUM(value) OVER (PARTITION BY spatial_label, date) AS total_value,
        ROW_NUMBER() OVER (PARTITION BY spatial_label, date ORDER BY value DESC) AS rn
    FROM demo_base
)
SELECT
    b.spatial_label,
    b.date,
    b.foot_traffic,
    b.sales,
    b.ft_prev,
    CASE WHEN b.ft_prev IS NULL OR b.ft_prev = 0 THEN NULL ELSE (b.foot_traffic - b.ft_prev) / b.ft_prev END AS ft_mom_pct,
    b.ft_prev_year,
    CASE WHEN b.ft_prev_year IS NULL OR b.ft_prev_year = 0 THEN NULL ELSE (b.foot_traffic - b.ft_prev_year) / b.ft_prev_year END AS ft_yoy_pct,
    b.sales_prev,
    CASE WHEN b.sales_prev IS NULL OR b.sales_prev = 0 THEN NULL ELSE (b.sales - b.sales_prev) / b.sales_prev END AS sales_mom_pct,
    b.sales_prev_year,
    CASE WHEN b.sales_prev_year IS NULL OR b.sales_prev_year = 0 THEN NULL ELSE (b.sales - b.sales_prev_year) / b.sales_prev_year END AS sales_yoy_pct,
    b.ft_avg,
    b.sales_avg,
    b.ft_std,
    b.sales_std,
    CASE WHEN b.ft_std IS NULL OR b.ft_std = 0 THEN NULL ELSE (b.foot_traffic - b.ft_avg) / b.ft_std END AS ft_zscore,
    CASE WHEN b.sales_std IS NULL OR b.sales_std = 0 THEN NULL ELSE (b.sales - b.sales_avg) / b.sales_std END AS sales_zscore,
    b.ft_avg_date,
    b.sales_avg_date,
    b.ft_rank,
    b.sales_rank,
    d.sex || '_' || d.age_group AS dominant_group,
    CASE WHEN d.total_value = 0 THEN NULL ELSE d.value / d.total_value END AS dominant_share
FROM baseline b
LEFT JOIN demo_rank d
  ON d.spatial_label = b.spatial_label
 AND d.date = b.date
 AND d.rn = 1;

CREATE OR REPLACE VIEW v_insight_candidate_sig AS
WITH base AS (
    SELECT
        spatial_label,
        date,
        foot_traffic,
        sales
    FROM v_activity_monthly_trend_sig
),
baseline AS (
    SELECT
        *,
        LAG(foot_traffic) OVER (PARTITION BY spatial_label ORDER BY date) AS ft_prev,
        LAG(sales) OVER (PARTITION BY spatial_label ORDER BY date) AS sales_prev,
        LAG(foot_traffic, 12) OVER (PARTITION BY spatial_label ORDER BY date) AS ft_prev_year,
        LAG(sales, 12) OVER (PARTITION BY spatial_label ORDER BY date) AS sales_prev_year,
        AVG(foot_traffic) OVER (PARTITION BY spatial_label) AS ft_avg,
        STDDEV_SAMP(foot_traffic) OVER (PARTITION BY spatial_label) AS ft_std,
        AVG(sales) OVER (PARTITION BY spatial_label) AS sales_avg,
        STDDEV_SAMP(sales) OVER (PARTITION BY spatial_label) AS sales_std,
        AVG(foot_traffic) OVER (PARTITION BY date) AS ft_avg_date,
        AVG(sales) OVER (PARTITION BY date) AS sales_avg_date,
        RANK() OVER (PARTITION BY date ORDER BY foot_traffic DESC NULLS LAST) AS ft_rank,
        RANK() OVER (PARTITION BY date ORDER BY sales DESC NULLS LAST) AS sales_rank
    FROM base
)
SELECT
    spatial_label,
    date,
    foot_traffic,
    sales,
    ft_prev,
    CASE WHEN ft_prev IS NULL OR ft_prev = 0 THEN NULL ELSE (foot_traffic - ft_prev) / ft_prev END AS ft_mom_pct,
    ft_prev_year,
    CASE WHEN ft_prev_year IS NULL OR ft_prev_year = 0 THEN NULL ELSE (foot_traffic - ft_prev_year) / ft_prev_year END AS ft_yoy_pct,
    sales_prev,
    CASE WHEN sales_prev IS NULL OR sales_prev = 0 THEN NULL ELSE (sales - sales_prev) / sales_prev END AS sales_mom_pct,
    sales_prev_year,
    CASE WHEN sales_prev_year IS NULL OR sales_prev_year = 0 THEN NULL ELSE (sales - sales_prev_year) / sales_prev_year END AS sales_yoy_pct,
    ft_avg,
    sales_avg,
    ft_std,
    sales_std,
    CASE WHEN ft_std IS NULL OR ft_std = 0 THEN NULL ELSE (foot_traffic - ft_avg) / ft_std END AS ft_zscore,
    CASE WHEN sales_std IS NULL OR sales_std = 0 THEN NULL ELSE (sales - sales_avg) / sales_std END AS sales_zscore,
    ft_avg_date,
    sales_avg_date,
    ft_rank,
    sales_rank,
    NULL AS dominant_group,
    NULL AS dominant_share
FROM baseline;

CREATE OR REPLACE VIEW v_insight_candidate_sido AS
WITH base AS (
    SELECT
        spatial_label,
        date,
        foot_traffic,
        sales
    FROM v_activity_monthly_trend_sido
),
baseline AS (
    SELECT
        *,
        LAG(foot_traffic) OVER (PARTITION BY spatial_label ORDER BY date) AS ft_prev,
        LAG(sales) OVER (PARTITION BY spatial_label ORDER BY date) AS sales_prev,
        LAG(foot_traffic, 12) OVER (PARTITION BY spatial_label ORDER BY date) AS ft_prev_year,
        LAG(sales, 12) OVER (PARTITION BY spatial_label ORDER BY date) AS sales_prev_year,
        AVG(foot_traffic) OVER (PARTITION BY spatial_label) AS ft_avg,
        STDDEV_SAMP(foot_traffic) OVER (PARTITION BY spatial_label) AS ft_std,
        AVG(sales) OVER (PARTITION BY spatial_label) AS sales_avg,
        STDDEV_SAMP(sales) OVER (PARTITION BY spatial_label) AS sales_std,
        AVG(foot_traffic) OVER (PARTITION BY date) AS ft_avg_date,
        AVG(sales) OVER (PARTITION BY date) AS sales_avg_date,
        RANK() OVER (PARTITION BY date ORDER BY foot_traffic DESC NULLS LAST) AS ft_rank,
        RANK() OVER (PARTITION BY date ORDER BY sales DESC NULLS LAST) AS sales_rank
    FROM base
)
SELECT
    spatial_label,
    date,
    foot_traffic,
    sales,
    ft_prev,
    CASE WHEN ft_prev IS NULL OR ft_prev = 0 THEN NULL ELSE (foot_traffic - ft_prev) / ft_prev END AS ft_mom_pct,
    ft_prev_year,
    CASE WHEN ft_prev_year IS NULL OR ft_prev_year = 0 THEN NULL ELSE (foot_traffic - ft_prev_year) / ft_prev_year END AS ft_yoy_pct,
    sales_prev,
    CASE WHEN sales_prev IS NULL OR sales_prev = 0 THEN NULL ELSE (sales - sales_prev) / sales_prev END AS sales_mom_pct,
    sales_prev_year,
    CASE WHEN sales_prev_year IS NULL OR sales_prev_year = 0 THEN NULL ELSE (sales - sales_prev_year) / sales_prev_year END AS sales_yoy_pct,
    ft_avg,
    sales_avg,
    ft_std,
    sales_std,
    CASE WHEN ft_std IS NULL OR ft_std = 0 THEN NULL ELSE (foot_traffic - ft_avg) / ft_std END AS ft_zscore,
    CASE WHEN sales_std IS NULL OR sales_std = 0 THEN NULL ELSE (sales - sales_avg) / sales_std END AS sales_zscore,
    ft_avg_date,
    sales_avg_date,
    ft_rank,
    sales_rank,
    NULL AS dominant_group,
    NULL AS dominant_share
FROM baseline;
