CREATE OR REPLACE VIEW v_insight_candidate_all AS
SELECT
    'norm' AS level,
    spatial_label,
    date,
    foot_traffic,
    sales,
    ft_prev,
    ft_mom_pct,
    ft_prev_year,
    ft_yoy_pct,
    sales_prev,
    sales_mom_pct,
    sales_prev_year,
    sales_yoy_pct,
    ft_avg,
    sales_avg,
    ft_std,
    sales_std,
    ft_zscore,
    sales_zscore,
    ft_avg_date,
    sales_avg_date,
    ft_rank,
    sales_rank,
    dominant_group::text AS dominant_group,
    dominant_share::double precision AS dominant_share
FROM v_insight_candidate_norm
UNION ALL
SELECT
    'sig' AS level,
    spatial_label,
    date,
    foot_traffic,
    sales,
    ft_prev,
    ft_mom_pct,
    ft_prev_year,
    ft_yoy_pct,
    sales_prev,
    sales_mom_pct,
    sales_prev_year,
    sales_yoy_pct,
    ft_avg,
    sales_avg,
    ft_std,
    sales_std,
    ft_zscore,
    sales_zscore,
    ft_avg_date,
    sales_avg_date,
    ft_rank,
    sales_rank,
    dominant_group::text AS dominant_group,
    dominant_share::double precision AS dominant_share
FROM v_insight_candidate_sig
UNION ALL
SELECT
    'sido' AS level,
    spatial_label,
    date,
    foot_traffic,
    sales,
    ft_prev,
    ft_mom_pct,
    ft_prev_year,
    ft_yoy_pct,
    sales_prev,
    sales_mom_pct,
    sales_prev_year,
    sales_yoy_pct,
    ft_avg,
    sales_avg,
    ft_std,
    sales_std,
    ft_zscore,
    sales_zscore,
    ft_avg_date,
    sales_avg_date,
    ft_rank,
    sales_rank,
    dominant_group::text AS dominant_group,
    dominant_share::double precision AS dominant_share
FROM v_insight_candidate_sido;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_insight_advanced AS
SELECT
    level,
    spatial_label,
    corr(sales, foot_traffic) AS corr_sales_foot_traffic,
    regr_slope(sales, foot_traffic) AS sales_impact_slope,
    AVG(ABS(sales_zscore)) AS sales_impact_score,
    AVG(ABS(ft_zscore)) AS foot_traffic_impact_score
FROM v_insight_candidate_all
WHERE sales IS NOT NULL
  AND foot_traffic IS NOT NULL
GROUP BY level, spatial_label
WITH NO DATA;
