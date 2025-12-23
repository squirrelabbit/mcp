CREATE OR REPLACE VIEW v_activity_monthly_trend_norm AS
WITH mapped AS (
    SELECT
        a.*,
        d1.spatial_label AS label_by_key,
        d1.spatial_type AS type_by_key,
        d2.spatial_label AS label_by_label,
        d2.spatial_type AS type_by_label
    FROM gold_activity a
    LEFT JOIN dim_spatial d1 ON d1.spatial_key = a.spatial_key
    LEFT JOIN dim_spatial d2 ON d2.spatial_label = a.spatial_key
)
SELECT
    COALESCE(label_by_key, label_by_label, spatial_key) AS spatial_label,
    COALESCE(type_by_key, type_by_label) AS spatial_type,
    date,
    SUM(foot_traffic) AS foot_traffic,
    SUM(sales) AS sales,
    SUM(sales_count) AS sales_count
FROM mapped
WHERE granularity = 'month'
GROUP BY spatial_label, spatial_type, date;

CREATE OR REPLACE VIEW v_activity_monthly_trend_emd AS
SELECT *
FROM v_activity_monthly_trend_norm
WHERE spatial_type = 'emd';
