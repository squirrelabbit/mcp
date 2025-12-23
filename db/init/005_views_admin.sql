CREATE OR REPLACE VIEW v_activity_monthly_trend_sig AS
WITH base AS (
    SELECT
        a.*,
        d.spatial_label,
        d.code
    FROM gold_activity a
    LEFT JOIN dim_spatial d ON d.spatial_key = a.spatial_key
    WHERE a.granularity = 'month'
),
sig_code AS (
    SELECT
        b.*,
        s.sig_name AS sig_name_by_code
    FROM base b
    LEFT JOIN admin_sig s
        ON s.sig_code = LEFT(b.code, 5)
),
sig_name AS (
    SELECT
        b.*,
        s.sig_name AS sig_name_by_label
    FROM base b
    LEFT JOIN admin_sig s
        ON s.sig_name = b.spatial_label
)
SELECT
    COALESCE(sc.sig_name_by_code, sn.sig_name_by_label, sc.spatial_label, sc.spatial_key) AS spatial_label,
    'sig' AS spatial_type,
    sc.date,
    SUM(sc.foot_traffic) AS foot_traffic,
    SUM(sc.sales) AS sales,
    SUM(sc.sales_count) AS sales_count
FROM sig_code sc
LEFT JOIN sig_name sn
  ON sn.spatial_key = sc.spatial_key
 AND sn.date = sc.date
GROUP BY 1, 2, 3;

CREATE OR REPLACE VIEW v_activity_monthly_trend_sido AS
WITH base AS (
    SELECT
        a.*,
        d.spatial_label,
        d.code
    FROM gold_activity a
    LEFT JOIN dim_spatial d ON d.spatial_key = a.spatial_key
    WHERE a.granularity = 'month'
),
sig_code AS (
    SELECT
        b.*,
        s.sido_name AS sido_name_by_code
    FROM base b
    LEFT JOIN admin_sig s
        ON s.sig_code = LEFT(b.code, 5)
),
sig_name AS (
    SELECT
        b.*,
        s.sido_name AS sido_name_by_label
    FROM base b
    LEFT JOIN admin_sig s
        ON s.sig_name = b.spatial_label
)
SELECT
    COALESCE(sc.sido_name_by_code, sn.sido_name_by_label, sc.spatial_label, sc.spatial_key) AS spatial_label,
    'sido' AS spatial_type,
    sc.date,
    SUM(sc.foot_traffic) AS foot_traffic,
    SUM(sc.sales) AS sales,
    SUM(sc.sales_count) AS sales_count
FROM sig_code sc
LEFT JOIN sig_name sn
  ON sn.spatial_key = sc.spatial_key
 AND sn.date = sc.date
GROUP BY 1, 2, 3;
