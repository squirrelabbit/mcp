CREATE OR REPLACE VIEW v_activity_monthly_trend AS
SELECT
    spatial_key,
    date,
    SUM(foot_traffic) AS foot_traffic,
    SUM(sales) AS sales,
    SUM(sales_count) AS sales_count
FROM gold_activity
WHERE granularity = 'month'
GROUP BY spatial_key, date;

CREATE OR REPLACE VIEW v_activity_yoy AS
WITH base AS (
    SELECT
        spatial_key,
        date,
        SUM(foot_traffic) AS foot_traffic,
        SUM(sales) AS sales
    FROM gold_activity
    WHERE granularity = 'month'
    GROUP BY spatial_key, date
)
SELECT
    cur.spatial_key,
    cur.date,
    cur.foot_traffic,
    prev.foot_traffic AS foot_traffic_prev_year,
    CASE
        WHEN prev.foot_traffic IS NULL OR prev.foot_traffic = 0 THEN NULL
        ELSE (cur.foot_traffic - prev.foot_traffic) / prev.foot_traffic
    END AS foot_traffic_yoy,
    cur.sales,
    prev.sales AS sales_prev_year,
    CASE
        WHEN prev.sales IS NULL OR prev.sales = 0 THEN NULL
        ELSE (cur.sales - prev.sales) / prev.sales
    END AS sales_yoy
FROM base cur
LEFT JOIN base prev
    ON prev.spatial_key = cur.spatial_key
   AND prev.date = (cur.date - INTERVAL '1 year');

CREATE OR REPLACE VIEW v_activity_top_spatial AS
SELECT
    date,
    spatial_key,
    SUM(foot_traffic) AS foot_traffic,
    SUM(sales) AS sales
FROM gold_activity
WHERE granularity = 'month'
GROUP BY date, spatial_key;

CREATE OR REPLACE VIEW v_demographics_share AS
WITH totals AS (
    SELECT
        spatial_key,
        date,
        source,
        SUM(value) AS total_value
    FROM gold_demographics
    WHERE granularity = 'month'
    GROUP BY spatial_key, date, source
)
SELECT
    d.spatial_key,
    d.date,
    d.source,
    d.sex,
    d.age_group,
    d.value,
    CASE
        WHEN t.total_value = 0 THEN NULL
        ELSE d.value / t.total_value
    END AS share
FROM gold_demographics d
JOIN totals t
  ON t.spatial_key = d.spatial_key
 AND t.date = d.date
 AND t.source = d.source
WHERE d.granularity = 'month';
