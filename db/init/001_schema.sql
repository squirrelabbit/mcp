CREATE TABLE IF NOT EXISTS dim_spatial (
    spatial_key TEXT PRIMARY KEY,
    spatial_label TEXT,
    spatial_type TEXT,
    code TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS gold_activity (
    spatial_key TEXT NOT NULL REFERENCES dim_spatial(spatial_key),
    date DATE NOT NULL,
    granularity TEXT NOT NULL,
    source TEXT NOT NULL,
    foot_traffic DOUBLE PRECISION,
    sales DOUBLE PRECISION,
    sales_count DOUBLE PRECISION,
    PRIMARY KEY (spatial_key, date, granularity, source)
);

CREATE TABLE IF NOT EXISTS gold_demographics (
    spatial_key TEXT NOT NULL REFERENCES dim_spatial(spatial_key),
    date DATE NOT NULL,
    granularity TEXT NOT NULL,
    source TEXT NOT NULL,
    sex TEXT NOT NULL,
    age_group TEXT NOT NULL,
    value DOUBLE PRECISION,
    PRIMARY KEY (spatial_key, date, granularity, source, sex, age_group)
);

CREATE TABLE IF NOT EXISTS dim_event (
    event_id BIGSERIAL PRIMARY KEY,
    event_name TEXT NOT NULL,
    event_type TEXT,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    center_lat DOUBLE PRECISION,
    center_lon DOUBLE PRECISION,
    radius_m INTEGER,
    metadata JSONB
);

CREATE TABLE IF NOT EXISTS dim_weather (
    weather_id BIGSERIAL PRIMARY KEY,
    weather_code TEXT NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS fact_weather (
    spatial_key TEXT NOT NULL REFERENCES dim_spatial(spatial_key),
    date DATE NOT NULL,
    weather_code TEXT,
    temperature_c DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    PRIMARY KEY (spatial_key, date)
);

CREATE TABLE IF NOT EXISTS insights_summary (
    id BIGSERIAL PRIMARY KEY,
    generated_at TIMESTAMPTZ NOT NULL,
    spatial_key TEXT,
    date DATE,
    source TEXT,
    foot_traffic DOUBLE PRECISION,
    sales DOUBLE PRECISION,
    dominant_group TEXT,
    dominant_share DOUBLE PRECISION,
    trend_direction TEXT,
    trend_slope DOUBLE PRECISION,
    correlation DOUBLE PRECISION,
    impact_score DOUBLE PRECISION,
    impact_class TEXT,
    metrics_summary JSONB,
    demographics JSONB
);

CREATE INDEX IF NOT EXISTS idx_gold_activity_date ON gold_activity (date);
CREATE INDEX IF NOT EXISTS idx_gold_activity_spatial ON gold_activity (spatial_key);
CREATE INDEX IF NOT EXISTS idx_gold_demographics_date ON gold_demographics (date);
CREATE INDEX IF NOT EXISTS idx_gold_demographics_group ON gold_demographics (sex, age_group);
CREATE INDEX IF NOT EXISTS idx_fact_weather_date ON fact_weather (date);
CREATE INDEX IF NOT EXISTS idx_event_date_range ON dim_event (start_date, end_date);
