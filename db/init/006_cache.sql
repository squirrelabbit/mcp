CREATE TABLE IF NOT EXISTS query_cache (
    cache_key TEXT PRIMARY KEY,
    sql_text TEXT NOT NULL,
    params_json JSONB,
    response_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
