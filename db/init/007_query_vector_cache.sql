CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS query_mapping_cache (
    request_hash TEXT PRIMARY KEY,
    request_text TEXT NOT NULL,
    parser_model TEXT NOT NULL,
    parser_template_hash TEXT NOT NULL,
    cache_version TEXT NOT NULL,
    query_json JSONB NOT NULL,
    embedding VECTOR(1024) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS query_mapping_cache_embedding_idx
    ON query_mapping_cache
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);
