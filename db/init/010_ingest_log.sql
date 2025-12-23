CREATE TABLE IF NOT EXISTS ingest_file_log (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    path TEXT NOT NULL,
    mtime BIGINT NOT NULL,
    size BIGINT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source, path, mtime, size)
);
