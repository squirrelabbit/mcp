class PostgresWriter:
    def __init__(self, dsn: str) -> None:
        try:
            import psycopg2
            from psycopg2 import extras
        except ImportError as exc:
            raise RuntimeError(
                "psycopg2가 필요합니다. `pip install psycopg2-binary` 후 다시 실행하세요."
            ) from exc
        self._psycopg2 = psycopg2
        self._extras = extras
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = True
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS insights (
                    id BIGSERIAL PRIMARY KEY,
                    generated_at TIMESTAMPTZ NOT NULL,
                    spatial TEXT,
                    time TEXT,
                    source TEXT,
                    payload JSONB NOT NULL
                );
                """
            )

    def write_insights(self, payload: dict) -> None:
        generated_at = payload.get("generated_at")
        insights = payload.get("insights", [])
        if not insights:
            return
        rows = [
            (
                generated_at,
                record.get("spatial"),
                record.get("time"),
                record.get("source"),
                self._extras.Json(record),
            )
            for record in insights
        ]
        with self._conn.cursor() as cur:
            self._extras.execute_values(
                cur,
                """
                INSERT INTO insights (generated_at, spatial, time, source, payload)
                VALUES %s
                """,
                rows,
            )

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
