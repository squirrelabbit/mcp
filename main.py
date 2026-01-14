import os
from pathlib import Path


DATA_DIR = Path("data")
LEGACY_DATA_DIR = Path(".data")
DATA_SOURCES = [DATA_DIR, LEGACY_DATA_DIR]
SALES_PATTERNS = ["JN_SALES_AGE_YM_*.CSV"]
TELCO_PATTERNS = ["DJ_SKT_SGG_SX_STY_DAY_CNT_*.csv"]
TELCO_GRID_PATTERNS = ["GJ_SKT_SERVICE_SEX_AGE_PCELL_POP_*.csv"]
SUNGNAM_PATTERNS = ["sungnam_*.csv", "sungnam_*.CSV"]


def main():
    mode = os.getenv("MCP_MODE", "db_ingest").lower()
    if mode == "db_ingest":
        from services.db_ingest import DBIngestor
        from services.admin_loader import (
            load_sig_admin,
            update_dim_spatial_from_admin,
            update_dim_spatial_label_full,
        )
        from services.admin_geo_backfill import backfill as vworld_backfill
        from services.lake_writer import LakeWriter
        from services.emd_mapper import update_dim_spatial_with_emd

        dsn = os.getenv("MCP_DB_DSN", "postgresql://mcp:mcp@localhost:5432/mcp")
        granularity = os.getenv("MCP_GOLD_GRANULARITY", "month").lower()
        lake_target = os.getenv("MCP_LAKE_TARGET")
        lake_writer = None
        if lake_target:
            lake_writer = LakeWriter(
                target=lake_target,
                root=os.getenv("MCP_LAKE_ROOT", "lake"),
                format=os.getenv("MCP_LAKE_FORMAT", "parquet"),
                s3_endpoint=os.getenv("MCP_LAKE_S3_ENDPOINT"),
                s3_access_key=os.getenv("MCP_LAKE_S3_ACCESS_KEY"),
                s3_secret_key=os.getenv("MCP_LAKE_S3_SECRET_KEY"),
                s3_bucket=os.getenv("MCP_LAKE_S3_BUCKET"),
            )
        ingestor = DBIngestor(dsn, granularity=granularity, lake_writer=lake_writer)
        ingestor.ingest_all(
            DATA_SOURCES,
            SALES_PATTERNS,
            TELCO_PATTERNS,
            TELCO_GRID_PATTERNS,
            SUNGNAM_PATTERNS,
        )
        ingestor.close()
        sig_shapefile = os.getenv("MCP_SIG_SHAPEFILE")
        if sig_shapefile:
            sig_name_field = os.getenv("MCP_SIG_NAME_FIELD", "A2")
            sig_code_field = os.getenv("MCP_SIG_CODE_FIELD", "A1")
            sig_epsg_raw = os.getenv("MCP_SIG_EPSG")
            sig_epsg = int(sig_epsg_raw) if sig_epsg_raw else None
            sig_encoding = os.getenv("MCP_SIG_ENCODING")
            loaded = load_sig_admin(
                dsn,
                sig_shapefile,
                name_field=sig_name_field,
                code_field=sig_code_field,
                epsg=sig_epsg,
                encoding=sig_encoding,
            )
            print(f"[batch] sig admin loaded {loaded} rows")
            updated_spatial = update_dim_spatial_from_admin(dsn)
            print(f"[batch] sig mapping updated {updated_spatial} rows")
        shapefile_path = os.getenv("MCP_EMD_SHAPEFILE")
        if shapefile_path:
            name_field = os.getenv("MCP_EMD_NAME_FIELD")
            code_field = os.getenv("MCP_EMD_CODE_FIELD")
            epsg_raw = os.getenv("MCP_EMD_EPSG")
            epsg = int(epsg_raw) if epsg_raw else None
            updated = update_dim_spatial_with_emd(
                dsn,
                shapefile_path,
                name_field=name_field,
                code_field=code_field,
                epsg=epsg,
            )
            print(f"[batch] emd mapping updated {updated} rows")
        vworld_key = os.getenv("MCP_VWORLD_KEY")
        vworld_domain = os.getenv("MCP_VWORLD_DOMAIN")
        if vworld_key and vworld_domain:
            vworld_limit = int(os.getenv("MCP_VWORLD_LIMIT", "5000"))
            vworld_sleep = float(os.getenv("MCP_VWORLD_SLEEP", "0.2"))
            vworld_backfill(vworld_limit, vworld_sleep)
        updated_label = update_dim_spatial_label_full(dsn)
        print(f"[batch] spatial_label updated {updated_label} rows")
        print("[batch] db ingest complete")
        return
    if mode == "refresh_advanced_insights":
        try:
            import psycopg2
        except ImportError as exc:
            raise SystemExit(
                "psycopg2가 필요합니다. `pip install psycopg2-binary` 후 다시 실행하세요."
            ) from exc

        dsn = os.getenv("MCP_DB_DSN", "postgresql://mcp:mcp@localhost:5432/mcp")
        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("REFRESH MATERIALIZED VIEW mv_insight_advanced;")
        print("[batch] mv_insight_advanced refreshed")
        return
    raise SystemExit("MCP_MODE must be one of: db_ingest, refresh_advanced_insights.")


if __name__ == "__main__":
    main()
