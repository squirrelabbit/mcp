import json
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path

from domain.sales_module import SalesDomainModule
from domain.telco_module import TelcoDomainModule
from domain.telco_grid_module import TelcoGridDomainModule
from core.mcp_engine import MCPEngine
from utils.domain_loader import load_domain_records


DATA_DIR = Path("data")
LEGACY_DATA_DIR = Path(".data")
DATA_SOURCES = [DATA_DIR, LEGACY_DATA_DIR]
SALES_PATTERNS = ["JN_SALES_AGE_YM_*.CSV"]
TELCO_PATTERNS = ["DJ_SKT_SGG_SX_STY_DAY_CNT_*.csv"]
TELCO_GRID_PATTERNS = ["GJ_SKT_SERVICE_SEX_AGE_PCELL_POP_*.csv"]
OUTPUT_DIR = Path("output")
DEFAULT_RESULT_PATH = OUTPUT_DIR / "result.json"
DEFAULT_RESULT_JSONL_PATH = OUTPUT_DIR / "result.ndjson"
DEFAULT_RESULT_ROW_DIR = OUTPUT_DIR / "insights"


def _write_jsonl(target_path: Path, payload: dict) -> None:
    """Persist newline-delimited insights for easier row-wise inspection."""
    lines = []
    for record in payload.get("insights", []):
        enriched = {"generated_at": payload.get("generated_at"), **record}
        lines.append(json.dumps(enriched, ensure_ascii=False))
    target_path.write_text("\n".join(lines), encoding="utf-8")


def _write_row_files(target_dir: Path, payload: dict) -> None:
    """Persist each insight into its own JSON file."""
    records = payload.get("insights", [])
    if target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    for idx, record in enumerate(records, start=1):
        spatial = record.get("spatial") or "unknown_spatial"
        time_key = record.get("time") or "unknown_time"
        slug = _slugify(f"{spatial}_{time_key}")
        filename = f"{idx:06d}_{slug}.json"
        file_path = target_dir / filename
        file_path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")


def _slugify(value: str, max_length: int = 64) -> str:
    """Make filesystem-friendly label."""
    slug = re.sub(r"[^0-9A-Za-z_-]+", "-", value).strip("-")
    slug = slug or "record"
    if len(slug) > max_length:
        slug = slug[:max_length].rstrip("-")
    return slug.lower()


def main():
    output_target = os.getenv("MCP_OUTPUT_TARGET", "file").lower()
    save_local_raw = os.getenv("MCP_SAVE_LOCAL")
    if save_local_raw is None:
        save_local = output_target != "postgres"
    else:
        save_local = save_local_raw != "0"
    mode = os.getenv("MCP_MODE", "full").lower()
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
        ingestor.ingest_all(DATA_SOURCES, SALES_PATTERNS, TELCO_PATTERNS, TELCO_GRID_PATTERNS)
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

    # 1) Load + normalize per domain with caching
    all_records = []
    all_records.extend(
        load_domain_records("sales", SALES_PATTERNS, SalesDomainModule, DATA_SOURCES)
    )
    all_records.extend(
        load_domain_records("telco", TELCO_PATTERNS, TelcoDomainModule, DATA_SOURCES)
    )
    all_records.extend(
        load_domain_records(
            "telco_grid",
            TELCO_GRID_PATTERNS,
            TelcoGridDomainModule,
            DATA_SOURCES,
            chunked=True,
        )
    )

    if not all_records:
        raise SystemExit("Failed to normalize any records.")

    # 2) MCP pipeline
    engine = MCPEngine()
    result = engine.run(all_records)

    # 4) Persist result.json for Summarizer layer
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "insights": result["insights"],
    }
    if save_local:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        DEFAULT_RESULT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
        _write_jsonl(DEFAULT_RESULT_JSONL_PATH, payload)
        _write_row_files(DEFAULT_RESULT_ROW_DIR, payload)
        print(f"[batch] result saved to {DEFAULT_RESULT_PATH}")
        print(f"[batch] ndjson saved to {DEFAULT_RESULT_JSONL_PATH}")
        print(f"[batch] per-record insights saved under {DEFAULT_RESULT_ROW_DIR}")

    if output_target == "postgres":
        from services.postgres_writer import PostgresWriter

        dsn = os.getenv("MCP_DB_DSN", "postgresql://mcp:mcp@localhost:5432/mcp")
        writer = PostgresWriter(dsn)
        writer.write_insights(payload)
        writer.close()
        print("[batch] insights saved to postgres")

    # 5) Print example
    print("\n=== Sample Insight ===")
    print(result["insights"][:3])


if __name__ == "__main__":
    main()
