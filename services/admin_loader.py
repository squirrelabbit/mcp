from __future__ import annotations

import os
from typing import Dict, Optional, Tuple

import geopandas as gpd
import psycopg2
from psycopg2 import extras


SIDO_CODE_TO_NAME: Dict[str, str] = {
    "11": "서울특별시",
    "26": "부산광역시",
    "27": "대구광역시",
    "28": "인천광역시",
    "29": "광주광역시",
    "30": "대전광역시",
    "31": "울산광역시",
    "36": "세종특별자치시",
    "41": "경기도",
    "42": "강원도",
    "43": "충청북도",
    "44": "충청남도",
    "45": "전라북도",
    "46": "전라남도",
    "47": "경상북도",
    "48": "경상남도",
    "50": "제주특별자치도",
    "51": "강원특별자치도",
    "52": "전북특별자치도",
}


def load_sig_admin(
    dsn: str,
    shapefile_path: str,
    *,
    name_field: str = "A2",
    code_field: str = "A1",
    epsg: Optional[int] = None,
    encoding: Optional[str] = None,
) -> int:
    if encoding:
        shape = gpd.read_file(shapefile_path, encoding=encoding)
    else:
        shape = gpd.read_file(shapefile_path)
    if shape.crs is None:
        if epsg is None:
            raise RuntimeError("Shapefile CRS가 없습니다. MCP_SIG_EPSG로 EPSG를 지정하세요.")
        shape = shape.set_crs(epsg)

    rows = []
    for _, row in shape.iterrows():
        sig_code_raw = row.get(code_field)
        sig_name = row.get(name_field)
        if sig_code_raw is None or sig_name is None:
            continue
        sig_code = str(sig_code_raw).strip().zfill(5)
        if not sig_code.isdigit():
            continue
        sido_code = sig_code[:2]
        sido_name = SIDO_CODE_TO_NAME.get(sido_code, sido_code)
        rows.append((sig_code, str(sig_name).strip(), sido_code, sido_name))

    if not rows:
        return 0

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            extras.execute_values(
                cur,
                """
                INSERT INTO admin_sig (sig_code, sig_name, sido_code, sido_name)
                VALUES %s
                ON CONFLICT (sig_code) DO UPDATE SET
                  sig_name = EXCLUDED.sig_name,
                  sido_code = EXCLUDED.sido_code,
                  sido_name = EXCLUDED.sido_name
                """,
                rows,
            )
            extras.execute_values(
                cur,
                """
                INSERT INTO admin_sido (sido_code, sido_name)
                VALUES %s
                ON CONFLICT (sido_code) DO UPDATE SET
                  sido_name = EXCLUDED.sido_name
                """,
                list({(r[2], r[3]) for r in rows}),
            )
    return len(rows)


def update_dim_spatial_from_admin(dsn: str) -> int:
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE dim_spatial AS d SET
                    sig_code = COALESCE(d.sig_code, s.sig_code),
                    sig_name = COALESCE(d.sig_name, s.sig_name),
                    sido_code = COALESCE(d.sido_code, s.sido_code),
                    sido_name = COALESCE(d.sido_name, s.sido_name)
                FROM admin_sig s
                WHERE
                    (d.emd_code IS NOT NULL AND LEFT(d.emd_code, 5) = s.sig_code)
                    OR (d.code IS NOT NULL AND LEFT(d.code, 5) = s.sig_code)
                    OR (d.spatial_label = s.sig_name)
                """
            )
            return cur.rowcount or 0


def update_dim_spatial_label_full(dsn: str) -> int:
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE dim_spatial
                SET spatial_label = TRIM(
                    CONCAT_WS(
                        ' ',
                        NULLIF(sido_name, ''),
                        NULLIF(sig_name, ''),
                        NULLIF(emd_name, '')
                    )
                )
                WHERE (sido_name IS NOT NULL OR sig_name IS NOT NULL OR emd_name IS NOT NULL)
                """
            )
            return cur.rowcount or 0


def main() -> None:
    dsn = os.getenv("MCP_DB_DSN", "postgresql://mcp:mcp@localhost:5432/mcp")
    shapefile_path = os.getenv("MCP_SIG_SHAPEFILE")
    if not shapefile_path:
        raise SystemExit("MCP_SIG_SHAPEFILE 경로가 필요합니다.")
    name_field = os.getenv("MCP_SIG_NAME_FIELD", "A2")
    code_field = os.getenv("MCP_SIG_CODE_FIELD", "A1")
    epsg_raw = os.getenv("MCP_SIG_EPSG")
    epsg = int(epsg_raw) if epsg_raw else None
    encoding = os.getenv("MCP_SIG_ENCODING")
    updated = load_sig_admin(
        dsn,
        shapefile_path,
        name_field=name_field,
        code_field=code_field,
        epsg=epsg,
        encoding=encoding,
    )
    print(f"[admin] loaded {updated} sig rows")
    updated_spatial = update_dim_spatial_from_admin(dsn)
    print(f"[admin] updated dim_spatial {updated_spatial} rows")


if __name__ == "__main__":
    main()
