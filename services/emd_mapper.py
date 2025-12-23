from __future__ import annotations

import os
from typing import Optional

import geopandas as gpd
import pandas as pd
import psycopg2
from psycopg2 import extras


def update_dim_spatial_with_emd(
    dsn: str,
    shapefile_path: str,
    *,
    name_field: Optional[str] = None,
    code_field: Optional[str] = None,
    epsg: Optional[int] = None,
    encoding: Optional[str] = None,
) -> int:
    if encoding:
        shape = gpd.read_file(shapefile_path, encoding=encoding)
    else:
        shape = gpd.read_file(shapefile_path)
    if shape.crs is None:
        if epsg is None:
            raise RuntimeError("Shapefile CRS가 없습니다. MCP_EMD_EPSG로 EPSG를 지정하세요.")
        shape = shape.set_crs(epsg)
    shape = shape.to_crs(epsg=4326)

    if name_field is None:
        candidates = [c for c in shape.columns if "EMD" in c.upper() or "NAME" in c.upper()]
        if not candidates:
            raise RuntimeError("행정동 이름 필드를 찾지 못했습니다. MCP_EMD_NAME_FIELD를 지정하세요.")
        name_field = candidates[0]

    fields = [name_field, "geometry"]
    if code_field:
        fields.insert(1, code_field)
    shape = shape[fields]

    with psycopg2.connect(dsn) as conn:
        df = pd.read_sql(
            "SELECT spatial_key, lat, lon FROM dim_spatial WHERE lat IS NOT NULL AND lon IS NOT NULL",
            conn,
        )
        if df.empty:
            return 0

        points = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df["lon"], df["lat"]),
            crs="EPSG:4326",
        )

        joined = gpd.sjoin(points, shape, how="left", predicate="within")
        if name_field not in joined.columns:
            raise RuntimeError(f"필드 {name_field}가 조인 결과에 없습니다.")

        updates = []
        for _, row in joined.iterrows():
            emd_name = row.get(name_field)
            if not emd_name or pd.isna(emd_name):
                continue
            emd_code = row.get(code_field) if code_field else None
            updates.append(
                (
                    str(emd_name),
                    "emd",
                    str(emd_code) if emd_code and not pd.isna(emd_code) else None,
                    row["spatial_key"],
                )
            )

        if not updates:
            return 0

        with conn.cursor() as cur:
            extras.execute_values(
                cur,
                """
                UPDATE dim_spatial AS d SET
                    spatial_label = data.spatial_label,
                    spatial_type = data.spatial_type,
                    code = COALESCE(data.code, d.code),
                    emd_code = COALESCE(data.emd_code, d.emd_code),
                    emd_name = COALESCE(data.emd_name, d.emd_name)
                FROM (VALUES %s) AS data(spatial_label, spatial_type, code, emd_code, emd_name, spatial_key)
                WHERE d.spatial_key = data.spatial_key
                """,
                [
                    (
                        u[0],
                        u[1],
                        u[2],
                        u[2],
                        u[0],
                        u[3],
                    )
                    for u in updates
                ],
            )
        return len(updates)


def main() -> None:
    dsn = os.getenv("MCP_DB_DSN", "postgresql://mcp:mcp@localhost:5432/mcp")
    shapefile_path = os.getenv("MCP_EMD_SHAPEFILE")
    if not shapefile_path:
        raise SystemExit("MCP_EMD_SHAPEFILE 경로가 필요합니다.")
    name_field = os.getenv("MCP_EMD_NAME_FIELD")
    code_field = os.getenv("MCP_EMD_CODE_FIELD")
    epsg_raw = os.getenv("MCP_EMD_EPSG")
    epsg = int(epsg_raw) if epsg_raw else None
    encoding = os.getenv("MCP_EMD_ENCODING")
    updated = update_dim_spatial_with_emd(
        dsn,
        shapefile_path,
        name_field=name_field,
        code_field=code_field,
        epsg=epsg,
        encoding=encoding,
    )
    print(f"[emd] updated {updated} spatial rows")


if __name__ == "__main__":
    main()
