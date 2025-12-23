from __future__ import annotations

import os
import time
from typing import List, Optional, Tuple

import psycopg2

from services.vworld_geocoder import reverse_geocode_sig


DSN = os.getenv("MCP_DB_DSN", "postgresql://mcp:mcp@localhost:5432/mcp")


def _fetch_targets(limit: int) -> List[Tuple[str, float, float]]:
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT spatial_key, lat, lon
                FROM dim_spatial
                WHERE lat IS NOT NULL
                  AND lon IS NOT NULL
                  AND (sig_name IS NULL OR sido_name IS NULL)
                ORDER BY spatial_key
                LIMIT %s
                """,
                (limit,),
            )
            return cur.fetchall()


def _lookup_admin(sig_code: str) -> Tuple[Optional[str], Optional[str]]:
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT sig_name, sido_name FROM admin_sig WHERE sig_code = %s",
                (sig_code,),
            )
            row = cur.fetchone()
            if not row:
                return None, None
            return row[0], row[1]


def _update_spatial(
    spatial_key: str,
    sig_code: Optional[str],
    sig_name: Optional[str],
    sido_code: Optional[str],
    sido_name: Optional[str],
) -> None:
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE dim_spatial
                SET sig_code = COALESCE(%s, sig_code),
                    sig_name = COALESCE(%s, sig_name),
                    sido_code = COALESCE(%s, sido_code),
                    sido_name = COALESCE(%s, sido_name)
                WHERE spatial_key = %s
                """,
                (sig_code, sig_name, sido_code, sido_name, spatial_key),
            )


def _derive_sido_from_full(full_nm: Optional[str]) -> Optional[str]:
    if not full_nm:
        return None
    parts = full_nm.split()
    return parts[0] if parts else None


def backfill(limit: int, sleep_sec: float) -> None:
    targets = _fetch_targets(limit)
    if not targets:
        print("[vworld] no targets found")
        return

    updated = 0
    for spatial_key, lat, lon in targets:
        result = reverse_geocode_sig(float(lon), float(lat))
        if not result:
            continue
        sig_code = result.get("sig_cd")
        sig_name = result.get("sig_kor_nm")
        full_nm = result.get("full_nm")
        sido_name = _derive_sido_from_full(full_nm)
        sido_code = sig_code[:2] if sig_code else None

        print(spatial_key, sig_name)
        if sig_code:
            admin_sig_name, admin_sido_name = _lookup_admin(sig_code)
            if admin_sig_name:
                sig_name = admin_sig_name
            if admin_sido_name:
                sido_name = admin_sido_name

        _update_spatial(spatial_key, sig_code, sig_name, sido_code, sido_name)
        updated += 1
        if sleep_sec:
            time.sleep(sleep_sec)

    print(f"[vworld] updated {updated} rows")


if __name__ == "__main__":
    limit = int(os.getenv("MCP_VWORLD_LIMIT", "5000"))
    sleep_sec = float(os.getenv("MCP_VWORLD_SLEEP", "0.2"))
    backfill(limit, sleep_sec)
