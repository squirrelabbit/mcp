from __future__ import annotations

import os
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


DOMAIN_METRIC_MAP = {
    "population": "foot_traffic",
    "sales": "sales",
}

ALLOWED_LEVELS = {"norm", "sig", "sido"}
ALLOWED_DOMAINS = {"population", "sales"}
DEFAULT_LEVEL = os.getenv("MCP_DEFAULT_LEVEL", "sig")


def _parse_date(value: Optional[str], prefer_end: bool = False) -> Optional[date]:
    if not value:
        return None
    value = value.strip()
    if len(value) == 4 and value.isdigit():
        year = int(value)
        if prefer_end:
            return date(year, 12, 31)
        return date(year, 1, 1)
    if len(value) == 7 and value[4] == "-":
        value = f"{value}-01"
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _parse_period_range(
    period_from: Optional[str],
    period_to: Optional[str],
) -> Tuple[Optional[date], Optional[date]]:
    start = _parse_date(period_from, prefer_end=False)
    end = _parse_date(period_to, prefer_end=True)
    if start and end and start > end:
        start, end = end, start
    return start, end


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_period_date(
    dsn: str,
    view: str,
    period: str,
    region: Optional[str] = None,
    level: Optional[str] = None,
) -> Optional[date]:
    period = period.strip()
    if len(period) == 4 and period.isdigit():
        year = int(period)
        start = date(year, 1, 1)
        end = date(year + 1, 1, 1)
        clauses = ["date >= %s", "date < %s"]
        params: List[Any] = [start, end]
        if region:
            clauses.append("spatial_label = %s")
            params.append(region)
        if level:
            clauses.append("level = %s")
            params.append(level)
        where_sql = " AND ".join(clauses)
        query = f"""
            SELECT date
            FROM {view}
            WHERE {where_sql}
            ORDER BY date DESC
            LIMIT 1
        """
        try:
            import psycopg2
        except ImportError as exc:
            raise RuntimeError(
                "psycopg2가 필요합니다. `pip install psycopg2-binary` 후 다시 실행하세요."
            ) from exc
        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
                return row[0] if row else None
    return _parse_date(period)


def _trend_label(change_rate: Optional[float]) -> str:
    if change_rate is None:
        return "flat"
    if change_rate > 0:
        return "up"
    if change_rate < 0:
        return "down"
    return "flat"


def _signal_label(change_rate: Optional[float]) -> str:
    if change_rate is None:
        return "insufficient_data"
    abs_rate = abs(change_rate)
    if abs_rate >= 0.2:
        return "strong_change"
    if abs_rate >= 0.05:
        return "moderate_change"
    return "minor_change"


def compare_domains(
    region: str,
    period_from: Optional[str],
    period_to: Optional[str],
    domains: List[str],
    level: Optional[str] = None,
) -> Dict[str, Any]:
    try:
        import psycopg2
    except ImportError as exc:
        raise RuntimeError(
            "psycopg2가 필요합니다. `pip install psycopg2-binary` 후 다시 실행하세요."
        ) from exc

    dsn = os.getenv("MCP_DB_DSN", "postgresql://mcp:mcp@localhost:5432/mcp")
    view = os.getenv("MCP_INSIGHT_VIEW", "v_insight_candidate_all")

    start, end = _parse_period_range(period_from, period_to)
    resolved_level = level or DEFAULT_LEVEL
    if resolved_level not in ALLOWED_LEVELS:
        raise ValueError("level must be one of: norm, sig, sido")

    clauses = ["spatial_label = %s"]
    params: List[Any] = [region]
    clauses.append("level = %s")
    params.append(resolved_level)
    if start:
        clauses.append("date >= %s")
        params.append(start)
    if end:
        clauses.append("date <= %s")
        params.append(end)
    where_sql = " AND ".join(clauses)

    query = f"""
        SELECT
            date,
            foot_traffic,
            sales,
            ft_mom_pct,
            sales_mom_pct
        FROM {view}
        WHERE {where_sql}
        ORDER BY date DESC
        LIMIT 1
    """

    row = None
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            row = cur.fetchone()

    comparisons = []
    valid_domains = [d for d in domains if d in ALLOWED_DOMAINS]
    if not valid_domains:
        raise ValueError("domains must include at least one of: population, sales")
    metric_values = {
        "foot_traffic": None,
        "sales": None,
        "foot_traffic_mom_pct": None,
        "sales_mom_pct": None,
    }
    if row:
        _, foot_traffic, sales, ft_mom_pct, sales_mom_pct = row
        metric_values.update(
            {
                "foot_traffic": foot_traffic,
                "sales": sales,
                "foot_traffic_mom_pct": ft_mom_pct,
                "sales_mom_pct": sales_mom_pct,
            }
        )

    for domain in valid_domains:
        metric = DOMAIN_METRIC_MAP.get(domain)
        if metric is None:
            continue
        change_rate = metric_values.get(f"{metric}_mom_pct")
        comparisons.append(
            {
                "domain": domain,
                "metric": metric,
                "trend": _trend_label(change_rate),
                "change_rate": change_rate,
                "signal": _signal_label(change_rate),
            }
        )

    return {
        "comparisons": comparisons,
        "metadata": {
            "source": [view],
            "generated_at": _now_iso(),
            "period_from": start.isoformat() if start else None,
            "period_to": end.isoformat() if end else None,
            "level": resolved_level,
        },
    }


def get_rankings(
    metric: str,
    period: str,
    top_k: int = 10,
    level: Optional[str] = None,
) -> Dict[str, Any]:
    try:
        import psycopg2
    except ImportError as exc:
        raise RuntimeError(
            "psycopg2가 필요합니다. `pip install psycopg2-binary` 후 다시 실행하세요."
        ) from exc

    dsn = os.getenv("MCP_DB_DSN", "postgresql://mcp:mcp@localhost:5432/mcp")
    view = os.getenv("MCP_INSIGHT_VIEW", "v_insight_candidate_all")

    metric_map = {
        "activity_volume": "foot_traffic",
        "sales": "sales",
    }
    metric_column = metric_map.get(metric, metric)
    if metric_column not in {"foot_traffic", "sales"}:
        raise ValueError("metric must be one of: activity_volume, foot_traffic, sales")
    resolved_level = level or DEFAULT_LEVEL
    if resolved_level not in ALLOWED_LEVELS:
        raise ValueError("level must be one of: norm, sig, sido")
    target_date = _resolve_period_date(dsn, view, period, level=resolved_level)
    if target_date is None:
        raise ValueError("period must be YYYY, YYYY-MM, or YYYY-MM-DD")
    if top_k <= 0:
        raise ValueError("top_k must be greater than 0")
    if top_k > 100:
        top_k = 100

    query = f"""
        SELECT spatial_label, {metric_column}
        FROM {view}
        WHERE date = %s
          AND level = %s
        ORDER BY {metric_column} DESC NULLS LAST
        LIMIT %s
    """

    rows = []
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (target_date, resolved_level, top_k))
            rows = cur.fetchall()

    rankings = [
        {"spatial_label": spatial_label, "metric": metric_column, "value": value}
        for spatial_label, value in rows
    ]

    return {
        "rankings": rankings,
        "metadata": {
            "source": [view],
            "metric": metric_column,
            "period": target_date.isoformat(),
            "generated_at": _now_iso(),
            "period_from": target_date.isoformat(),
            "period_to": target_date.isoformat(),
            "level": resolved_level,
        },
    }


def detect_anomaly(
    region: str,
    domain: str,
    period: str,
    z_threshold: float = 2.0,
    level: Optional[str] = None,
) -> Dict[str, Any]:
    try:
        import psycopg2
    except ImportError as exc:
        raise RuntimeError(
            "psycopg2가 필요합니다. `pip install psycopg2-binary` 후 다시 실행하세요."
        ) from exc

    dsn = os.getenv("MCP_DB_DSN", "postgresql://mcp:mcp@localhost:5432/mcp")
    view = os.getenv("MCP_INSIGHT_VIEW", "v_insight_candidate_all")

    domain_map = {
        "population": "foot_traffic",
        "sales": "sales",
    }
    if domain not in ALLOWED_DOMAINS:
        raise ValueError("domain must be one of: population, sales")
    metric = domain_map.get(domain, domain)
    resolved_level = level or DEFAULT_LEVEL
    if resolved_level not in ALLOWED_LEVELS:
        raise ValueError("level must be one of: norm, sig, sido")
    target_date = _resolve_period_date(
        dsn, view, period, region=region, level=resolved_level
    )
    if target_date is None:
        raise ValueError("period must be YYYY, YYYY-MM, or YYYY-MM-DD")
    if z_threshold <= 0:
        raise ValueError("z_threshold must be greater than 0")

    z_column = f"{metric}_zscore"
    query = f"""
        SELECT {metric}, {z_column}
        FROM {view}
        WHERE spatial_label = %s
          AND date = %s
          AND level = %s
        LIMIT 1
    """

    row = None
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (region, target_date, resolved_level))
            row = cur.fetchone()

    value = None
    zscore = None
    is_anomaly = False
    if row:
        value, zscore = row
        if zscore is not None and abs(zscore) >= z_threshold:
            is_anomaly = True

    return {
        "anomaly": {
            "region": region,
            "metric": metric,
            "period": target_date.isoformat(),
            "value": value,
            "zscore": zscore,
            "threshold": z_threshold,
            "is_anomaly": is_anomaly,
        },
        "metadata": {
            "source": [view],
            "generated_at": _now_iso(),
            "period_from": target_date.isoformat(),
            "period_to": target_date.isoformat(),
            "level": resolved_level,
        },
    }


def get_advanced_insight(
    region: str,
    domains: List[str],
    period: str,
    level: Optional[str] = None,
) -> Dict[str, Any]:
    try:
        import psycopg2
    except ImportError as exc:
        raise RuntimeError(
            "psycopg2가 필요합니다. `pip install psycopg2-binary` 후 다시 실행하세요."
        ) from exc

    dsn = os.getenv("MCP_DB_DSN", "postgresql://mcp:mcp@localhost:5432/mcp")
    mv = os.getenv("MCP_ADVANCED_MV", "mv_insight_advanced")

    target_date = _parse_date(period)
    if target_date is None:
        raise ValueError("period must be YYYY, YYYY-MM, or YYYY-MM-DD")
    valid_domains = [d for d in domains if d in ALLOWED_DOMAINS]
    if not valid_domains:
        raise ValueError("domains must include at least one of: population, sales")

    clauses = ["spatial_label = %s"]
    params: List[Any] = [region]
    resolved_level = level or DEFAULT_LEVEL
    if resolved_level not in ALLOWED_LEVELS:
        raise ValueError("level must be one of: norm, sig, sido")
    clauses.append("level = %s")
    params.append(resolved_level)
    where_sql = " AND ".join(clauses)

    query = f"""
        SELECT
            level,
            spatial_label,
            corr_sales_foot_traffic,
            sales_impact_slope,
            sales_impact_score,
            foot_traffic_impact_score
        FROM {mv}
        WHERE {where_sql}
        LIMIT 1
    """

    row = None
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            row = cur.fetchone()

    payload = {}
    if row:
        (
            level_value,
            spatial_label,
            corr_sales_foot_traffic,
            sales_impact_slope,
            sales_impact_score,
            foot_traffic_impact_score,
        ) = row
        payload = {
            "level": level_value,
            "region": spatial_label,
            "period": target_date.isoformat(),
            "domains": valid_domains,
            "correlation": {
                "sales_vs_foot_traffic": corr_sales_foot_traffic,
            },
            "impact": {
                "sales_impact_slope": sales_impact_slope,
                "sales_impact_score": sales_impact_score,
                "foot_traffic_impact_score": foot_traffic_impact_score,
            },
        }

    return {
        "advanced_insight": payload,
        "metadata": {
            "source": [mv],
            "generated_at": _now_iso(),
            "period_from": target_date.isoformat(),
            "period_to": target_date.isoformat(),
            "level": resolved_level,
        },
    }
