from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple


def _build_insight_payload_from_maps(
    row_maps: List[Dict[str, Any]],
    context: Dict[str, Any],
) -> Dict[str, Any]:
    if not row_maps:
        return {}

    series = row_maps[-12:] if len(row_maps) > 12 else row_maps
    date_min = row_maps[0].get("date")
    date_max = row_maps[-1].get("date")

    def _pick_extreme(field: str, mode: str) -> Optional[Dict[str, Any]]:
        candidates = [r for r in row_maps if r.get(field) is not None]
        if not candidates:
            return None
        if mode == "max":
            chosen = max(candidates, key=lambda r: r.get(field))
        else:
            chosen = min(candidates, key=lambda r: r.get(field))
        return {
            "date": chosen.get("date"),
            "value": chosen.get(field),
            "foot_traffic": chosen.get("foot_traffic"),
            "sales": chosen.get("sales"),
        }

    def _pick_anomalies(field: str) -> List[Dict[str, Any]]:
        anomalies = [
            r
            for r in row_maps
            if r.get(field) is not None and abs(float(r.get(field))) >= 2
        ]
        anomalies = sorted(
            anomalies, key=lambda r: abs(float(r.get(field))), reverse=True
        )[:5]
        return [
            {
                "date": r.get("date"),
                "zscore": r.get(field),
                "foot_traffic": r.get("foot_traffic"),
                "sales": r.get("sales"),
            }
            for r in anomalies
        ]

    latest = row_maps[-1]
    dominant_group = latest.get("dominant_group")
    dominant_share = latest.get("dominant_share")

    domains = {
        "foot_traffic": _build_domain_candidates(
            row_maps,
            value_field="foot_traffic",
            mom_field="ft_mom_pct",
            yoy_field="ft_yoy_pct",
            z_field="ft_zscore",
            avg_field="ft_avg",
            std_field="ft_std",
        ),
        "sales": _build_domain_candidates(
            row_maps,
            value_field="sales",
            mom_field="sales_mom_pct",
            yoy_field="sales_yoy_pct",
            z_field="sales_zscore",
            avg_field="sales_avg",
            std_field="sales_std",
        ),
    }

    return {
        "level": context.get("level"),
        "spatial_label": context.get("spatial_label") or context.get("spatial_key"),
        "date_min": str(date_min) if date_min else None,
        "date_max": str(date_max) if date_max else None,
        "series": series,
        "highlights": {
            "foot_traffic_mom_max": _pick_extreme("ft_mom_pct", "max"),
            "foot_traffic_mom_min": _pick_extreme("ft_mom_pct", "min"),
            "foot_traffic_yoy_max": _pick_extreme("ft_yoy_pct", "max"),
            "foot_traffic_yoy_min": _pick_extreme("ft_yoy_pct", "min"),
            "sales_mom_max": _pick_extreme("sales_mom_pct", "max"),
            "sales_mom_min": _pick_extreme("sales_mom_pct", "min"),
            "sales_yoy_max": _pick_extreme("sales_yoy_pct", "max"),
            "sales_yoy_min": _pick_extreme("sales_yoy_pct", "min"),
            "foot_traffic_anomalies": _pick_anomalies("ft_zscore"),
            "sales_anomalies": _pick_anomalies("sales_zscore"),
        },
        "domains": domains,
        "dominant_group": dominant_group,
        "dominant_share": dominant_share,
    }


def _build_domain_candidates(
    row_maps: List[Dict[str, Any]],
    *,
    value_field: str,
    mom_field: str,
    yoy_field: str,
    z_field: str,
    avg_field: str,
    std_field: str,
) -> Dict[str, Any]:
    series = []
    for row in row_maps:
        series.append(
            {
                "date": row.get("date"),
                "value": row.get(value_field),
                "mom_pct": row.get(mom_field),
                "yoy_pct": row.get(yoy_field),
                "avg": row.get(avg_field),
                "std": row.get(std_field),
                "zscore": row.get(z_field),
            }
        )

    def _pick_extreme(field: str, mode: str) -> Optional[Dict[str, Any]]:
        candidates = [r for r in series if r.get(field) is not None]
        if not candidates:
            return None
        if mode == "max":
            chosen = max(candidates, key=lambda r: r.get(field))
        else:
            chosen = min(candidates, key=lambda r: r.get(field))
        return {
            "date": chosen.get("date"),
            "value": chosen.get(field),
            "series_value": chosen.get("value"),
        }

    def _pick_anomalies() -> List[Dict[str, Any]]:
        anomalies = [
            r for r in series if r.get("zscore") is not None and abs(r["zscore"]) >= 2
        ]
        anomalies = sorted(anomalies, key=lambda r: abs(r["zscore"]), reverse=True)[:5]
        return [
            {
                "date": r.get("date"),
                "zscore": r.get("zscore"),
                "value": r.get("value"),
            }
            for r in anomalies
        ]

    return {
        "series": series[-12:] if len(series) > 12 else series,
        "highlights": {
            "mom_max": _pick_extreme("mom_pct", "max"),
            "mom_min": _pick_extreme("mom_pct", "min"),
            "yoy_max": _pick_extreme("yoy_pct", "max"),
            "yoy_min": _pick_extreme("yoy_pct", "min"),
            "anomalies": _pick_anomalies(),
        },
    }


def build_insight_payload(
    cols: List[str],
    rows: List[Tuple[Any, ...]],
    context: Dict[str, Any],
) -> Dict[str, Any]:
    if not rows:
        return {}

    row_maps = []
    for row in rows:
        row_map = dict(zip(cols, row))
        if "date" in row_map and row_map["date"] is not None:
            row_map["date"] = str(row_map["date"])
        row_maps.append(row_map)

    return _build_insight_payload_from_maps(row_maps, context)


def _safe_mean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return sum(values) / len(values)


def _safe_std(values: List[float]) -> Optional[float]:
    if len(values) < 2:
        return None
    mean = _safe_mean(values)
    if mean is None:
        return None
    variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(variance)


def _calc_trend(values: List[float]) -> Tuple[Optional[str], Optional[float]]:
    if len(values) < 2:
        return None, None
    first = values[0]
    last = values[-1]
    if first == 0:
        return None, None
    change_pct = (last - first) / abs(first)
    if abs(change_pct) < 0.02:
        trend = "flat"
    elif change_pct > 0:
        trend = "upward"
    else:
        trend = "downward"
    return trend, change_pct


def build_global_baseline(
    rows: List[Tuple[Any, ...]], field_index: int, period_label: str
) -> Dict[str, Any]:
    if not rows:
        return {
            "period": period_label,
            "avg": None,
            "trend": None,
            "change_pct": None,
            "volatile_months": [],
        }

    values = [float(r[field_index]) for r in rows if r[field_index] is not None]
    avg = _safe_mean(values)
    std = _safe_std(values)
    trend, change_pct = _calc_trend(values)
    volatile_months = []
    if avg is not None and std not in (None, 0):
        for dt, *rest in rows:
            value = rest[field_index - 1]
            if value is None:
                continue
            zscore = (value - avg) / std
            if abs(zscore) >= 2:
                volatile_months.append(str(dt))

    return {
        "period": period_label,
        "avg": avg,
        "trend": trend,
        "change_pct": change_pct,
        "volatile_months": volatile_months,
        "first_value": values[0] if values else None,
        "last_value": values[-1] if values else None,
    }


def build_dataset_insight_payload(
    rows: List[Tuple[Any, ...]],
) -> Dict[str, Any]:
    if not rows:
        return {}

    series = []
    foot_values = [float(r[1]) for r in rows if r[1] is not None]
    sales_values = [float(r[2]) for r in rows if r[2] is not None]
    ft_avg = _safe_mean(foot_values)
    sales_avg = _safe_mean(sales_values)
    ft_std = _safe_std(foot_values)
    sales_std = _safe_std(sales_values)

    for idx, (dt, foot, sales) in enumerate(rows):
        ft_prev = rows[idx - 1][1] if idx >= 1 else None
        sales_prev = rows[idx - 1][2] if idx >= 1 else None
        ft_prev_year = rows[idx - 12][1] if idx >= 12 else None
        sales_prev_year = rows[idx - 12][2] if idx >= 12 else None
        ft_mom_pct = None
        if ft_prev not in (None, 0) and foot is not None:
            ft_mom_pct = (foot - ft_prev) / ft_prev
        ft_yoy_pct = None
        if ft_prev_year not in (None, 0) and foot is not None:
            ft_yoy_pct = (foot - ft_prev_year) / ft_prev_year
        sales_mom_pct = None
        if sales_prev not in (None, 0) and sales is not None:
            sales_mom_pct = (sales - sales_prev) / sales_prev
        sales_yoy_pct = None
        if sales_prev_year not in (None, 0) and sales is not None:
            sales_yoy_pct = (sales - sales_prev_year) / sales_prev_year
        ft_zscore = None
        if ft_std not in (None, 0) and foot is not None and ft_avg is not None:
            ft_zscore = (foot - ft_avg) / ft_std
        sales_zscore = None
        if sales_std not in (None, 0) and sales is not None and sales_avg is not None:
            sales_zscore = (sales - sales_avg) / sales_std

        series.append(
            {
                "date": str(dt) if dt is not None else None,
                "foot_traffic": foot,
                "sales": sales,
                "ft_prev": ft_prev,
                "sales_prev": sales_prev,
                "ft_prev_year": ft_prev_year,
                "sales_prev_year": sales_prev_year,
                "ft_mom_pct": ft_mom_pct,
                "ft_yoy_pct": ft_yoy_pct,
                "sales_mom_pct": sales_mom_pct,
                "sales_yoy_pct": sales_yoy_pct,
                "ft_avg": ft_avg,
                "sales_avg": sales_avg,
                "ft_std": ft_std,
                "sales_std": sales_std,
                "ft_zscore": ft_zscore,
                "sales_zscore": sales_zscore,
                "ft_avg_date": None,
                "sales_avg_date": None,
                "ft_rank": None,
                "sales_rank": None,
                "dominant_group": None,
                "dominant_share": None,
            }
        )

    return _build_insight_payload_from_maps(series, {"level": "total"})
