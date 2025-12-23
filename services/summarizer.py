from __future__ import annotations

import calendar
import json
import math
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


DEFAULT_RESULT_PATH = Path("output/result.json")
DEFAULT_AGGREGATIONS = ["avg"]


@dataclass
class SummarizerResult:
    query: Dict[str, Any]
    generated_at: Optional[str]
    insight_count: int
    aggregations: Dict[str, Any]
    insights: List[Dict[str, Any]]
    groups: List[Dict[str, Any]]
    compare: Optional[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "generated_at": self.generated_at,
            "insight_count": self.insight_count,
            "aggregations": self.aggregations,
            "groups": self.groups,
            "compare": self.compare,
            "insights": self.insights,
        }


class InsightSummarizer:
    """
    MCP Summarizer Layer.

    - Loads batch-computed result.json once
    - Applies filters / aggregations / top-N logic based on Query Schema
    - Returns a compact projection that LLM can consume with minimal tokens
    """

    def __init__(self, result_path: Path | str = DEFAULT_RESULT_PATH):
        self.result_path = Path(result_path)
        self._loaded = False
        self._generated_at: Optional[str] = None
        self._insights: List[Dict[str, Any]] = []

    def summarize(self, query: Dict[str, Any]) -> SummarizerResult:
        self._ensure_loaded()
        filters = deepcopy(query.get("filters") or {})
        target_path = query.get("target")
        if not target_path:
            raise ValueError("query.target is required for summarization.")

        aggregations = query.get("aggregations") or DEFAULT_AGGREGATIONS
        base_filtered = self._apply_filters(self._insights, filters)
        limited = self._apply_top_n(base_filtered, filters, target_path)
        summary_stats = self._compute_aggregations(limited, target_path, aggregations)
        groups = self._compute_groups(limited, query.get("group_by") or [], target_path, aggregations)
        compare_info = self._compute_comparison(
            base_filtered, filters, query.get("compare"), target_path, aggregations
        )
        projected = self._project_records(limited, target_path)

        return SummarizerResult(
            query=query,
            generated_at=self._generated_at,
            insight_count=len(limited),
            aggregations=summary_stats,
            insights=projected,
            groups=groups,
            compare=compare_info,
        )

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        if not self.result_path.exists():
            raise FileNotFoundError(
                f"MCP result file not found: {self.result_path}. "
                "Run the batch MCP engine first (python main.py)."
            )

        payload = json.loads(self.result_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            self._generated_at = payload.get("generated_at")
            insights = payload.get("insights")
        else:
            insights = payload
        if not isinstance(insights, list):
            raise ValueError("result.json must contain a list of insight records.")
        self._insights = insights
        self._loaded = True

    def _apply_filters(self, insights: Iterable[Dict[str, Any]], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        spatial_filter = set(filters.get("spatial") or [])
        time_filter = filters.get("time") or {}
        demo_filter = filters.get("demographics") or {}

        start = self._parse_date(time_filter.get("start"))
        end = self._parse_date(time_filter.get("end"))
        month = time_filter.get("month")
        weekdays = set(time_filter.get("weekdays") or [])

        filtered: List[Dict[str, Any]] = []
        for item in insights:
            if spatial_filter and item.get("spatial") not in spatial_filter:
                continue
            if not self._match_time(item.get("time"), start, end, month, weekdays):
                continue
            if not self._match_demographics(item, demo_filter):
                continue
            filtered.append(item)
        return filtered

    def _match_demographics(self, item: Dict[str, Any], filters: Dict[str, Any]) -> bool:
        if not filters:
            return True
        flattened = self._flatten_demographics(item)
        if not flattened:
            return False
        for key, values in filters.items():
            if not values:
                continue
            values_set = set(map(str, values))
            if not (flattened & values_set):
                return False
        return True

    def _flatten_demographics(self, item: Dict[str, Any]) -> set:
        result = set()
        demographics = (
            (((item.get("population") or {}).get("demographics")) or None)
            or ((item.get("analysis") or {}).get("demographics") or None)
        )

        def _walk(prefix: str, node: Any) -> None:
            if isinstance(node, dict):
                for k, v in node.items():
                    name = f"{prefix}.{k}" if prefix else str(k)
                    if isinstance(v, dict):
                        _walk(name, v)
                    elif isinstance(v, (int, float, str)):
                        result.add(name if isinstance(v, (int, float)) else str(v))
            elif isinstance(node, list):
                for entry in node:
                    if isinstance(entry, dict):
                        _walk(prefix, entry)
                    elif isinstance(entry, str):
                        result.add(entry)

        if demographics is not None:
            _walk("", demographics)
        return result

    def _match_time(
        self,
        time_str: Optional[str],
        start: Optional[datetime],
        end: Optional[datetime],
        month: Optional[int],
        weekdays: set,
    ) -> bool:
        if not time_str:
            return True
        dt = self._parse_any_datetime(time_str)
        if dt is None:
            return True
        if start and dt < start:
            return False
        if end and dt > end:
            return False
        if month and dt.month != month:
            return False
        if weekdays:
            weekday = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"][dt.weekday()]
            if weekday not in weekdays:
                return False
        return True

    def _apply_top_n(
        self,
        insights: List[Dict[str, Any]],
        filters: Dict[str, Any],
        target_path: str,
    ) -> List[Dict[str, Any]]:
        top_config = filters.get("top_n")
        if not top_config:
            return insights

        if isinstance(top_config, dict):
            count = int(top_config.get("count", 0))
            sort_path = top_config.get("sort_by") or target_path
            order = top_config.get("order", "desc").lower()
        else:
            count = int(top_config)
            sort_path = target_path
            order = "desc"

        if count <= 0:
            return insights
        descending = order != "asc"

        tokens = sort_path.split(".")

        def _sort_key(rec: Dict[str, Any]) -> float:
            value = self._safe_numeric(self._dig(rec, tokens))
            if value is None:
                return float("-inf") if descending else float("inf")
            return value

        sorted_records = sorted(insights, key=_sort_key, reverse=descending)
        return sorted_records[:count]

    def _compute_aggregations(
        self,
        insights: Sequence[Dict[str, Any]],
        target_path: str,
        aggregations: Sequence[str],
    ) -> Dict[str, Any]:
        values: List[float] = []
        tokens = target_path.split(".")
        for item in insights:
            val = self._safe_numeric(self._dig(item, tokens))
            if val is not None:
                values.append(val)
        if not values:
            return {}
        summary: Dict[str, Any] = {}
        for agg in aggregations:
            if agg == "sum":
                summary["sum"] = float(sum(values))
            elif agg == "avg":
                summary["avg"] = float(sum(values) / len(values))
            elif agg == "min":
                summary["min"] = float(min(values))
            elif agg == "max":
                summary["max"] = float(max(values))
            elif agg == "median":
                midpoint = len(values) // 2
                sorted_vals = sorted(values)
                if len(sorted_vals) % 2 == 1:
                    summary["median"] = float(sorted_vals[midpoint])
                else:
                    summary["median"] = float(
                        (sorted_vals[midpoint - 1] + sorted_vals[midpoint]) / 2
                    )
            elif agg == "count":
                summary["count"] = len(values)
        return summary

    def _compute_groups(
        self,
        insights: Sequence[Dict[str, Any]],
        group_paths: Sequence[str],
        target_path: str,
        aggregations: Sequence[str],
    ) -> List[Dict[str, Any]]:
        if not group_paths:
            return []
        buckets: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = defaultdict(list)
        for rec in insights:
            key = tuple(self._dig(rec, path.split(".")) for path in group_paths)
            buckets[key].append(rec)
        groups = []
        for key, records in buckets.items():
            groups.append(
                {
                    "group": {group_paths[idx]: key_part for idx, key_part in enumerate(key)},
                    "count": len(records),
                    "aggregations": self._compute_aggregations(records, target_path, aggregations),
                }
            )
        groups.sort(key=lambda row: tuple(str(row["group"].get(path)) for path in group_paths))
        return groups

    def _compute_comparison(
        self,
        current_records: Sequence[Dict[str, Any]],
        filters: Dict[str, Any],
        compare: Optional[Dict[str, Any]],
        target_path: str,
        aggregations: Sequence[str],
    ) -> Optional[Dict[str, Any]]:
        if not compare:
            return None
        time_filter = filters.get("time") or {}
        shifted = self._shift_time_filter(time_filter, compare)
        if shifted is None:
            return None

        cloned_filters = deepcopy(filters)
        cloned_filters["time"] = shifted
        previous_records = self._apply_filters(self._insights, cloned_filters)
        if not previous_records:
            return None
        current_summary = self._compute_aggregations(current_records, target_path, aggregations)
        previous_summary = self._compute_aggregations(previous_records, target_path, aggregations)
        delta = self._compute_delta(current_summary, previous_summary)
        return {
            "type": compare.get("type"),
            "interval": compare.get("interval"),
            "current": current_summary,
            "previous": previous_summary,
            "delta": delta,
        }

    def _shift_time_filter(self, time_filter: Dict[str, Any], compare: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        start = self._parse_date(time_filter.get("start"))
        end = self._parse_date(time_filter.get("end"))
        if not start or not end:
            return None

        compare_type = compare.get("type")
        interval = compare.get("interval")

        if compare_type == "yoy":
            prev_start = self._add_months(start, -12)
            prev_end = self._add_months(end, -12)
        elif compare_type == "mom":
            prev_start = self._add_months(start, -1)
            prev_end = self._add_months(end, -1)
        elif compare_type == "dod":
            prev_start = start - timedelta(days=1)
            prev_end = end - timedelta(days=1)
        elif compare_type == "custom" and interval:
            prev_start, prev_end = self._shift_custom_interval(start, end, interval)
        else:
            return None

        return {
            "start": prev_start.date().isoformat(),
            "end": prev_end.date().isoformat(),
        }

    def _shift_custom_interval(
        self, start: datetime, end: datetime, interval: str
    ) -> Tuple[datetime, datetime]:
        interval = interval.strip().lower()
        if interval.endswith("m"):
            amount = int(interval[:-1] or "1")
            return self._add_months(start, -amount), self._add_months(end, -amount)
        if interval.endswith("y"):
            amount = int(interval[:-1] or "1") * 12
            return self._add_months(start, -amount), self._add_months(end, -amount)
        if interval.endswith("w"):
            amount = int(interval[:-1] or "1") * 7
            delta = timedelta(days=amount)
            return start - delta, end - delta
        if interval.endswith("d"):
            amount = int(interval[:-1] or "1")
            delta = timedelta(days=amount)
            return start - delta, end - delta
        raise ValueError(f"Unsupported interval format: {interval}")

    def _project_records(self, insights: Sequence[Dict[str, Any]], target_path: str) -> List[Dict[str, Any]]:
        tokens = target_path.split(".")
        projected = []
        for rec in insights:
            projected.append(
                {
                    "spatial": rec.get("spatial"),
                    "time": rec.get("time"),
                    "source": rec.get("source"),
                    "target": {"path": target_path, "value": self._dig(rec, tokens)},
                    "analysis": rec.get("analysis"),
                    "narrative": rec.get("narrative"),
                }
            )
        return projected

    def _dig(self, item: Dict[str, Any], path: Sequence[str]) -> Any:
        cursor: Any = item
        for key in path:
            if not isinstance(cursor, dict):
                return None
            cursor = cursor.get(key)
        return cursor

    def _safe_numeric(self, value: Any) -> Optional[float]:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)) and not math.isnan(float(value)):
            return float(value)
        return None

    def _compute_delta(
        self, current: Dict[str, Any], previous: Dict[str, Any]
    ) -> Dict[str, Any]:
        delta = {}
        for key in set(current.keys()) & set(previous.keys()):
            curr = current.get(key)
            prev = previous.get(key)
            if isinstance(curr, (int, float)) and isinstance(prev, (int, float)):
                delta[key] = float(curr) - float(prev)
        return delta

    def _parse_date(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    def _parse_any_datetime(self, value: str) -> Optional[datetime]:
        for fmt in ("%Y-%m-%d", "%Y-%m", "%Y%m", "%Y%m%d"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    def _add_months(self, dt: datetime, months: int) -> datetime:
        year = dt.year + (dt.month - 1 + months) // 12
        month = (dt.month - 1 + months) % 12 + 1
        last_day = calendar.monthrange(year, month)[1]
        day = min(dt.day, last_day)
        return dt.replace(year=year, month=month, day=day)
