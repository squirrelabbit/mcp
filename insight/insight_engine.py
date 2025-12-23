from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from typing import Dict, List, Tuple, Any

import numpy as np


MAX_DEMOGRAPHIC_RECORDS = 200


class InsightEngine:
    def build(self, joined, metrics):
        """Normalize joined records, enrich with analysis layers, and return LLM-ready JSON."""
        aggregated: Dict[Tuple[str, str], Dict[str, Any]] = {}
        spatial_timelines: Dict[str, List[Tuple[datetime, Tuple[str, str], Dict[str, Any]]]] = defaultdict(list)

        for key, items in joined.items():
            spatial, time_key = key
            metric = metrics.get(key, {})
            population_summary = self._pop_summary(items)
            econ_summary = self._econ_summary(items)
            source_labels = self._collect_sources(items)
            source_value = self._resolve_source_label(source_labels)

            record = {
                "spatial": spatial,
                "time": time_key,
                "source": source_value,
                "sources": source_labels,
                "population": population_summary,
                "economic": econ_summary,
                "metrics": metric,
            }
            aggregated[key] = record
            parsed_time = self._parse_time(time_key)
            spatial_timelines[spatial].append((parsed_time, key, record))

        # Precompute timeline-aware insights
        trend_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
        correlation_map: Dict[str, Dict[str, Any]] = {}

        for spatial, entries in spatial_timelines.items():
            entries.sort(key=lambda x: (x[0] or datetime.min))
            trend_results = self._trend_insight(entries)
            trend_map.update(trend_results)
            correlation_map[spatial] = self._correlation_insight(entries)

        output = []
        for key, record in aggregated.items():
            spatial = record["spatial"]
            record_metrics = record["metrics"]
            insights = {
                "trend": trend_map.get(key),
                "correlation": correlation_map.get(spatial),
                "impact": self._impact_insight(record_metrics),
                "demographics": self._demo_insight(record["population"].get("demographics")),
                "metrics_summary": self._metric_highlights(record_metrics),
            }
            enriched = {**record, "analysis": insights}
            enriched["narrative"] = self.to_narrative(enriched)
            output.append(enriched)
        return output

    def _collect_sources(self, items):
        sources = []
        for record in items:
            domain = record.get("source")
            if domain and domain not in sources:
                sources.append(domain)
        return sources

    def _resolve_source_label(self, sources):
        if not sources:
            return None
        if len(sources) == 1:
            return sources[0]
        return "+".join(sorted(sources))

    def _pop_summary(self, items):
        demographics = []
        for record in items:
            demo = record["population"].get("demographics")
            if demo:
                demographics.append(demo)
                if len(demographics) >= MAX_DEMOGRAPHIC_RECORDS:
                    break
        return {
            "foot_traffic": sum(i["population"].get("foot_traffic", 0) for i in items),
            "demographics": demographics,
        }

    def _econ_summary(self, items):
        return {
            "sales": sum(i["economic"].get("sales", 0) for i in items)
        }

    def _trend_insight(self, entries: List[Tuple[datetime, Tuple[str, str], Dict[str, Any]]]) -> Dict[Tuple[str, str], Dict[str, Any]]:
        """
        Determine increase/decrease/stable trend for each time point within a spatial timeline.
        Uses linear regression slope across the latest 3 observations.
        """
        results: Dict[Tuple[str, str], Dict[str, Any]] = {}
        history: List[Tuple[datetime, float]] = []
        for parsed_time, key, record in entries:
            value = record["economic"].get("sales")
            if value is None:
                results[key] = {"direction": None, "slope": None, "temporal_unit": self._resolve_time_unit(record["metrics"])}
                continue

            history.append((parsed_time, float(value)))
            recent = [(idx, val) for idx, (_, val) in enumerate(history[-3:])]
            if len(recent) < 2:
                results[key] = {"direction": None, "slope": None, "temporal_unit": self._resolve_time_unit(record["metrics"])}
                continue

            x = np.arange(len(recent))
            y = np.array([val for _, val in recent], dtype=float)
            slope = float(np.polyfit(x, y, 1)[0])
            threshold = max(1e-6, abs(y.mean()) * 0.01)
            if slope > threshold:
                direction = "increase"
            elif slope < -threshold:
                direction = "decrease"
            else:
                direction = "stable"

            temporal_unit = self._resolve_time_unit(record["metrics"])
            results[key] = {
                "direction": direction,
                "slope": slope,
                "temporal_unit": temporal_unit,
            }
        return results

    def _correlation_insight(self, entries: List[Tuple[datetime, Tuple[str, str], Dict[str, Any]]]) -> Dict[str, Any]:
        """Compute correlation between foot traffic and sales for a spatial timeline."""
        traffic = []
        sales = []
        for _, _, record in entries:
            ft = record["population"].get("foot_traffic")
            sale = record["economic"].get("sales")
            if ft is None or sale is None:
                continue
            traffic.append(float(ft))
            sales.append(float(sale))

        if len(traffic) < 3 or len(set(traffic)) <= 1 or len(set(sales)) <= 1:
            return {"correlation": None, "sample_size": len(traffic)}

        corr_matrix = np.corrcoef(traffic, sales)
        corr_value = float(corr_matrix[0, 1])
        return {
            "correlation": corr_value,
            "interpretation": self._interpret_correlation(corr_value),
            "sample_size": len(traffic),
        }

    def _impact_insight(self, metrics_entry: Dict[str, Any]) -> Dict[str, Any]:
        """Classify impact based on average uplift derived from metrics layer."""
        uplifts = []
        for metric in metrics_entry.values():
            if isinstance(metric, dict):
                uplift = metric.get("uplift")
                if uplift is not None:
                    uplifts.append(float(uplift))

        if not uplifts:
            return {"impact_score": None, "classification": None}

        score = float(sum(uplifts) / len(uplifts))
        if score >= 0.2:
            classification = "high"
        elif score >= 0.05:
            classification = "moderate"
        else:
            classification = "low"

        return {
            "impact_score": score,
            "classification": classification,
            "sources": len(uplifts),
        }

    def _demo_insight(self, demographics: List[Any]) -> Dict[str, Any]:
        """Extract dominant demographic group with room for shift detection."""
        if not demographics:
            return {"dominant_group": None, "share": None, "top_groups": []}

        counter = Counter()
        for demo in demographics:
            if isinstance(demo, dict):
                self._accumulate_demographics(counter, demo)
            else:
                counter[str(demo)] += 1

        if not counter:
            return {"dominant_group": None, "share": None, "top_groups": []}

        total = sum(counter.values())
        dominant, value = counter.most_common(1)[0]
        share = value / total if total else None
        top_groups = [{"group": k, "value": v} for k, v in counter.most_common(5)]

        return {
            "dominant_group": dominant,
            "share": share,
            "top_groups": top_groups,
        }

    def _accumulate_demographics(self, counter: Counter, demo: Dict[str, Any], prefix: str = "") -> None:
        for key, value in demo.items():
            name = f"{prefix}.{key}" if prefix else key
            if isinstance(value, (int, float)):
                counter[name] += float(value)
            elif isinstance(value, dict):
                self._accumulate_demographics(counter, value, name)

    def _metric_highlights(self, metrics_entry: Dict[str, Any]) -> List[Dict[str, Any]]:
        highlights: List[Dict[str, Any]] = []
        for domain, metric in metrics_entry.items():
            if not isinstance(metric, dict):
                continue
            highlight = {}
            if metric.get("volatility") is not None:
                highlight["volatility"] = metric["volatility"]
            if metric.get("rate_of_change") is not None:
                highlight["rate_of_change"] = metric["rate_of_change"]
            if highlight:
                highlight["domain"] = domain
                highlights.append(highlight)
        return highlights

    def to_narrative(self, record: Dict[str, Any]) -> str:
        """Convert structured insight into a lightweight narrative for LLM consumption."""
        trend = record["analysis"].get("trend", {})
        correlation = record["analysis"].get("correlation", {})
        impact = record["analysis"].get("impact", {})
        demo = record["analysis"].get("demographics", {})
        metrics_summary = record["analysis"].get("metrics_summary") or []

        trend_desc = trend.get("direction") or "insufficient data"
        corr_value = correlation.get("correlation")
        corr_desc = (
            f"{corr_value:.2f} ({correlation.get('interpretation')})"
            if corr_value is not None
            else "not enough observations"
        )
        impact_desc = impact.get("classification") or "unknown"
        demo_desc = demo.get("dominant_group") or "no dominant group"
        metric_desc = "n/a"
        if metrics_summary:
            first = metrics_summary[0]
            parts = []
            if "rate_of_change" in first and first["rate_of_change"] is not None:
                parts.append(f"Δ={first['rate_of_change']:.2f}")
            if "volatility" in first and first["volatility"] is not None:
                parts.append(f"vol={first['volatility']:.2f}")
            metric_desc = f"{first['domain']} {'/'.join(parts)}" if parts else first["domain"]

        return (
            f"{record['spatial']} @ {record['time']}: trend={trend_desc}, "
            f"traffic-sales correlation={corr_desc}, impact={impact_desc}, "
            f"leading demographic={demo_desc}, metrics={metric_desc}."
        )

    def _interpret_correlation(self, value: float) -> str:
        abs_val = abs(value)
        if abs_val >= 0.75:
            return "strong"
        if abs_val >= 0.4:
            return "moderate"
        return "weak"

    def _parse_time(self, value: Any) -> datetime | None:
        if value is None:
            return None
        for fmt in ("%Y-%m-%d", "%Y-%m", "%Y%m", "%Y%m%d"):
            try:
                return datetime.strptime(str(value), fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(str(value))
        except ValueError:
            return None

    def _resolve_time_unit(self, metrics_entry: Dict[str, Any]) -> str:
        for metric in metrics_entry.values():
            if isinstance(metric, dict) and metric.get("time_unit"):
                return metric["time_unit"]
        return "time_series"
