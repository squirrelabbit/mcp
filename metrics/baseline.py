from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Tuple

from core.metrics_engine import MetricsEngine
from metrics.adapters import get_adapter
from metrics.cross_domain import CrossDomainMetrics


class BaselineMetrics:
    """Join 결과를 받아 도메인별 baseline/uplift 메트릭을 계산."""

    def __init__(self):
        self.engine = MetricsEngine()
        self.cross_domain = CrossDomainMetrics()

    def compute(self, joined_records, normalized_records):
        history = self._build_history(normalized_records)
        results = {}
        for key, records in joined_records.items():
            per_domain = {}
            for record in records:
                domain = record.get("source")
                adapter = get_adapter(domain)
                if adapter is None:
                    continue
                history_key = adapter.history_key(record)
                series = adapter.build_series(record, history.get(history_key, []))
                current_value = adapter.current_value(record)
                if current_value is None:
                    continue
                metrics = self.engine.compute(
                    domain, series, current_value, adapter.context(record)
                )
                per_domain[domain] = metrics
            cross_metrics = self.cross_domain.compute(per_domain)
            if cross_metrics:
                per_domain["cross_domain"] = cross_metrics
            results[key] = per_domain
        return results

    def compute_baseline(self, joined_records, normalized_records):
        """기존 API 호환을 위한 alias."""
        return self.compute(joined_records, normalized_records)

    def _build_history(self, normalized_records) -> Dict[Tuple[str, str], List[Tuple[str, float]]]:
        history = defaultdict(list)
        for record in normalized_records:
            domain = record.get("source")
            adapter = get_adapter(domain)
            if adapter is None:
                continue
            entry = adapter.history_entry(record)
            if entry is None:
                continue
            history_key = adapter.history_key(record)
            history[history_key].append(entry)
        for key in history:
            history[key].sort(key=lambda item: item[0])
        return history
