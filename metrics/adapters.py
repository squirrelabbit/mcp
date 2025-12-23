from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple


class BaseMetricsAdapter(ABC):
    """도메인별 metrics input 추출기."""

    domain: str = "base"

    @abstractmethod
    def current_value(self, record: Dict[str, Any]) -> Optional[float]:
        raise NotImplementedError

    def history_key(self, record: Dict[str, Any]) -> Tuple[str, str]:
        return record.get("source"), record.get("spatial_key")

    def history_entry(self, record: Dict[str, Any]) -> Optional[Tuple[str, float]]:
        value = self.current_value(record)
        time_key = record.get("time_key")
        if value is None or time_key is None:
            return None
        return time_key, value

    def build_series(
        self, record: Dict[str, Any], history: List[Tuple[str, float]]
    ) -> List[float]:
        """현재 시점 이전까지의 값을 시간 순으로 반환."""
        target_time = record.get("time_key")
        if target_time is None:
            return []
        filtered = [entry for entry in history if entry[0] <= target_time]
        filtered.sort(key=lambda item: item[0])
        return [value for _, value in filtered]

    def context(self, record: Dict[str, Any]) -> Dict[str, Any]:
        return {}


class TelcoMetricsAdapter(BaseMetricsAdapter):
    domain = "telco"

    def current_value(self, record: Dict[str, Any]) -> Optional[float]:
        return record.get("population", {}).get("foot_traffic")


class SalesMetricsAdapter(BaseMetricsAdapter):
    domain = "sales"

    def current_value(self, record: Dict[str, Any]) -> Optional[float]:
        return record.get("economic", {}).get("sales")


ADAPTERS: Dict[str, BaseMetricsAdapter] = {
    adapter.domain: adapter
    for adapter in [TelcoMetricsAdapter(), SalesMetricsAdapter()]
}


def get_adapter(domain: str) -> Optional[BaseMetricsAdapter]:
    return ADAPTERS.get(domain)
