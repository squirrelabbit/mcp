from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class BaseDomainModule(ABC):
    """도메인별 normalize 로직의 공통 기반 클래스."""

    domain_name: str = "domain"

    @abstractmethod
    def normalize(self, df) -> List[Dict[str, Any]]:
        """각 도메인 raw dataframe을 MCP 표준 record 리스트로 변환."""
        raise NotImplementedError

    def build_record(
        self,
        spatial_key: str,
        time_key: str,
        population: Optional[Dict[str, Any]] = None,
        economic: Optional[Dict[str, Any]] = None,
        behavior: Optional[Dict[str, Any]] = None,
        events: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """공통 필드가 채워진 MCP record 기본 골격을 생성한다."""
        record = {
            "spatial_key": spatial_key,
            "time_key": time_key,
            "population": population or {},
            "economic": economic or {},
            "behavior": behavior or {},
            "events": list(events) if events else [],
            "source": self.domain_name,
        }
        return record
