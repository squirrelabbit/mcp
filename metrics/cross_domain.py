from typing import Dict, Optional


class CrossDomainMetrics:
    """여러 도메인 지표를 조합한 합성 지표 계산기."""

    def compute(self, domain_metrics: Dict[str, Dict]) -> Dict[str, Optional[float]]:
        impact = self.impact_score(domain_metrics)
        return {"impact_score": impact} if impact is not None else {}

    def impact_score(self, domain_metrics: Dict[str, Dict]) -> Optional[float]:
        """여러 도메인의 uplift 평균을 간단한 impact score로 사용."""
        uplifts = [
            metrics.get("uplift")
            for metrics in domain_metrics.values()
            if metrics.get("uplift") is not None
        ]
        if len(uplifts) < 2:
            return None
        return sum(uplifts) / len(uplifts)
