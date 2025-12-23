import logging

from normalization.normalization_layer import NormalizationLayer
from core.cross_domain_fusion_engine import CrossDomainFusionEngine
from metrics.baseline import BaselineMetrics
from core.insight_engine import CoreInsightEngine


logger = logging.getLogger(__name__)

class MCPEngine:
    def __init__(self):
        self.normalizer = NormalizationLayer()
        self.joiner = CrossDomainFusionEngine()
        self.metrics = BaselineMetrics()
        self.insight = CoreInsightEngine()

    def run(self, domain_records):
        """
        domain_records = Sales + Telco + ... 의 리스트
        """

        # 1) Normalization Layer 통해 MCP 표준화
        normalized = self.normalizer.normalize(domain_records)
        logger.info("Normalization complete: %d records", len(normalized))

        # 2) Cross-domain join
        joined = self.joiner.run(normalized)
        logger.info("Fusion complete: %d spatial-time buckets", len(joined))

        # 3) Metrics 계산 (baseline, uplift 등)
        metrics = self.metrics.compute(joined, normalized)
        logger.info("Metrics computed: %d entries", len(metrics))

        # 4) LLM Insight-friendly 구조 생성
        insights = self.insight.build(joined, metrics)
        logger.info("Insights built: %d items", len(insights))

        return {
            "normalized": normalized,
            "joined": joined,
            "metrics": metrics,
            "insights": insights
        }
