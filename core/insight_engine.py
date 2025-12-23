from insight.insight_engine import InsightEngine as InsightEngineImpl


class CoreInsightEngine:
    """Insight 레이어 호출을 Core 단에서 래핑."""

    def __init__(self):
        self._engine = InsightEngineImpl()

    def build(self, joined, metrics):
        return self._engine.build(joined, metrics)
