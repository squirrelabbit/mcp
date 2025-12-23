from linking.joiner import CrossDomainJoiner


class CrossDomainFusionEngine:
    """공간/시간 축으로 cross-domain 레코드를 융합하는 Core 엔진."""

    def __init__(self):
        self._joiner = CrossDomainJoiner()

    def run(self, records):
        """정규화된 record 리스트를 공간/시간 키별로 묶는다."""
        if not records:
            return {}
        return self._joiner.join(records)
