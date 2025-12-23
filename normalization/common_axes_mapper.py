# normalization/common_axes_mapper.py

from .type_normalizer import TypeNormalizer

class CommonAxesMapper:
    def __init__(self):
        self.normalizer = TypeNormalizer()

    def map(self, record):
        """
        Domain Module이 반환한 record(dict)를 받아
        MCP 표준 schema로 매핑 후 반환한다.
        """

        record["time_key"] = self._normalize_time(record.get("time_key"))
        record["spatial_key"] = self._normalize_spatial(record.get("spatial_key"))

        # population
        pop = record.get("population", {})
        record["population"] = {
            "foot_traffic": pop.get("foot_traffic", 0),
            "demographics": pop.get("demographics", None)
        }

        # economic
        econ = record.get("economic", {})
        record["economic"] = {
            "sales": econ.get("sales", 0),
            "sales_count": econ.get("sales_count", 0)
        }

        # behavior/events 그대로 두되 빈값 보완
        record.setdefault("behavior", {})
        record.setdefault("events", [])

        return record

    def _normalize_time(self, time_value):
        dt = self.normalizer.normalize_time(time_value)
        if not dt:
            return None
        # normalization된 time을 YYYY-MM-DD 또는 YYYY-MM로 표현
        if dt.day == 1 and dt.hour == 0:
            return dt.strftime("%Y-%m")  # 월 단위
        return dt.strftime("%Y-%m-%d")

    def _normalize_spatial(self, spatial):
        return self.normalizer.normalize_spatial(spatial)
