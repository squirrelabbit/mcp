from .missing_field_handler import MissingFieldHandler
from .common_axes_mapper import CommonAxesMapper
from .schema_validator import SchemaValidator

class NormalizationLayer:
    def __init__(self):
        self.miss = MissingFieldHandler()
        self.mapper = CommonAxesMapper()
        self.validator = SchemaValidator()

    def normalize(self, records):
        normalized = []
        for r in records:
            r = self.miss.fill(r)        # 1) 누락 필드 채움
            r = self.mapper.map(r)       # 2) MCP 공통축 매핑
            self.validator.validate(r)   # 3) schema 검사
            normalized.append(r)
        return normalized
