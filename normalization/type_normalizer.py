from datetime import datetime
from typing import Optional

from config.spatial_codes import SGG_CODE_TO_NAME


class TypeNormalizer:
    def normalize_time(self, value):
        """YYYYMM / YYYY-MM / YYYY-MM-DD 등을 datetime으로 변환."""
        if value is None:
            return None
        try:
            text = str(value)
            if len(text) == 6 and text.isdigit():
                return datetime.strptime(text, "%Y%m")
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    def normalize_spatial(self, value):
        """시군구 값을 코드/명칭에 상관없이 공통 명칭으로 변환."""
        if value is None:
            return None

        text = str(value).strip()
        code = self._extract_numeric_code(text)
        if code:
            return SGG_CODE_TO_NAME.get(code, code)

        normalized = text.replace("서울", "").strip()
        if normalized.endswith(("구", "시", "군")):
            return normalized
        return normalized + "구"

    def _extract_numeric_code(self, value: str) -> Optional[str]:
        try:
            float_val = float(value)
            if float_val.is_integer():
                return str(int(float_val))
        except ValueError:
            pass
        if value.isdigit():
            return value
        return None

    def normalize_demographics(self, age, sex=None):
        age_group = f"{int(age)//10}0s"
        return f"{age_group}_{sex}" if sex else age_group
