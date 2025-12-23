import math
from typing import Dict, List, Optional

from config.domain_schema import DOMAIN_INPUT_SCHEMA
from .base_module import BaseDomainModule


class SalesDomainModule(BaseDomainModule):
    domain_name = "sales"

    def __init__(self):
        self.schema = DOMAIN_INPUT_SCHEMA["sales"]

    def normalize(self, df) -> List[dict]:
        sale_columns = self._collect_columns(df, self.schema["sales_prefixes"])
        count_columns = self._collect_columns(df, self.schema["count_prefixes"])

        normalized = []
        for _, row in df.iterrows():
            time_key = self._format_month(row.get(self.schema["time_column"]))
            spatial_key = self._resolve_spatial(row)
            sales_total = float(row[sale_columns].sum()) if sale_columns else 0.0
            count_total = float(row[count_columns].sum()) if count_columns else 0.0

            population = None
            economic = {"sales": sales_total, "sales_count": count_total}

            category_info = self._extract_category_info(row)
            if category_info:
                economic["category"] = category_info
                sector = self._resolve_sector(category_info)
                if sector:
                    economic["sector"] = sector

            record = self.build_record(
                spatial_key=spatial_key,
                time_key=time_key,
                population=population,
                economic=economic,
            )
            normalized.append(record)
        return normalized

    def _collect_columns(self, df, prefixes):
        cols = []
        for prefix in prefixes:
            cols.extend([c for c in df.columns if c.startswith(prefix)])
        return cols

    def _resolve_spatial(self, row):
        for column in self.schema["spatial_candidates"]:
            value = row.get(column)
            if value:
                return value
        return None

    def _format_month(self, value):
        if value is None:
            return None
        raw = str(int(value)) if isinstance(value, (int, float)) else str(value)
        raw = raw.strip()
        if len(raw) == 7 and "-" in raw:
            return raw.replace("-", "")
        return raw

    def _extract_category_info(self, row) -> Optional[Dict[str, str]]:
        categories = {}
        fields: Dict[str, str] = self.schema.get("category_fields") or {}
        for key, column in fields.items():
            normalized_value = self._clean_value(row.get(column))
            if normalized_value:
                categories[key] = normalized_value

        if categories:
            return categories

        fallback_column = self.schema.get("category_column")
        fallback_value = self._clean_value(row.get(fallback_column)) if fallback_column else None
        if fallback_value:
            return {"label": fallback_value}
        return None

    def _clean_value(self, value) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        text = str(value).strip()
        if not text or text.lower() == "nan":
            return None
        return text

    def _resolve_sector(self, category_info: Dict[str, str]) -> Optional[str]:
        for key in ("small", "medium", "large", "label"):
            value = category_info.get(key)
            if value:
                return value
        return None
