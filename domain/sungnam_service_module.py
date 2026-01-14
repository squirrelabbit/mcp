from __future__ import annotations

from typing import Optional, Tuple

from config.domain_schema import DOMAIN_INPUT_SCHEMA
from .base_module import BaseDomainModule


class SungnamServiceDomainModule(BaseDomainModule):
    domain_name = "sungnam_service"

    def __init__(self) -> None:
        self.schema = DOMAIN_INPUT_SCHEMA["sungnam_service"]

    def normalize(self, df):
        raise NotImplementedError(
            "Use table-specific handlers (inflow/sex_age/pcell/unique) for Sungnam data."
        )

    def detect_table(self, filename: str) -> Optional[str]:
        name = filename.lower()
        if "sungnam_service_inflow_pop" in name:
            return "inflow"
        if "sungnam_service_sex_age_pop" in name:
            return "sex_age"
        if "sungnam_service_pcell_sex_age_pop" in name:
            return "pcell_sex_age"
        if "sungnam_service_pcell_pop" in name:
            return "pcell"
        if "sungnam_unique_pop" in name:
            return "unique"
        return None

    def parse_sex_age(self, value: str) -> Tuple[Optional[str], Optional[str]]:
        if not value:
            return None, None
        text = str(value).strip().lower()
        if len(text) < 3 or "_" not in text:
            return None, None
        sex_token, age_token = text.split("_", 1)
        sex = {"m": "male", "w": "female"}.get(sex_token)
        age_group = age_token.strip()
        if not sex or not age_group:
            return None, None
        return sex, age_group

    def parse_prefixed_age(self, column: str, prefix: str) -> Optional[str]:
        if not column or not column.upper().startswith(prefix):
            return None
        return column[len(prefix) :].strip()

    def table_source(self, table_key: str) -> str:
        mapping = {
            "inflow": "sungnam_inflow",
            "sex_age": "sungnam_sex_age",
            "pcell_sex_age": "sungnam_pcell_sex_age",
            "pcell": "sungnam_pcell",
            "unique": "sungnam_unique",
        }
        return mapping.get(table_key, self.domain_name)
