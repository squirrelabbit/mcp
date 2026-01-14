from datetime import datetime
from typing import List

from config.domain_schema import DOMAIN_INPUT_SCHEMA
from .base_module import BaseDomainModule


class TelcoDomainModule(BaseDomainModule):
    domain_name = "telco"

    def __init__(self):
        self.schema = DOMAIN_INPUT_SCHEMA["telco"]

    def normalize(self, df) -> List[dict]:
        male_columns = [
            c for c in df.columns if c.startswith(self.schema["male_prefix"])
        ]
        female_columns = [
            c for c in df.columns if c.startswith(self.schema["female_prefix"])
        ]

        normalized = []
        for _, row in df.iterrows():
            time_key = self._format_date(
                row.get(self.schema["time_column"]), row.get(self.schema["day_column"])
            )
            spatial_raw = row.get(self.schema["spatial_column"])
            spatial_key = str(spatial_raw) if spatial_raw is not None else None

            male_total = float(row[male_columns].sum()) if male_columns else 0.0
            female_total = float(row[female_columns].sum()) if female_columns else 0.0

            population = {
                "foot_traffic": male_total + female_total,
                "demographics": {"male": male_total, "female": female_total},
            }
            behavior = {"day_index": int(row.get(self.schema["day_column"], 0))}

            record = self.build_record(
                spatial_key=spatial_key,
                time_key=time_key,
                population=population,
                behavior=behavior,
            )
            normalized.append(record)
        return normalized

    def _format_date(self, year_month, day_cnt):
        if year_month is None:
            return None
        try:
            ym_val = int(float(year_month))
        except (TypeError, ValueError):
            return None
        ym_str = f"{ym_val:06d}"
        dt = datetime.strptime(ym_str, "%Y%m")
        return dt.strftime("%Y-%m")
