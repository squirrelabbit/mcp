from typing import Dict, List, Optional

import pandas as pd
from config.domain_schema import DOMAIN_INPUT_SCHEMA
from .base_module import BaseDomainModule


class TelcoGridDomainModule(BaseDomainModule):
    domain_name = "telco_grid"

    def __init__(self) -> None:
        self.schema = DOMAIN_INPUT_SCHEMA["telco_grid"]
        self.male_prefixes = self.schema["male_prefixes"]
        self.female_prefixes = self.schema["female_prefixes"]

    def normalize(self, df) -> List[dict]:
        male_columns = self._collect_columns(df, self.male_prefixes)
        female_columns = self._collect_columns(df, self.female_prefixes)

        numeric_male = self._extract_numeric(df, male_columns).rename(
            columns=self._rename_map(male_columns, self.male_prefixes)
        )
        numeric_female = self._extract_numeric(df, female_columns).rename(
            columns=self._rename_map(female_columns, self.female_prefixes)
        )

        coord_x_col = self.schema.get("coord_x_column")
        coord_y_col = self.schema.get("coord_y_column")
        coord_x_series = (
            df[coord_x_col] if coord_x_col and coord_x_col in df.columns else None
        )
        coord_y_series = (
            df[coord_y_col] if coord_y_col and coord_y_col in df.columns else None
        )

        spatial_keys = self._resolve_spatial_series(
            df[self.schema["spatial_column"]], coord_x_series, coord_y_series
        )
        month_col = self.schema.get("month_column")
        month_series = df[month_col] if month_col and month_col in df.columns else None
        time_keys = self._format_time_series(
            df[self.schema["date_column"]], month_series
        )

        aggregated = self._aggregate_by_axes(
            spatial_keys, time_keys, numeric_male, numeric_female
        )
        normalized: List[dict] = []
        for (spatial_key, time_key), male_demo, female_demo in aggregated:
            male_total = sum(male_demo.values())
            female_total = sum(female_demo.values())
            population = {
                "foot_traffic": male_total + female_total,
                "demographics": {
                    "male_by_age": male_demo,
                    "female_by_age": female_demo,
                },
            }
            record = self.build_record(
                spatial_key=spatial_key, time_key=time_key, population=population
            )
            normalized.append(record)
        return normalized

    def select_columns(self, columns: List[str]) -> List[str]:
        required = set()
        for name in (
            self.schema.get("spatial_column"),
            self.schema.get("date_column"),
            self.schema.get("month_column"),
            self.schema.get("coord_x_column"),
            self.schema.get("coord_y_column"),
        ):
            if name and name in columns:
                required.add(name)
        prefixes = tuple(self.male_prefixes + self.female_prefixes)
        for col in columns:
            if col.startswith(prefixes):
                required.add(col)
        return list(required)

    def normalize_chunks(self, frames) -> List[dict]:
        aggregated = {}
        for df in frames:
            df = df.reset_index(drop=True)
            male_columns = self._collect_columns(df, self.male_prefixes)
            female_columns = self._collect_columns(df, self.female_prefixes)

            numeric_male = self._extract_numeric(df, male_columns).rename(
                columns=self._rename_map(male_columns, self.male_prefixes)
            )
            numeric_female = self._extract_numeric(df, female_columns).rename(
                columns=self._rename_map(female_columns, self.female_prefixes)
            )

            coord_x_col = self.schema.get("coord_x_column")
            coord_y_col = self.schema.get("coord_y_column")
            coord_x_series = (
                df[coord_x_col] if coord_x_col and coord_x_col in df.columns else None
            )
            coord_y_series = (
                df[coord_y_col] if coord_y_col and coord_y_col in df.columns else None
            )

            spatial_keys = self._resolve_spatial_series(
                df[self.schema["spatial_column"]], coord_x_series, coord_y_series
            )
            month_col = self.schema.get("month_column")
            month_series = (
                df[month_col] if month_col and month_col in df.columns else None
            )
            time_keys = self._format_time_series(
                df[self.schema["date_column"]], month_series
            )

            for (
                spatial_key,
                time_key,
            ), male_demo, female_demo in self._aggregate_by_axes(
                spatial_keys, time_keys, numeric_male, numeric_female
            ):
                key = (spatial_key, time_key)
                bucket = aggregated.get(key)
                if bucket is None:
                    aggregated[key] = {
                        "male": dict(male_demo),
                        "female": dict(female_demo),
                    }
                    continue
                for age_key, value in male_demo.items():
                    bucket["male"][age_key] = bucket["male"].get(age_key, 0.0) + float(
                        value
                    )
                for age_key, value in female_demo.items():
                    bucket["female"][age_key] = bucket["female"].get(
                        age_key, 0.0
                    ) + float(value)

        normalized: List[dict] = []
        for spatial_key, time_key in sorted(aggregated.keys()):
            bucket = aggregated[(spatial_key, time_key)]
            male_demo = bucket["male"]
            female_demo = bucket["female"]
            male_total = sum(male_demo.values())
            female_total = sum(female_demo.values())
            population = {
                "foot_traffic": male_total + female_total,
                "demographics": {
                    "male_by_age": male_demo,
                    "female_by_age": female_demo,
                },
            }
            record = self.build_record(
                spatial_key=spatial_key, time_key=time_key, population=population
            )
            normalized.append(record)
        return normalized

    def _aggregate_by_axes(
        self,
        spatial_keys: List[str],
        time_keys: List[str],
        male_df: pd.DataFrame,
        female_df: pd.DataFrame,
    ):
        meta = pd.DataFrame({"spatial": spatial_keys, "time": time_keys})
        valid_mask = (meta["spatial"] != "") & (meta["time"] != "")
        if not valid_mask.any():
            return []

        meta = meta[valid_mask].reset_index(drop=True)
        male_df = male_df.loc[valid_mask].reset_index(drop=True)
        female_df = female_df.loc[valid_mask].reset_index(drop=True)

        if male_df.empty:
            male_df = pd.DataFrame(index=meta.index)
        if female_df.empty:
            female_df = pd.DataFrame(index=meta.index)

        male_grouped = self._group_sum(meta, male_df)
        female_grouped = self._group_sum(meta, female_df)

        male_dict = male_grouped.to_dict(orient="index")
        female_dict = female_grouped.to_dict(orient="index")
        all_keys = sorted(set(male_dict.keys()) | set(female_dict.keys()))

        for key in all_keys:
            male_demo = male_dict.get(key, {})
            female_demo = female_dict.get(key, {})
            yield key, male_demo, female_demo

    def _group_sum(self, meta: pd.DataFrame, values: pd.DataFrame) -> pd.DataFrame:
        if values.empty:
            empty_index = pd.MultiIndex(
                levels=[[], []], codes=[[], []], names=["spatial", "time"]
            )
            return pd.DataFrame(index=empty_index)
        df = pd.concat([meta[["spatial", "time"]], values], axis=1)
        grouped = df.groupby(["spatial", "time"]).sum()
        return grouped

    def _collect_columns(self, df, prefixes) -> List[str]:
        columns = []
        for prefix in prefixes:
            columns.extend([col for col in df.columns if col.startswith(prefix)])
        return columns

    def _extract_numeric(self, df, columns: List[str]) -> pd.DataFrame:
        if not columns:
            return pd.DataFrame(index=df.index)
        numeric = df[columns].apply(pd.to_numeric, errors="coerce").fillna(0.0)
        return numeric

    def _rename_map(self, columns: List[str], prefixes: List[str]) -> Dict[str, str]:
        return {col: self._strip_prefix(col, prefixes) for col in columns}

    def _strip_prefix(self, column: str, prefixes: List[str]) -> str:
        for prefix in prefixes:
            if column.startswith(prefix):
                return column[len(prefix) :]
        return column

    def _resolve_spatial_series(
        self,
        codes: pd.Series,
        coord_x: Optional[pd.Series],
        coord_y: Optional[pd.Series],
    ) -> List[str]:
        cleaned_codes = codes.fillna("").astype(str).str.strip().replace("nan", "")
        lon_values = (
            pd.to_numeric(coord_x, errors="coerce").tolist()
            if coord_x is not None
            else [None] * len(cleaned_codes)
        )
        lat_values = (
            pd.to_numeric(coord_y, errors="coerce").tolist()
            if coord_y is not None
            else [None] * len(cleaned_codes)
        )
        result: List[str] = []
        for code, lon, lat in zip(cleaned_codes.tolist(), lon_values, lat_values):
            result.append(self._format_spatial_label(code, lon, lat))
        return result

    def _format_spatial_label(
        self, code: str, lon: Optional[float], lat: Optional[float]
    ) -> str:
        has_coords = lon is not None and lat is not None
        coord_label = f"{lat:.5f},{lon:.5f}" if has_coords else ""
        if coord_label and code:
            return f"{coord_label} ({code})"
        if coord_label:
            return coord_label
        return code

    def _format_time_series(
        self, day_series: pd.Series, month_series: Optional[pd.Series]
    ) -> List[str]:
        day_str = (
            day_series.fillna("")
            .astype(str)
            .str.replace("-", "", regex=False)
            .str.replace(".0", "", regex=False)
            .str.strip()
        )
        valid_day = day_str.str.match(r"^\d{8}$")
        day_formatted = day_str.where(
            ~valid_day,
            day_str.str.slice(0, 4)
            + "-"
            + day_str.str.slice(4, 6)
            + "-"
            + day_str.str.slice(6, 8),
        )

        if month_series is not None:
            month_str = (
                month_series.fillna("")
                .astype(str)
                .str.replace("-", "", regex=False)
                .str.replace(".0", "", regex=False)
                .str.strip()
            )
            valid_month = month_str.str.match(r"^\d{6}$")
            month_formatted = month_str.where(
                ~valid_month,
                month_str.str.slice(0, 4) + "-" + month_str.str.slice(4, 6) + "-01",
            )
        else:
            month_formatted = pd.Series([""] * len(day_series))

        result = []
        for day_val, month_val in zip(day_formatted.tolist(), month_formatted.tolist()):
            if day_val and day_val != "nan":
                result.append(day_val)
            elif month_val and month_val != "nan":
                result.append(month_val)
            else:
                result.append("")
        return result
