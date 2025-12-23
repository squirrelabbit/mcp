from __future__ import annotations

import re
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import pandas as pd

from config.domain_schema import DOMAIN_INPUT_SCHEMA
from domain.telco_grid_module import TelcoGridDomainModule
from loaders.csv_loader import CSVLoader
from services.lake_writer import LakeWriter
from utils.domain_loader import collect_files


class DBIngestor:
    def __init__(
        self,
        dsn: str,
        *,
        granularity: str = "month",
        lake_writer: Optional[LakeWriter] = None,
    ) -> None:
        try:
            import psycopg2
            from psycopg2 import extras
        except ImportError as exc:
            raise RuntimeError(
                "psycopg2가 필요합니다. `pip install psycopg2-binary` 후 다시 실행하세요."
            ) from exc
        self._psycopg2 = psycopg2
        self._extras = extras
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = True
        self._granularity = granularity
        self._lake_writer = lake_writer

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def ingest_all(
        self,
        data_sources: Iterable[Path],
        sales_patterns: List[str],
        telco_patterns: List[str],
        telco_grid_patterns: List[str],
        *,
        chunksize: int = 200_000,
    ) -> None:
        sales_files = collect_files(sales_patterns, data_sources)
        new_sales_files = self._filter_new_files("sales", sales_files)
        if new_sales_files:
            print(f"[db] ingest sales: {len(new_sales_files)} new files")
            self.ingest_sales(new_sales_files, chunksize=chunksize)
            self._mark_files_ingested("sales", new_sales_files)
        elif sales_files:
            print(f"[db] ingest sales: no new files (total {len(sales_files)})")

        telco_files = collect_files(telco_patterns, data_sources)
        new_telco_files = self._filter_new_files("telco", telco_files)
        if new_telco_files:
            print(f"[db] ingest telco: {len(new_telco_files)} new files")
            self.ingest_telco(new_telco_files, chunksize=chunksize)
            self._mark_files_ingested("telco", new_telco_files)
        elif telco_files:
            print(f"[db] ingest telco: no new files (total {len(telco_files)})")

        telco_grid_files = collect_files(telco_grid_patterns, data_sources)
        new_telco_grid_files = self._filter_new_files("telco_grid", telco_grid_files)
        if new_telco_grid_files:
            print(f"[db] ingest telco_grid: {len(new_telco_grid_files)} new files")
            self.ingest_telco_grid(new_telco_grid_files, chunksize=chunksize)
            self._mark_files_ingested("telco_grid", new_telco_grid_files)
        elif telco_grid_files:
            print(f"[db] ingest telco_grid: no new files (total {len(telco_grid_files)})")

    def ingest_telco_grid(self, files: List[Path], *, chunksize: int = 200_000) -> None:
        loader = CSVLoader()
        module = TelcoGridDomainModule()
        header = loader.peek_columns(files[0])
        usecols = module.select_columns(list(header))
        frames = loader.load_many_chunks(files, chunksize=chunksize, usecols=usecols)

        chunk_index = 0
        started = time.time()
        for df in frames:
            chunk_index += 1
            if chunk_index % 5 == 0:
                elapsed = time.time() - started
                print(f"[db] telco_grid chunk {chunk_index} processed ({elapsed:.1f}s)")
            df = df.reset_index(drop=True)
            male_columns = module._collect_columns(df, module.male_prefixes)
            female_columns = module._collect_columns(df, module.female_prefixes)

            numeric_male = module._extract_numeric(df, male_columns).rename(
                columns=module._rename_map(male_columns, module.male_prefixes)
            )
            numeric_female = module._extract_numeric(df, female_columns).rename(
                columns=module._rename_map(female_columns, module.female_prefixes)
            )

            coord_x_col = module.schema.get("coord_x_column")
            coord_y_col = module.schema.get("coord_y_column")
            coord_x_series = df[coord_x_col] if coord_x_col and coord_x_col in df.columns else None
            coord_y_series = df[coord_y_col] if coord_y_col and coord_y_col in df.columns else None

            codes = df[module.schema["spatial_column"]].fillna("").astype(str).str.strip().replace("nan", "")
            spatial_keys = codes.tolist()
            month_col = module.schema.get("month_column")
            month_series = df[month_col] if month_col and month_col in df.columns else None
            time_keys = module._format_time_series(df[module.schema["date_column"]], month_series)
            time_keys = [self._bucket_time_value(t) for t in time_keys]

            meta = pd.DataFrame({"spatial": spatial_keys, "time": time_keys})
            valid_mask = (meta["spatial"] != "") & (meta["time"] != "")
            if not valid_mask.any():
                continue

            meta = meta[valid_mask].reset_index(drop=True)
            numeric_male = numeric_male.loc[valid_mask].reset_index(drop=True)
            numeric_female = numeric_female.loc[valid_mask].reset_index(drop=True)

            if numeric_male.empty:
                numeric_male = pd.DataFrame(index=meta.index)
            if numeric_female.empty:
                numeric_female = pd.DataFrame(index=meta.index)

            male_grouped = self._group_sum(meta, numeric_male)
            female_grouped = self._group_sum(meta, numeric_female)
            combined = male_grouped.add(female_grouped, fill_value=0.0)
            foot_traffic = combined.sum(axis=1)

            activity_rows = []
            spatial_rows = []
            lon_values = (
                pd.to_numeric(coord_x_series, errors="coerce").tolist()
                if coord_x_series is not None
                else [None] * len(codes)
            )
            lat_values = (
                pd.to_numeric(coord_y_series, errors="coerce").tolist()
                if coord_y_series is not None
                else [None] * len(codes)
            )
            for (spatial_key, time_key), ft in foot_traffic.items():
                date_value = self._parse_date(time_key)
                if date_value is None:
                    continue
                activity_rows.append(
                    (spatial_key, date_value, self._granularity, "telco_grid", float(ft))
                )
            for spatial_key, lon, lat in zip(codes.tolist(), lon_values, lat_values):
                if not spatial_key:
                    continue
                if pd.isna(lon):
                    lon = None
                if pd.isna(lat):
                    lat = None
                spatial_rows.append(
                    (
                        spatial_key,
                        spatial_key,
                        "coord" if lat is not None and lon is not None else None,
                        spatial_key,
                        lat,
                        lon,
                    )
                )

            if spatial_rows:
                self._upsert_spatial(spatial_rows)
            if activity_rows:
                self._upsert_activity(activity_rows)

            demo_rows = self._demo_rows(
                male_grouped, female_grouped, source="telco_grid", granularity=self._granularity
            )
            if demo_rows:
                self._upsert_demographics(demo_rows)

            if self._lake_writer:
                bronze_df = df.copy()
                bronze_df["dt"] = self._parse_date_series(pd.Series(time_keys))
                bronze_df = bronze_df.dropna(subset=["dt"])
                if not bronze_df.empty:
                    self._lake_writer.write_dataframe(
                        "bronze", "telco_grid", bronze_df, partition_cols=["dt"]
                    )

                male_prefixed = male_grouped.add_prefix("male_")
                female_prefixed = female_grouped.add_prefix("female_")
                silver_df = male_prefixed.join(female_prefixed, how="outer").fillna(0.0)
                silver_df["foot_traffic"] = foot_traffic
                silver_df = silver_df.reset_index()
                silver_df["dt"] = self._parse_date_series(silver_df["time"])
                silver_df = silver_df.rename(columns={"spatial": "spatial_key"})
                silver_df = silver_df.dropna(subset=["dt"])
                if not silver_df.empty:
                    self._lake_writer.write_dataframe(
                        "silver", "telco_grid", silver_df, partition_cols=["dt"]
                    )

    def ingest_sales(self, files: List[Path], *, chunksize: int = 200_000) -> None:
        loader = CSVLoader()
        schema = DOMAIN_INPUT_SCHEMA["sales"]
        header = loader.peek_columns(files[0])
        sale_columns = [c for c in header if c.startswith(tuple(schema["sales_prefixes"]))]
        count_columns = [c for c in header if c.startswith(tuple(schema["count_prefixes"]))]
        spatial_candidates = [c for c in schema["spatial_candidates"] if c in header]
        usecols = list({schema["time_column"], *sale_columns, *count_columns, *spatial_candidates})

        frames = loader.load_many_chunks(files, chunksize=chunksize, usecols=usecols)
        chunk_index = 0
        started = time.time()
        for df in frames:
            chunk_index += 1
            if chunk_index % 5 == 0:
                elapsed = time.time() - started
                print(f"[db] sales chunk {chunk_index} processed ({elapsed:.1f}s)")
            df = df.reset_index(drop=True)
            if not sale_columns:
                continue
            sales_total = (
                df[sale_columns].apply(pd.to_numeric, errors="coerce").fillna(0.0).sum(axis=1)
            )
            sales_count = (
                df[count_columns].apply(pd.to_numeric, errors="coerce").fillna(0.0).sum(axis=1)
                if count_columns
                else 0.0
            )

            spatial_key = self._first_non_empty(df, spatial_candidates)
            time_key = df[schema["time_column"]].astype(str).str.strip()
            bucketed = time_key.apply(self._bucket_time_value)
            date_value = bucketed.apply(self._parse_date)

            payload = pd.DataFrame(
                {
                    "spatial_key": spatial_key,
                    "date": date_value,
                    "sales": sales_total,
                    "sales_count": sales_count,
                }
            )
            payload = payload.dropna(subset=["spatial_key", "date"])
            if payload.empty:
                continue

            grouped = payload.groupby(["spatial_key", "date"]).sum(numeric_only=True)
            activity_rows = []
            spatial_rows = []
            for (spatial_key, date_value), row in grouped.iterrows():
                activity_rows.append(
                    (
                        spatial_key,
                        date_value,
                        self._granularity,
                        "sales",
                        float(row["sales"]),
                        float(row["sales_count"]),
                    )
                )
                spatial_rows.append(self._spatial_row(spatial_key))

            if spatial_rows:
                self._upsert_spatial(spatial_rows)
            if activity_rows:
                self._upsert_activity(activity_rows, include_sales_count=True)

            if self._lake_writer:
                bronze_df = df.copy()
                bronze_df["dt"] = self._parse_date_series(bucketed)
                bronze_df = bronze_df.dropna(subset=["dt"])
                if not bronze_df.empty:
                    self._lake_writer.write_dataframe(
                        "bronze", "sales", bronze_df, partition_cols=["dt"]
                    )
                silver_df = payload.copy()
                silver_df["dt"] = self._parse_date_series(payload["date"])
                silver_df = silver_df.dropna(subset=["dt"])
                if not silver_df.empty:
                    self._lake_writer.write_dataframe(
                        "silver", "sales", silver_df, partition_cols=["dt"]
                    )

    def ingest_telco(self, files: List[Path], *, chunksize: int = 200_000) -> None:
        loader = CSVLoader()
        schema = DOMAIN_INPUT_SCHEMA["telco"]
        header = loader.peek_columns(files[0])
        male_columns = [c for c in header if c.startswith(schema["male_prefix"])]
        female_columns = [c for c in header if c.startswith(schema["female_prefix"])]
        usecols = list(
            {
                schema["time_column"],
                schema["spatial_column"],
                *male_columns,
                *female_columns,
            }
        )

        frames = loader.load_many_chunks(files, chunksize=chunksize, usecols=usecols)
        chunk_index = 0
        started = time.time()
        for df in frames:
            chunk_index += 1
            if chunk_index % 5 == 0:
                elapsed = time.time() - started
                print(f"[db] telco chunk {chunk_index} processed ({elapsed:.1f}s)")
            df = df.reset_index(drop=True)
            male_total = (
                df[male_columns].apply(pd.to_numeric, errors="coerce").fillna(0.0).sum(axis=1)
                if male_columns
                else 0.0
            )
            female_total = (
                df[female_columns].apply(pd.to_numeric, errors="coerce").fillna(0.0).sum(axis=1)
                if female_columns
                else 0.0
            )
            spatial_key = df[schema["spatial_column"]].astype(str).str.strip()
            time_key = df[schema["time_column"]].astype(str).str.strip()
            bucketed = time_key.apply(self._bucket_time_value)
            date_value = bucketed.apply(self._parse_date)
            foot_traffic = male_total + female_total

            payload = pd.DataFrame(
                {"spatial_key": spatial_key, "date": date_value, "foot_traffic": foot_traffic}
            )
            payload = payload.dropna(subset=["spatial_key", "date"])
            if payload.empty:
                continue
            grouped = payload.groupby(["spatial_key", "date"]).sum(numeric_only=True)
            activity_rows = []
            spatial_rows = []
            for (spatial_key, date_value), row in grouped.iterrows():
                activity_rows.append(
                    (spatial_key, date_value, self._granularity, "telco", float(row["foot_traffic"]))
                )
                spatial_rows.append(self._spatial_row(spatial_key))
            if spatial_rows:
                self._upsert_spatial(spatial_rows)
            if activity_rows:
                self._upsert_activity(activity_rows)

            if self._lake_writer:
                bronze_df = df.copy()
                bronze_df["dt"] = self._parse_date_series(bucketed)
                bronze_df = bronze_df.dropna(subset=["dt"])
                if not bronze_df.empty:
                    self._lake_writer.write_dataframe(
                        "bronze", "telco", bronze_df, partition_cols=["dt"]
                    )
                silver_df = pd.DataFrame(
                    {
                        "spatial_key": spatial_key,
                        "date": date_value,
                        "male_total": male_total,
                        "female_total": female_total,
                        "foot_traffic": foot_traffic,
                    }
                )
                silver_df["dt"] = self._parse_date_series(silver_df["date"])
                silver_df = silver_df.dropna(subset=["dt"])
                if not silver_df.empty:
                    self._lake_writer.write_dataframe(
                        "silver", "telco", silver_df, partition_cols=["dt"]
                    )

    def _group_sum(self, meta: pd.DataFrame, values: pd.DataFrame) -> pd.DataFrame:
        if values.empty:
            empty_index = pd.MultiIndex(levels=[[], []], codes=[[], []], names=["spatial", "time"])
            return pd.DataFrame(index=empty_index)
        df = pd.concat([meta[["spatial", "time"]], values], axis=1)
        return df.groupby(["spatial", "time"]).sum()

    def _demo_rows(
        self,
        male_grouped: pd.DataFrame,
        female_grouped: pd.DataFrame,
        *,
        source: str,
        granularity: str,
    ) -> List[Tuple[str, datetime.date, str, str, str, str, float]]:
        rows: List[Tuple[str, datetime.date, str, str, str, str, float]] = []
        for sex, grouped in (("male", male_grouped), ("female", female_grouped)):
            if grouped.empty:
                continue
            stacked = grouped.stack()
            for (spatial_key, time_key, age_group), value in stacked.items():
                date_value = self._parse_date(time_key)
                if date_value is None:
                    continue
                rows.append(
                    (spatial_key, date_value, granularity, source, sex, str(age_group), float(value))
                )
        return rows

    def _upsert_spatial(self, rows: List[Tuple[str, Optional[str], Optional[str], Optional[str], Optional[float], Optional[float]]]) -> None:
        unique = {}
        for row in rows:
            unique[row[0]] = row
        rows = list(unique.values())
        with self._conn.cursor() as cur:
            self._extras.execute_values(
                cur,
                """
                INSERT INTO dim_spatial (spatial_key, spatial_label, spatial_type, code, lat, lon)
                VALUES %s
                ON CONFLICT (spatial_key) DO UPDATE SET
                  spatial_label = COALESCE(dim_spatial.spatial_label, EXCLUDED.spatial_label),
                  spatial_type = COALESCE(dim_spatial.spatial_type, EXCLUDED.spatial_type),
                  code = COALESCE(dim_spatial.code, EXCLUDED.code),
                  lat = COALESCE(dim_spatial.lat, EXCLUDED.lat),
                  lon = COALESCE(dim_spatial.lon, EXCLUDED.lon)
                """,
                rows,
            )

    def _upsert_activity(
        self,
        rows: List[Tuple],
        *,
        include_sales_count: bool = False,
    ) -> None:
        if include_sales_count:
            sql = """
                INSERT INTO gold_activity (spatial_key, date, granularity, source, sales, sales_count)
                VALUES %s
                ON CONFLICT (spatial_key, date, granularity, source) DO UPDATE SET
                  sales = COALESCE(gold_activity.sales, 0) + COALESCE(EXCLUDED.sales, 0),
                  sales_count = COALESCE(gold_activity.sales_count, 0) + COALESCE(EXCLUDED.sales_count, 0)
            """
        else:
            sql = """
                INSERT INTO gold_activity (spatial_key, date, granularity, source, foot_traffic)
                VALUES %s
                ON CONFLICT (spatial_key, date, granularity, source) DO UPDATE SET
                  foot_traffic = COALESCE(gold_activity.foot_traffic, 0) + COALESCE(EXCLUDED.foot_traffic, 0)
            """
        with self._conn.cursor() as cur:
            self._extras.execute_values(cur, sql, rows)

    def _upsert_demographics(
        self,
        rows: List[Tuple[str, datetime.date, str, str, str, str, float]],
    ) -> None:
        with self._conn.cursor() as cur:
            self._extras.execute_values(
                cur,
                """
                INSERT INTO gold_demographics (spatial_key, date, granularity, source, sex, age_group, value)
                VALUES %s
                ON CONFLICT (spatial_key, date, granularity, source, sex, age_group) DO UPDATE SET
                  value = COALESCE(gold_demographics.value, 0) + COALESCE(EXCLUDED.value, 0)
                """,
                rows,
            )

    def _first_non_empty(self, df: pd.DataFrame, candidates: List[str]) -> pd.Series:
        if not candidates:
            return pd.Series([None] * len(df))
        cleaned = df[candidates].astype(str).apply(lambda s: s.str.strip())
        cleaned = cleaned.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
        return cleaned.bfill(axis=1).iloc[:, 0]

    def _parse_date(self, value: Optional[str]) -> Optional[datetime.date]:
        if value is None:
            return None
        text = str(value).strip()
        if not text or text.lower() == "nan":
            return None
        if len(text) == 6 and text.isdigit():
            text = f"{text[:4]}-{text[4:6]}-01"
        elif len(text) == 7 and text[4] == "-":
            text = f"{text}-01"
        try:
            return datetime.fromisoformat(text).date()
        except ValueError:
            return None

    def _parse_date_series(self, series: pd.Series) -> pd.Series:
        parsed = series.copy()
        parsed = parsed.astype(str).str.strip()
        parsed = parsed.where(parsed.str.len() != 6, parsed.str.slice(0, 4) + "-" + parsed.str.slice(4, 6) + "-01")
        parsed = parsed.where(~(parsed.str.len() == 7) | (parsed.str[4] != "-"), parsed + "-01")
        dt = pd.to_datetime(parsed, errors="coerce")
        return dt.dt.strftime("%Y-%m-%d")

    def _bucket_time_value(self, value: Optional[str]) -> str:
        date_value = self._parse_date(value)
        if date_value is None:
            return ""
        if self._granularity == "month":
            date_value = date_value.replace(day=1)
        return date_value.isoformat()

    def _spatial_row(
        self, spatial_key: str
    ) -> Tuple[str, Optional[str], Optional[str], Optional[str], Optional[float], Optional[float]]:
        label = spatial_key
        lat, lon, code = self._parse_spatial_label(spatial_key)
        spatial_type = "coord" if lat is not None and lon is not None else None
        return (spatial_key, label, spatial_type, code, lat, lon)

    def _parse_spatial_label(
        self, value: str
    ) -> Tuple[Optional[float], Optional[float], Optional[str]]:
        match = re.match(r"^\s*([0-9.]+),([0-9.]+)\s*(?:\\(([^)]+)\\))?$", value)
        if not match:
            return None, None, None
        lat = float(match.group(1))
        lon = float(match.group(2))
        code = match.group(3)
        return lat, lon, code

    def _file_signature(self, path: Path) -> Tuple[str, int, int]:
        stat = path.stat()
        return (str(path.resolve()), int(stat.st_mtime), int(stat.st_size))

    def _filter_new_files(self, source: str, files: List[Path]) -> List[Path]:
        if not files:
            return []
        signatures = [self._file_signature(path) for path in files]
        existing = set()
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT path, mtime, size
                FROM ingest_file_log
                WHERE source = %s AND path = ANY(%s)
                """,
                (source, [sig[0] for sig in signatures]),
            )
            for row in cur.fetchall():
                existing.add((row[0], int(row[1]), int(row[2])))
        new_files = []
        for path, mtime, size in signatures:
            if (path, mtime, size) in existing:
                continue
            new_files.append(Path(path))
        return new_files

    def _mark_files_ingested(self, source: str, files: List[Path]) -> None:
        if not files:
            return
        rows = []
        for path in files:
            resolved, mtime, size = self._file_signature(path)
            rows.append((source, resolved, mtime, size))
        with self._conn.cursor() as cur:
            self._extras.execute_values(
                cur,
                """
                INSERT INTO ingest_file_log (source, path, mtime, size)
                VALUES %s
                ON CONFLICT (source, path, mtime, size) DO NOTHING
                """,
                rows,
            )
