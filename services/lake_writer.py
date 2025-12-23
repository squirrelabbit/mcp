from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional

import pandas as pd


class LakeWriter:
    def __init__(
        self,
        *,
        target: str = "local",
        root: str = "lake",
        format: str = "parquet",
        s3_endpoint: Optional[str] = None,
        s3_access_key: Optional[str] = None,
        s3_secret_key: Optional[str] = None,
        s3_bucket: Optional[str] = None,
    ) -> None:
        self._target = target
        self._root = root.rstrip("/")
        self._format = format
        self._s3_endpoint = s3_endpoint
        self._s3_access_key = s3_access_key
        self._s3_secret_key = s3_secret_key
        self._s3_bucket = s3_bucket

    def write_dataframe(
        self,
        layer: str,
        domain: str,
        df: pd.DataFrame,
        *,
        partition_cols: Optional[Iterable[str]] = None,
    ) -> None:
        if df.empty:
            return
        if self._target == "minio":
            path = f"s3://{self._s3_bucket}/{layer}/{domain}"
            storage_options = {
                "key": self._s3_access_key,
                "secret": self._s3_secret_key,
                "client_kwargs": {"endpoint_url": self._s3_endpoint},
            }
        else:
            path = str(Path(self._root) / layer / domain)
            storage_options = None

        if self._format == "parquet":
            try:
                import pyarrow  # noqa: F401
            except ImportError as exc:
                raise RuntimeError(
                    "Parquet 저장에는 pyarrow가 필요합니다. `pip install pyarrow` 후 다시 실행하세요."
                ) from exc
            df.to_parquet(
                path,
                index=False,
                partition_cols=list(partition_cols) if partition_cols else None,
                storage_options=storage_options,
            )
        elif self._format == "csv":
            file_path = self._resolve_csv_path(path)
            df.to_csv(file_path, index=False)
        else:
            raise ValueError(f"Unsupported lake format: {self._format}")

    def _resolve_csv_path(self, base_path: str) -> str:
        if self._target == "minio":
            return f"{base_path}/data.csv"
        path = Path(base_path)
        path.mkdir(parents=True, exist_ok=True)
        return str(path / "data.csv")
