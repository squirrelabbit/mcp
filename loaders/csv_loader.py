import csv
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Iterable, Iterator, Optional, Sequence

import pandas as pd


def _read_csv_task(args):
    path_str, sep, encoding, kwargs = args
    loader = CSVLoader()
    return loader.load(path_str, sep=sep, encoding=encoding, **kwargs)


class CSVLoader:
    def load(
        self,
        path: str,
        *,
        sep: Optional[str] = None,
        encoding: str = "utf-8-sig",
        **kwargs,
    ) -> pd.DataFrame:
        file_path = Path(path)
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {path}")

        delimiter = sep or self._detect_delimiter(file_path, encoding)
        return pd.read_csv(file_path, sep=delimiter, encoding=encoding, **kwargs)

    def load_many(
        self,
        paths: Iterable[Path],
        *,
        sep: Optional[str] = None,
        encoding: str = "utf-8-sig",
        parallel: bool = False,
        max_workers: Optional[int] = None,
        **kwargs,
    ) -> pd.DataFrame:
        file_list = [Path(p) for p in paths]
        if not file_list:
            raise FileNotFoundError("No CSV files provided to load_many.")

        if parallel and len(file_list) > 1:
            workers = max_workers or min(len(file_list), multiprocessing.cpu_count())
            tasks = [
                (
                    str(path),
                    sep,
                    encoding,
                    kwargs,
                )
                for path in file_list
            ]
            with ProcessPoolExecutor(max_workers=workers) as executor:
                frames = list(executor.map(_read_csv_task, tasks))
        else:
            frames = [
                self.load(path, sep=sep, encoding=encoding, **kwargs)
                for path in file_list
            ]
        return pd.concat(frames, ignore_index=True, copy=False)

    def load_many_chunks(
        self,
        paths: Iterable[Path],
        *,
        sep: Optional[str] = None,
        encoding: str = "utf-8-sig",
        chunksize: int = 200_000,
        usecols=None,
        **kwargs,
    ) -> Iterator[pd.DataFrame]:
        file_list = [Path(p) for p in paths]
        if not file_list:
            raise FileNotFoundError("No CSV files provided to load_many_chunks.")

        for path in file_list:
            delimiter = sep or self._detect_delimiter(path, encoding)
            for chunk in pd.read_csv(
                path,
                sep=delimiter,
                encoding=encoding,
                chunksize=chunksize,
                usecols=usecols,
                **kwargs,
            ):
                yield chunk

    def peek_columns(self, path: Path, encoding: str = "utf-8-sig") -> Sequence[str]:
        file_path = Path(path)
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {path}")
        delimiter = self._detect_delimiter(file_path, encoding)
        with file_path.open("r", encoding=encoding, errors="ignore") as f:
            reader = csv.reader(f, delimiter=delimiter)
            row = next(reader, [])
        return row

    def load_directory(
        self,
        directory: Path,
        pattern: str = "*.csv",
        *,
        recursive: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        path_obj = Path(directory)
        if not path_obj.exists():
            raise FileNotFoundError(f"Directory not found: {directory}")

        files = sorted(path_obj.rglob(pattern) if recursive else path_obj.glob(pattern))
        if not files:
            raise FileNotFoundError(
                f"No CSV files matching pattern '{pattern}' in {directory}"
            )
        return self.load_many(files, **kwargs)

    def _detect_delimiter(self, path: Path, encoding: str) -> str:
        sample_size = 2048
        with path.open("r", encoding=encoding, errors="ignore") as f:
            sample = f.read(sample_size)
        if not sample:
            return ","
        try:
            dialect = csv.Sniffer().sniff(sample)
            return dialect.delimiter
        except csv.Error:
            return "|" if "|" in sample else ","
