import hashlib
import os
import pickle
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Type

from domain.base_module import BaseDomainModule
from loaders.csv_loader import CSVLoader


CACHE_DIR = Path(".mcp_cache/domain")
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def collect_files(patterns: Sequence[str], data_sources: Iterable[Path]) -> List[Path]:
    files: List[Path] = []
    for directory in data_sources:
        if not directory.exists():
            continue
        for pattern in patterns:
            files.extend(sorted(directory.glob(pattern)))
    return files


def compute_fingerprint(files: Sequence[Path]) -> str:
    entries = []
    for file in files:
        try:
            stat = file.stat()
        except FileNotFoundError:
            continue
        entries.append(f"{file}:{stat.st_mtime_ns}:{stat.st_size}")
    digest = "|".join(entries)
    return hashlib.sha256(digest.encode("utf-8")).hexdigest()


def cache_path(domain_name: str, fingerprint: str) -> Path:
    return CACHE_DIR / f"{domain_name}_{fingerprint}.pkl"


def load_cached(domain_name: str, fingerprint: str):
    path = cache_path(domain_name, fingerprint)
    if not path.exists():
        return None
    with path.open("rb") as f:
        return pickle.load(f)


def save_cache(domain_name: str, fingerprint: str, records):
    path = cache_path(domain_name, fingerprint)
    with path.open("wb") as f:
        pickle.dump(records, f)
    # clean old caches for same domain
    for old_file in CACHE_DIR.glob(f"{domain_name}_*.pkl"):
        if old_file == path:
            continue
        old_file.unlink(missing_ok=True)


def load_domain_records(
    domain_name: str,
    patterns: Sequence[str],
    module_cls: Type[BaseDomainModule],
    data_sources: Iterable[Path],
    *,
    parallel: bool = True,
    chunked: bool = False,
    chunksize: Optional[int] = None,
) -> List[dict]:
    files = collect_files(patterns, data_sources)
    if not files:
        return []

    fingerprint = compute_fingerprint(files)
    cached = load_cached(domain_name, fingerprint)
    if cached is not None:
        print(f"[cache] hit: {domain_name} ({len(cached)} records)")
        return cached

    print(f"[cache] miss: {domain_name} → processing {len(files)} files")
    loader = CSVLoader()
    module = module_cls()
    if chunked and hasattr(module, "normalize_chunks"):
        env_chunksize = os.getenv("MCP_CSV_CHUNKSIZE")
        resolved_chunksize = chunksize or (int(env_chunksize) if env_chunksize else 200_000)
        usecols = None
        if hasattr(module, "select_columns"):
            header = loader.peek_columns(files[0])
            usecols = module.select_columns(header)
        frames = loader.load_many_chunks(files, chunksize=resolved_chunksize, usecols=usecols)
        records = module.normalize_chunks(frames)
    else:
        df = loader.load_many(files, parallel=parallel)
        records = module.normalize(df)
    save_cache(domain_name, fingerprint, records)
    return records
