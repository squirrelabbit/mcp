import argparse
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from services.embedding_client import EmbeddingClient
from services.llm_client import LLMClient
from services.llm_mapper import llm_map_query
from tools import query_runner


PARSER_PROMPT = Path("docs/llm-parser-prompt.md")
OUTPUT_PROMPT = Path("docs/llm-output-prompt.md")
CACHE_ROOT = Path(".mcp_cache")
QUERY_CACHE_DIR = CACHE_ROOT / "query"
RESULT_CACHE_DIR = CACHE_ROOT / "result"
NARRATIVE_CACHE_DIR = CACHE_ROOT / "narrative"
CACHE_VERSION = "2024-12-17"
DATA_SOURCE_DIRS = [Path("data"), Path(".data")]
DB_DSN = os.getenv("MCP_DB_DSN", "postgresql://mcp:mcp@localhost:5432/mcp")
VECTOR_CACHE_TABLE = os.getenv("MCP_QUERY_VECTOR_TABLE", "query_mapping_cache")
VECTOR_SIM_THRESHOLD = float(os.getenv("MCP_QUERY_VECTOR_THRESHOLD", "0.92"))
VECTOR_TOP_K = int(os.getenv("MCP_QUERY_VECTOR_TOP_K", "5"))
VECTOR_LOOKUP_TIMEOUT_MS = int(os.getenv("MCP_QUERY_VECTOR_TIMEOUT_MS", "200"))

for _cache_dir in (QUERY_CACHE_DIR, RESULT_CACHE_DIR, NARRATIVE_CACHE_DIR):
    _cache_dir.mkdir(parents=True, exist_ok=True)


def _data_fingerprint(paths: Iterable[Path]) -> str:
    entries = []
    for base in paths:
        if not base.exists():
            continue
        for file in sorted(base.rglob("*.csv")):
            try:
                stat = file.stat()
            except FileNotFoundError:
                continue
            rel = file.relative_to(base)
            entries.append(f"{base.name}/{rel}:{stat.st_mtime_ns}:{stat.st_size}")
    if not entries:
        return "no-data"
    digest_source = "|".join(entries)
    return hashlib.sha256(digest_source.encode("utf-8")).hexdigest()


DATA_FINGERPRINT = _data_fingerprint(DATA_SOURCE_DIRS)


def load_prompt_template(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def build_parser_prompt(user_request: str, template: str) -> str:
    prefix = template or "사용자 요청을 MCP Query Schema JSON으로만 응답하세요."
    return f"{prefix}\n\n사용자 입력:\n{user_request}\n\nJSON:"


def build_output_prompt(
    result_json: Dict[str, Any], template: str, user_request: str
) -> str:
    template_text = template or (
        "당신은 MCP 분석 결과를 요약하는 어시스턴트입니다. "
        "사용자 요청과 하단 JSON을 분석하여 중요한 인사이트를 한국어 문장으로 작성하세요."
    )
    result_str = json.dumps(result_json, ensure_ascii=False, indent=2)
    return (
        f"{template_text}\n\n"
        f"사용자 요청:\n{user_request}\n\n"
        f"result.json:\n{result_str}\n\n"
        "요약:"
    )


def map_request_to_query(
    request: str,
    parser_model: str = "mock-parser",
    use_cache: bool = True,
    return_meta: bool = False,
    store_cache: bool = True,
    validate_schema: bool = True,
    vector_timeout_ms: Optional[int] = None,
) -> Union[Dict[str, Any], Tuple[Dict[str, Any], Dict[str, Any]]]:
    parser_template = load_prompt_template(PARSER_PROMPT)
    normalized_request = request.strip()

    query_obj: Optional[Dict[str, Any]] = None
    query_generated = False
    query_cache_key: Optional[str] = None
    request_embedding: List[float] = []
    cache_source = "miss"
    vector_similarity: Optional[float] = None
    embedding_error: Optional[str] = None
    llm_error: Optional[str] = None

    if use_cache:
        query_cache_key = _hash_components(
            "query",
            CACHE_VERSION,
            DATA_FINGERPRINT,
            parser_model,
            parser_template,
            normalized_request,
        )
        cached_query = _load_cache_json(QUERY_CACHE_DIR, query_cache_key)
        if cached_query is not None:
            query_obj = cached_query
            cache_source = "file"

    if query_obj is None and use_cache:
        try:
            embedding_client = EmbeddingClient()
            request_embedding = embedding_client.embed_text(normalized_request)
            cached_vector = _vector_cache_lookup(
                normalized_request,
                parser_model,
                parser_template,
                request_embedding,
                timeout_ms=vector_timeout_ms,
            )
            if cached_vector is not None:
                query_obj, vector_similarity = cached_vector
                cache_source = "vector"
        except Exception as exc:
            embedding_error = str(exc)
            request_embedding = []

    if query_obj is None:
        if parser_model in ("gemini", "gemini-1.5-flash", "gemini-1.5-pro"):
            try:
                query_obj = llm_map_query(request, dump=False)
                query_generated = True
                cache_source = "llm"
            except Exception as exc:
                llm_error = str(exc)
                query_obj = {}
                cache_source = "llm_error"
        if query_obj is None:
            parser_llm = LLMClient(
                provider=_mock_parser_provider, cache_dir=Path(".llm_cache/parser")
            )
            parser_prompt = build_parser_prompt(request, parser_template)
            parser_use_cache = use_cache and parser_model != "mock-parser"
            parser_response = parser_llm.call(
                parser_prompt, model=parser_model, use_cache=parser_use_cache
            )
            try:
                query_obj = json.loads(parser_response)
            except json.JSONDecodeError as exc:
                raise ValueError(f"LLM Parser JSON 파싱 실패: {exc}") from exc
            query_generated = True
            if cache_source == "llm_error":
                cache_source = "llm_fallback"
            else:
                cache_source = "llm"

    if validate_schema:
        schema = query_runner.load_schema()
        query_runner.validate_query(query_obj, schema)
    if use_cache and query_generated and query_cache_key:
        _save_cache_json(QUERY_CACHE_DIR, query_cache_key, query_obj)
    if use_cache and store_cache and query_generated and llm_error is None:
        if not request_embedding:
            try:
                embedding_client = EmbeddingClient()
                request_embedding = embedding_client.embed_text(normalized_request)
            except Exception as exc:
                embedding_error = str(exc)
                request_embedding = []
        if request_embedding:
            _vector_cache_store(
                normalized_request,
                parser_model,
                parser_template,
                request_embedding,
                query_obj,
            )

    if not return_meta:
        return query_obj

    meta = {
        "cache_source": cache_source,
        "vector_similarity": vector_similarity,
        "parser_model": parser_model,
        "query_generated": query_generated,
        "embedding_error": embedding_error,
        "embedding": request_embedding,
        "parser_template": parser_template,
        "llm_error": llm_error,
    }
    return query_obj, meta


def _cache_file_path(directory: Path, key: str) -> Path:
    return directory / f"{key}.json"


def _load_cache_json(directory: Path, key: str) -> Optional[Any]:
    path = _cache_file_path(directory, key)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _save_cache_json(directory: Path, key: str, payload: Any) -> None:
    path = _cache_file_path(directory, key)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _hash_components(*components: str) -> str:
    hasher = hashlib.sha256()
    for comp in components:
        hasher.update(str(comp).encode("utf-8"))
    return hasher.hexdigest()


def _canonical_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def _vector_literal(vector: List[float]) -> str:
    return "[" + ",".join(f"{value:.6f}" for value in vector) + "]"


def _vector_cache_lookup(
    request_text: str,
    parser_model: str,
    parser_template: str,
    embedding: List[float],
    *,
    timeout_ms: Optional[int] = None,
) -> Optional[Tuple[Dict[str, Any], float]]:
    if not embedding:
        return None
    try:
        import psycopg2
    except ImportError:
        return None

    template_hash = _hash_components("parser-template", parser_template)
    vector_literal = _vector_literal(embedding)
    timeout = VECTOR_LOOKUP_TIMEOUT_MS if timeout_ms is None else int(timeout_ms)
    try:
        with psycopg2.connect(DB_DSN) as conn:
            with conn.cursor() as cur:
                if timeout > 0:
                    cur.execute(f"SET LOCAL statement_timeout = {timeout}")
                cur.execute(
                    f"""
                    SELECT query_json, 1 - (embedding <=> %s::vector) AS similarity
                    FROM {VECTOR_CACHE_TABLE}
                    WHERE cache_version = %s
                      AND parser_model = %s
                      AND parser_template_hash = %s
                    ORDER BY embedding <=> %s::vector
                    LIMIT %s
                    """,
                    (
                        vector_literal,
                        CACHE_VERSION,
                        parser_model,
                        template_hash,
                        vector_literal,
                        VECTOR_TOP_K,
                    ),
                )
                rows = cur.fetchall()
    except Exception:
        return None

    for query_json, similarity in rows:
        if similarity is not None and similarity >= VECTOR_SIM_THRESHOLD:
            if isinstance(query_json, str):
                try:
                    return json.loads(query_json), float(similarity)
                except json.JSONDecodeError:
                    return None
            return query_json, float(similarity)
    return None


def _vector_cache_store(
    request_text: str,
    parser_model: str,
    parser_template: str,
    embedding: List[float],
    query_obj: Dict[str, Any],
) -> None:
    if not embedding:
        return
    try:
        import psycopg2
    except ImportError:
        return

    template_hash = _hash_components("parser-template", parser_template)
    vector_literal = _vector_literal(embedding)
    request_hash = _hash_components("request", request_text)
    try:
        with psycopg2.connect(DB_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {VECTOR_CACHE_TABLE} (
                        request_hash,
                        request_text,
                        parser_model,
                        parser_template_hash,
                        cache_version,
                        query_json,
                        embedding
                    )
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::vector)
                    ON CONFLICT (request_hash) DO UPDATE SET
                        request_text = EXCLUDED.request_text,
                        parser_model = EXCLUDED.parser_model,
                        parser_template_hash = EXCLUDED.parser_template_hash,
                        cache_version = EXCLUDED.cache_version,
                        query_json = EXCLUDED.query_json,
                        embedding = EXCLUDED.embedding,
                        updated_at = NOW()
                    """,
                    (
                        request_hash,
                        request_text,
                        parser_model,
                        template_hash,
                        CACHE_VERSION,
                        json.dumps(query_obj, ensure_ascii=False),
                        vector_literal,
                    ),
                )
    except Exception:
        return


def store_query_vector_cache(
    request_text: str,
    parser_model: str,
    parser_template: str,
    embedding: List[float],
    query_obj: Dict[str, Any],
) -> None:
    _vector_cache_store(
        request_text, parser_model, parser_template, embedding, query_obj
    )


def run_assistant(
    request: str,
    parser_model: str = "mock-parser",
    writer_model: str = "mock-writer",
    skip_writer: bool = False,
    use_cache: bool = True,
) -> Dict[str, Any]:
    output_template = load_prompt_template(OUTPUT_PROMPT)
    normalized_request = request.strip()
    query_obj = map_request_to_query(
        request, parser_model=parser_model, use_cache=use_cache
    )

    result: Optional[Dict[str, Any]] = None
    result_cache_key: Optional[str] = None
    if use_cache:
        query_fingerprint = _canonical_json(query_obj)
        result_cache_key = _hash_components(
            "result", CACHE_VERSION, DATA_FINGERPRINT, query_fingerprint
        )
        cached_result = _load_cache_json(RESULT_CACHE_DIR, result_cache_key)
        if cached_result is not None:
            result = cached_result

    if result is None:
        result = query_runner.run_query(query_obj)
        if use_cache and result_cache_key:
            _save_cache_json(RESULT_CACHE_DIR, result_cache_key, result)

    response = {
        "request": request,
        "query": query_obj,
        "mcp_result": result,
        "llm_summary": None,
    }

    if skip_writer:
        return response

    narrative_cache_key: Optional[str] = None
    if use_cache:
        narrative_input = _canonical_json(result)
        narrative_cache_key = _hash_components(
            "narrative",
            CACHE_VERSION,
            DATA_FINGERPRINT,
            writer_model,
            output_template,
            narrative_input,
        )
        cached_narrative = _load_cache_json(NARRATIVE_CACHE_DIR, narrative_cache_key)
        if cached_narrative is not None:
            response["llm_summary"] = cached_narrative
            return response

    writer_llm = LLMClient(
        provider=_mock_writer_provider, cache_dir=Path(".llm_cache/writer")
    )
    output_prompt = build_output_prompt(result, output_template, normalized_request)
    writer_use_cache = use_cache and writer_model != "mock-writer"
    summary = writer_llm.call(
        output_prompt, model=writer_model, use_cache=writer_use_cache
    )
    response["llm_summary"] = summary

    if use_cache and narrative_cache_key:
        _save_cache_json(NARRATIVE_CACHE_DIR, narrative_cache_key, summary)
    return response


def main():
    parser = argparse.ArgumentParser(
        description="LLM 기반 자연어 → MCP Query → 결과 요약 파이프라인"
    )
    parser.add_argument("request", help="사용자 자연어 요청 문장")
    parser.add_argument("--parser-model", default="mock-parser")
    parser.add_argument("--writer-model", default="mock-writer")
    parser.add_argument("--skip-writer", action="store_true", help="결과 요약 LLM 생략")
    parser.add_argument(
        "--no-cache", action="store_true", help="LLM 캐시를 사용하지 않음"
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="결과를 저장할 JSON 파일 경로(미지정 시 stdout)",
    )
    args = parser.parse_args()
    try:
        response = run_assistant(
            args.request,
            parser_model=args.parser_model,
            writer_model=args.writer_model,
            skip_writer=args.skip_writer,
            use_cache=not args.no_cache,
        )
    except ValueError as exc:
        raise SystemExit(str(exc))

    output_json = json.dumps(response, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output_json, encoding="utf-8")
        print(f"결과를 저장했습니다: {args.output}")
    else:
        print(output_json)


def _mock_parser_provider(prompt: str, model: str) -> str:
    """간단한 규칙 기반 JSON 반환 (LLM 연결 전까지 사용)."""
    user_text = prompt
    marker = "사용자 입력:"
    if marker in prompt:
        after = prompt.split(marker, 1)[1]
        if "\n\nJSON:" in after:
            user_text = after.split("\n\nJSON:", 1)[0].strip()
        else:
            user_text = after.strip()

    lowered = user_text.lower()

    if "경제" in user_text or "매출" in user_text or "sales" in lowered:
        target = "economic.sales"
    elif "인구" in user_text or "유동" in user_text or "foot" in lowered:
        target = "population.foot_traffic"
    else:
        target = "metrics.uplift" if "uplift" in lowered else "population.foot_traffic"

    aggregations = ["avg"]
    if "총" in user_text or "합계" in user_text or "sum" in lowered:
        aggregations = ["sum"]
    elif "최대" in user_text or "max" in lowered:
        aggregations = ["max"]
    elif "최소" in user_text or "min" in lowered:
        aggregations = ["min"]

    filters: Dict[str, Any] = {}
    time_filter: Dict[str, Any] = {}
    month_match = re.search(r"(\d{1,2})월", user_text)
    if month_match:
        time_filter["month"] = max(1, min(12, int(month_match.group(1))))
    if "주말" in user_text:
        time_filter["weekdays"] = ["SAT", "SUN"]
    elif "평일" in user_text:
        time_filter["weekdays"] = ["MON", "TUE", "WED", "THU", "FRI"]
    if time_filter:
        filters["time"] = time_filter

    query: Dict[str, Any] = {"target": target, "aggregations": aggregations}
    if filters:
        query["filters"] = filters
    return json.dumps(query, ensure_ascii=False, indent=2)


def _mock_writer_provider(prompt: str, model: str) -> str:
    return "[MOCK SUMMARY]\n요청하신 분석 결과 요약입니다."


if __name__ == "__main__":
    main()
