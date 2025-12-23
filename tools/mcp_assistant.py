import argparse
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from services.llm_client import LLMClient
from tools import query_runner


PARSER_PROMPT = Path("docs/llm-parser-prompt.md")
OUTPUT_PROMPT = Path("docs/llm-output-prompt.md")
CACHE_ROOT = Path(".mcp_cache")
QUERY_CACHE_DIR = CACHE_ROOT / "query"
RESULT_CACHE_DIR = CACHE_ROOT / "result"
NARRATIVE_CACHE_DIR = CACHE_ROOT / "narrative"
CACHE_VERSION = "2024-12-17"
DATA_SOURCE_DIRS = [Path("data"), Path(".data")]

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


def build_output_prompt(result_json: Dict[str, Any], template: str) -> str:
    template_text = template or (
        "당신은 MCP 분석 결과를 요약하는 어시스턴트입니다. "
        "하단 JSON을 분석하여 중요한 인사이트를 한국어 문장으로 작성하세요."
    )
    result_str = json.dumps(result_json, ensure_ascii=False, indent=2)
    return f"{template_text}\n\nresult.json:\n{result_str}\n\n요약:"


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


def run_assistant(
    request: str,
    parser_model: str = "mock-parser",
    writer_model: str = "mock-writer",
    skip_writer: bool = False,
    use_cache: bool = True,
) -> Dict[str, Any]:
    parser_template = load_prompt_template(PARSER_PROMPT)
    output_template = load_prompt_template(OUTPUT_PROMPT)
    normalized_request = request.strip()

    query_obj: Optional[Dict[str, Any]] = None
    query_generated = False
    query_cache_key: Optional[str] = None
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

    if query_obj is None:
        parser_llm = LLMClient(provider=_mock_parser_provider, cache_dir=Path(".llm_cache/parser"))
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

    schema = query_runner.load_schema()
    query_runner.validate_query(query_obj, schema)
    if use_cache and query_generated and query_cache_key:
        _save_cache_json(QUERY_CACHE_DIR, query_cache_key, query_obj)

    result: Optional[Dict[str, Any]] = None
    result_cache_key: Optional[str] = None
    if use_cache:
        query_fingerprint = _canonical_json(query_obj)
        result_cache_key = _hash_components("result", CACHE_VERSION, DATA_FINGERPRINT, query_fingerprint)
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

    writer_llm = LLMClient(provider=_mock_writer_provider, cache_dir=Path(".llm_cache/writer"))
    output_prompt = build_output_prompt(result, output_template)
    writer_use_cache = use_cache and writer_model != "mock-writer"
    summary = writer_llm.call(output_prompt, model=writer_model, use_cache=writer_use_cache)
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
    parser.add_argument("--no-cache", action="store_true", help="LLM 캐시를 사용하지 않음")
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
