from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple


def _enabled() -> bool:
    return os.getenv("MCP_LLM_PROVIDER", "").lower() == "gemini" and bool(
        os.getenv("MCP_GEMINI_API_KEY")
    )


def _dump_payload(kind: str, payload: dict, prompt: str) -> None:
    target_dir = Path(os.getenv("MCP_LLM_DUMP_DIR", "llm_payloads/out"))
    target_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    path = target_dir / f"{timestamp}_{kind}.json"
    data = {"prompt": prompt, "payload": payload}
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _call_gemini(prompt: str, *, kind: str) -> str:
    api_key = os.getenv("MCP_GEMINI_API_KEY")
    model = os.getenv("MCP_LLM_MODEL", "gemini-1.5-flash")
    endpoint = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model}:generateContent?key={api_key}"
    )

    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [{"text": prompt}],
            }
        ],
        "generationConfig": {"temperature": 0.2},
    }
    _dump_payload(kind, payload, prompt)
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        endpoint,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    retries = 3
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read().decode("utf-8")
            response = json.loads(body)
            text_out = (
                response.get("candidates", [{}])[0]
                .get("content", {})
                .get("parts", [{}])[0]
                .get("text", "")
            )
            return text_out.strip()
        except urllib.error.HTTPError as exc:
            if exc.code == 429 and attempt < retries - 1:
                time.sleep(2**attempt)
                continue
            raise
        except urllib.error.URLError:
            if attempt < retries - 1:
                time.sleep(2**attempt)
                continue
            raise
    return ""


def llm_narrate(
    question: str,
    cols: List[str],
    rows: List[Tuple[Any, ...]],
    top_cols: List[str],
    top_rows: List[Tuple[Any, ...]],
) -> str:
    sample_rows = [dict(zip(cols, row)) for row in rows[:20]]
    top_rows_mapped = [dict(zip(top_cols, row)) for row in top_rows[:10]]
    payload_text = json.dumps(
        {
            "question": question,
            "sample_rows": sample_rows,
            "top_rows": top_rows_mapped,
        },
        ensure_ascii=False,
    )

    prompt = f"""
다음은 질의 결과 샘플이다. 한국어로 2~3문장 요약을 작성하라.
과장하지 말고, 수치가 있으면 언급하라.
{payload_text}
"""

    if not _enabled():
        if os.getenv("MCP_LLM_DUMP_ALWAYS", "1") == "1":
            payload = {
                "contents": [
                    {"role": "user", "parts": [{"text": prompt}]},
                ],
                "generationConfig": {"temperature": 0.2},
            }
            _dump_payload("narrator", payload, prompt)
        return "요약할 데이터가 없습니다."

    try:
        text_out = _call_gemini(prompt, kind="narrator")
        return text_out or "요약할 데이터가 없습니다."
    except Exception:
        return "요약할 데이터가 없습니다."


def llm_narrate_dataset(summary: dict) -> str:
    payload_text = json.dumps(summary, ensure_ascii=False)
    prompt = f"""
다음은 데이터베이스 요약 정보다. 제공된 숫자만 사용해서 한국어로 3~6문장 요약을 작성하라.
없는 정보는 추정하지 말고 "알 수 없음"이라고 쓴다.
형식 예시: "현재 DB는 [기간], [지역/소스 특성] 데이터입니다."
{payload_text}
"""

    if not _enabled():
        if os.getenv("MCP_LLM_DUMP_ALWAYS", "1") == "1":
            payload = {
                "contents": [
                    {"role": "user", "parts": [{"text": prompt}]},
                ],
                "generationConfig": {"temperature": 0.2},
            }
            _dump_payload("dataset", payload, prompt)
        return "요약할 데이터가 없습니다."

    try:
        text_out = _call_gemini(prompt, kind="dataset")
        return text_out or "요약할 데이터가 없습니다."
    except Exception:
        return "요약할 데이터가 없습니다."


def llm_narrate_insight(question: str, payload: Dict[str, Any]) -> str:
    payload_text = json.dumps(
        {"question": question, "insights": payload}, ensure_ascii=False
    )
    prompt = f"""
다음은 시스템이 계산한 인사이트 후보 정보다. 한국어로 4~6문장 요약을 작성하라.
규칙:
- 제공된 수치만 사용하고 없는 원인은 추정하지 말 것
- 비교 기준(전월/전년/평균/상위비교 등)을 반드시 언급
- 표현은 L2~L4 수준(비교/해석/시사점)만 작성
- 확정적 원인 단정 금지, "가능성" 수준으로만 표현
{payload_text}
"""

    if not _enabled():
        if os.getenv("MCP_LLM_DUMP_ALWAYS", "1") == "1":
            payload_req = {
                "contents": [
                    {"role": "user", "parts": [{"text": prompt}]},
                ],
                "generationConfig": {"temperature": 0.2},
            }
            _dump_payload("insight", payload_req, prompt)
        return "요약할 데이터가 없습니다."

    try:
        text_out = _call_gemini(prompt, kind="insight")
        return text_out or "요약할 데이터가 없습니다."
    except Exception:
        return "요약할 데이터가 없습니다."
