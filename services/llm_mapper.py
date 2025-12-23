from __future__ import annotations

import json
import os
import re
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


def _dump_payload(kind: str, payload: Dict[str, Any], prompt: str) -> Path:
    target_dir = Path(os.getenv("MCP_LLM_DUMP_DIR", "llm_payloads/out"))
    target_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    path = target_dir / f"{timestamp}_{kind}.json"
    data = {"prompt": prompt, "payload": payload}
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def build_mapper_prompt(text: str) -> str:
    return f"""
너는 한국어 질의를 SQL 조회 파라미터로 변환한다.
다음 JSON 스키마로만 응답하라. 다른 텍스트는 금지.
{{
  "qtype": "trend|yoy|demographics",
  "level": "emd|sig|sido",
  "spatial_label": "지명 또는 null",
  "spatial_key": "코드 또는 null",
  "date_start": "YYYY-MM or YYYY-MM-DD or null",
  "date_end": "YYYY-MM or YYYY-MM-DD or null",
  "view": "v_activity_monthly_trend_norm|v_activity_monthly_trend_sig|v_activity_monthly_trend_sido|v_activity_yoy|v_demographics_share"
}}
질의: {text}
"""


def build_mapper_payload(prompt: str) -> Dict[str, Any]:
    return {
        "contents": [
            {
                "role": "user",
                "parts": [{"text": prompt}],
            }
        ],
        "generationConfig": {
            "temperature": 0.2,
        },
    }


def dump_mapper_payload(text: str) -> Path:
    prompt = build_mapper_prompt(text)
    payload = build_mapper_payload(prompt)
    return _dump_payload("mapper", payload, prompt)


def llm_map_query(text: str, *, dump: bool = True) -> Dict[str, Any]:
    api_key = os.getenv("MCP_GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("MCP_GEMINI_API_KEY가 필요합니다.")
    model = os.getenv("MCP_LLM_MODEL", "gemini-1.5-flash")
    endpoint = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model}:generateContent?key={api_key}"
    )

    prompt = build_mapper_prompt(text)
    payload = build_mapper_payload(prompt)
    if dump:
        _dump_payload("mapper", payload, prompt)
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        endpoint,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read().decode("utf-8")
    payload = json.loads(body)
    text_out = (
        payload.get("candidates", [{}])[0]
        .get("content", {})
        .get("parts", [{}])[0]
        .get("text", "")
    )
    match = re.search(r"\{.*\}", text_out, re.DOTALL)
    if not match:
        raise RuntimeError("LLM 응답에서 JSON을 찾지 못했습니다.")
    return json.loads(match.group(0))
