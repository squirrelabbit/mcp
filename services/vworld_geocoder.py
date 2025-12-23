from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional

VWORLD_ENDPOINT = "https://api.vworld.kr/req/data"


def _get_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    return value


def _build_url(lon: float, lat: float) -> str:
    params = {
        "service": "data",
        "version": "2.0",
        "request": "GetFeature",
        "data": "LT_C_ADSIGG_INFO",
        "key": _get_env("MCP_VWORLD_KEY"),
        "domain": _get_env("MCP_VWORLD_DOMAIN"),
        "format": "json",
        "geometry": "false",
        "attribute": "true",
        "size": 1,
        "geomFilter": f"POINT({lon} {lat})",
    }
    return f"{VWORLD_ENDPOINT}?{urllib.parse.urlencode(params)}"


def _parse_response(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    response = payload.get("response") or {}
    result = response.get("result") or {}
    featureCollection = result.get("featureCollection") or {}
    features = featureCollection.get("features") or []

    if not features:
        return None
    props = features[0].get("properties") or {}
    return {
        "sig_cd": props.get("sig_cd"),
        "sig_kor_nm": props.get("sig_kor_nm"),
        "full_nm": props.get("full_nm"),
    }


def reverse_geocode_sig(lon: float, lat: float) -> Optional[Dict[str, Any]]:
    if not _get_env("MCP_VWORLD_KEY"):
        raise RuntimeError("MCP_VWORLD_KEY is not set")
    if not _get_env("MCP_VWORLD_DOMAIN"):
        raise RuntimeError("MCP_VWORLD_DOMAIN is not set")

    url = _build_url(lon, lat)
    req = urllib.request.Request(url, method="GET")

    retries = 3
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = resp.read().decode("utf-8")
            payload = json.loads(data)
            return _parse_response(payload)
        except urllib.error.HTTPError as exc:
            if exc.code in (429, 503) and attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise
        except urllib.error.URLError:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise
    return None
