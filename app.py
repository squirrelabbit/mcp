from __future__ import annotations

import html
import hashlib
import os
import re
import json
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from fastapi import BackgroundTasks, FastAPI, Form
from fastapi.responses import HTMLResponse, JSONResponse

from services.llm_mapper import dump_mapper_payload, llm_map_query
from services.llm_narrator import (
    llm_narrate,
    llm_narrate_dataset,
    llm_narrate_insight,
)
from services.insight_builder import (
    build_dataset_insight_payload,
    build_global_baseline,
    build_insight_payload,
)
from tools.mcp_assistant import map_request_to_query, store_query_vector_cache

DSN = os.getenv("MCP_DB_DSN", "postgresql://mcp:mcp@localhost:5432/mcp")

app = FastAPI(title="MCP Query API")


def _parse_date_token(token: str) -> Optional[date]:
    token = token.strip()
    if re.match(r"^\d{4}-\d{2}$", token):
        token = f"{token}-01"
    try:
        return date.fromisoformat(token)
    except ValueError:
        return None


def _extract_date_range(text: str) -> Tuple[Optional[date], Optional[date]]:
    matches = re.findall(r"\d{4}-\d{2}(?:-\d{2})?", text)
    if not matches:
        korean = re.findall(r"(\d{4})\s*년?\s*(\d{1,2})\s*월", text)
        if not korean:
            return None, None
        if len(korean) == 1:
            year, month = korean[0]
            return _parse_date_token(f"{year}-{int(month):02d}"), _parse_date_token(
                f"{year}-{int(month):02d}"
            )
        start_year, start_month = korean[0]
        end_year, end_month = korean[1]
        return (
            _parse_date_token(f"{start_year}-{int(start_month):02d}"),
            _parse_date_token(f"{end_year}-{int(end_month):02d}"),
        )
    if len(matches) == 1:
        start = _parse_date_token(matches[0])
        return start, start
    start = _parse_date_token(matches[0])
    end = _parse_date_token(matches[1])
    return start, end


def _extract_spatial_key(text: str) -> Optional[str]:
    match = re.search(r"\b\d{10,}\b", text)
    return match.group(0) if match else None


def _extract_spatial_label(text: str) -> Optional[str]:
    match = re.search(r"([가-힣]+(?:시|군|구|읍|면|동))", text)
    return match.group(1) if match else None


def _extract_sex(text: str) -> Optional[str]:
    if "여성" in text or "여자" in text:
        return "female"
    if "남성" in text or "남자" in text:
        return "male"
    return None


def _extract_age_like(text: str) -> Optional[str]:
    match = re.search(r"(\d{2})대", text)
    if not match:
        return None
    decade = match.group(1)
    return f"{decade}_%"


def _extract_level(text: str) -> str:
    if "도별" in text or "시도" in text:
        return "sido"
    if "시별" in text or "군별" in text or "구별" in text or "시군구" in text:
        return "sig"
    if "동별" in text or "읍면동" in text:
        return "emd"
    return "emd"


def _classify_query(text: str) -> str:
    if "전년" in text or "yoy" in text:
        return "yoy"
    if "비중" in text or "demographic" in text or "연령" in text or "성별" in text:
        return "demographics"
    return "trend"


def _has_filter_tokens(text: str) -> bool:
    tokens = ["주말", "평일", "날씨", "이벤트", "축제", "공휴일"]
    return any(token in text for token in tokens)


def _is_global_context(text: str, context: Dict[str, Any]) -> bool:
    if context.get("spatial_label") or context.get("spatial_key"):
        return False
    if context.get("date_start") or context.get("date_end"):
        return False
    if _extract_sex(text) or _extract_age_like(text):
        return False
    if _has_filter_tokens(text):
        return False
    return True


def _select_view(level: str) -> str:
    if level == "sido":
        return "v_activity_monthly_trend_sido"
    if level == "sig":
        return "v_activity_monthly_trend_sig"
    return "v_activity_monthly_trend_norm"


def _select_insight_view(level: str) -> str:
    if level == "sido":
        return "v_insight_candidate_sido"
    if level == "sig":
        return "v_insight_candidate_sig"
    return "v_insight_candidate_norm"


def _llm_enabled() -> bool:
    return os.getenv("MCP_LLM_PROVIDER", "").lower() == "gemini" and bool(
        os.getenv("MCP_GEMINI_API_KEY")
    )


def _normalize_date(value: Optional[str]) -> Optional[date]:
    if value is None:
        return None
    return _parse_date_token(value)


def _build_query_context(text: str) -> Dict[str, Any]:
    mapped: Dict[str, Any] = {}
    if _llm_enabled():
        try:
            mapped = llm_map_query(text)
        except Exception:
            mapped = {}

    qtype = mapped.get("qtype") or _classify_query(text)
    level = mapped.get("level") or _extract_level(text)
    spatial_label = mapped.get("spatial_label") or _extract_spatial_label(text)
    spatial_key = mapped.get("spatial_key") or _extract_spatial_key(text)
    start_date = _normalize_date(mapped.get("date_start"))
    end_date = _normalize_date(mapped.get("date_end"))
    if start_date is None and end_date is None:
        start_date, end_date = _extract_date_range(text)
    view = mapped.get("view")
    if view not in {
        "v_activity_monthly_trend_norm",
        "v_activity_monthly_trend_sig",
        "v_activity_monthly_trend_sido",
        "v_activity_yoy",
        "v_demographics_share",
    }:
        view = _select_view(level)

    return {
        "qtype": qtype,
        "level": level,
        "spatial_label": spatial_label,
        "spatial_key": spatial_key,
        "date_start": start_date,
        "date_end": end_date,
        "view": view,
    }


def _build_query_context_from_mapping(mapping: Dict[str, Any]) -> Dict[str, Any]:
    qtype = mapping.get("qtype") or "trend"
    level = mapping.get("level") or "emd"
    spatial_label = mapping.get("spatial_label")
    spatial_key = mapping.get("spatial_key")
    start_date = _normalize_date(mapping.get("date_start"))
    end_date = _normalize_date(mapping.get("date_end"))
    view = mapping.get("view")
    if view not in {
        "v_activity_monthly_trend_norm",
        "v_activity_monthly_trend_sig",
        "v_activity_monthly_trend_sido",
        "v_activity_yoy",
        "v_demographics_share",
    }:
        view = _select_view(level)
    return {
        "qtype": qtype,
        "level": level,
        "spatial_label": spatial_label,
        "spatial_key": spatial_key,
        "date_start": start_date,
        "date_end": end_date,
        "view": view,
    }


def _build_timeseries_query(text: str) -> Tuple[str, List[Any], str]:
    context = _build_query_context(text)
    qtype = context["qtype"]
    spatial_key = context["spatial_key"]
    spatial_label = context["spatial_label"]
    start_date = context["date_start"]
    end_date = context["date_end"]
    view_name = context["view"]
    params: List[Any] = []

    if qtype == "yoy":
        sql = "SELECT * FROM v_activity_yoy WHERE 1=1"
        if spatial_key:
            sql += " AND spatial_key = %s"
            params.append(spatial_key)
        if spatial_label and not spatial_key:
            sql += " AND spatial_label = %s"
            params.append(spatial_label)
        if start_date:
            sql += " AND date >= %s"
            params.append(start_date)
        if end_date:
            sql += " AND date <= %s"
            params.append(end_date)
        sql += " ORDER BY date"
        return sql, params, "yoy"

    if qtype == "demographics":
        sql = "SELECT * FROM v_demographics_share WHERE 1=1"
        if spatial_key:
            sql += " AND spatial_key = %s"
            params.append(spatial_key)
        if spatial_label and not spatial_key:
            sql += " AND spatial_label = %s"
            params.append(spatial_label)
        if start_date:
            sql += " AND date >= %s"
            params.append(start_date)
        if end_date:
            sql += " AND date <= %s"
            params.append(end_date)
        sex = _extract_sex(text)
        if sex:
            sql += " AND sex = %s"
            params.append(sex)
        age_like = _extract_age_like(text)
        if age_like:
            sql += " AND age_group LIKE %s"
            params.append(age_like)
        sql += " ORDER BY date"
        return sql, params, "demographics"

    sql = f"SELECT * FROM {view_name} WHERE 1=1"
    if spatial_label:
        sql += " AND spatial_label = %s"
        params.append(spatial_label)
    elif spatial_key:
        sql += " AND spatial_label = %s"
        params.append(spatial_key)
    if start_date:
        sql += " AND date >= %s"
        params.append(start_date)
    if end_date:
        sql += " AND date <= %s"
        params.append(end_date)
    sql += " ORDER BY date"
    return sql, params, "trend"


def _build_insight_query(text: str) -> Tuple[str, List[Any]]:
    context = _build_query_context(text)
    spatial_label = context["spatial_label"]
    spatial_key = context["spatial_key"]
    start_date = context["date_start"]
    end_date = context["date_end"]
    view_name = _select_insight_view(context["level"])
    params: List[Any] = []

    sql = f"SELECT * FROM {view_name} WHERE 1=1"
    if spatial_label:
        sql += " AND spatial_label = %s"
        params.append(spatial_label)
    elif spatial_key:
        sql += " AND spatial_label = %s"
        params.append(spatial_key)
    if start_date:
        sql += " AND date >= %s"
        params.append(start_date)
    if end_date:
        sql += " AND date <= %s"
        params.append(end_date)
    sql += " ORDER BY date"
    return sql, params


def _build_insight_query_from_mapping(mapping: Dict[str, Any]) -> Tuple[str, List[Any]]:
    context = _build_query_context_from_mapping(mapping)
    spatial_label = context["spatial_label"]
    spatial_key = context["spatial_key"]
    start_date = context["date_start"]
    end_date = context["date_end"]
    view_name = _select_insight_view(context["level"])
    params: List[Any] = []

    sql = f"SELECT * FROM {view_name} WHERE 1=1"
    if spatial_label:
        sql += " AND spatial_label = %s"
        params.append(spatial_label)
    elif spatial_key:
        sql += " AND spatial_label = %s"
        params.append(spatial_key)
    if start_date:
        sql += " AND date >= %s"
        params.append(start_date)
    if end_date:
        sql += " AND date <= %s"
        params.append(end_date)
    sql += " ORDER BY date"
    return sql, params


def _build_topn_query(text: str) -> Tuple[str, List[Any]]:
    context = _build_query_context(text)
    spatial_key = context["spatial_key"]
    spatial_label = context["spatial_label"]
    start_date = context["date_start"]
    end_date = context["date_end"]
    view_name = context["view"]
    params: List[Any] = []
    sql = (
        f"SELECT spatial_label, SUM(foot_traffic) AS foot_traffic, "
        f"SUM(sales) AS sales FROM {view_name} WHERE 1=1"
    )
    if spatial_label:
        sql += " AND spatial_label = %s"
        params.append(spatial_label)
    elif spatial_key:
        sql += " AND spatial_label = %s"
        params.append(spatial_key)
    if start_date:
        sql += " AND date >= %s"
        params.append(start_date)
    if end_date:
        sql += " AND date <= %s"
        params.append(end_date)
    sql += " GROUP BY spatial_label ORDER BY foot_traffic DESC NULLS LAST LIMIT 10"
    return sql, params


def _build_geo_query(text: str) -> Tuple[str, List[Any]]:
    context = _build_query_context(text)
    spatial_key = context["spatial_key"]
    spatial_label = context["spatial_label"]
    start_date = context["date_start"]
    end_date = context["date_end"]
    level = context["level"]
    params: List[Any] = []
    if level == "sido":
        label_expr = "COALESCE(sig.sido_name, d.spatial_label, a.spatial_key)"
    elif level == "sig":
        label_expr = "COALESCE(sig.sig_name, d.spatial_label, a.spatial_key)"
    else:
        label_expr = "COALESCE(d.spatial_label, a.spatial_key)"
    sql = f"""
        SELECT
            {label_expr} AS spatial_label,
            AVG(d.lat) AS lat,
            AVG(d.lon) AS lon,
            SUM(a.foot_traffic) AS foot_traffic,
            SUM(a.sales) AS sales
        FROM gold_activity a
        LEFT JOIN dim_spatial d ON d.spatial_key = a.spatial_key
        LEFT JOIN admin_sig sig ON sig.sig_code = LEFT(d.code, 5)
        WHERE a.granularity = 'month'
    """
    if spatial_label:
        sql += " AND d.spatial_label = %s"
        params.append(spatial_label)
    elif spatial_key:
        sql += " AND (d.spatial_label = %s OR a.spatial_key = %s)"
        params.extend([spatial_key, spatial_key])
    if start_date:
        sql += " AND a.date >= %s"
        params.append(start_date)
    if end_date:
        sql += " AND a.date <= %s"
        params.append(end_date)
    sql += " GROUP BY 1"
    return sql, params


def _execute(sql: str, params: List[Any]) -> Tuple[List[str], List[Tuple[Any, ...]]]:
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
    return cols, rows


def _cache_key(sql: str, params: List[Any]) -> str:
    blob = json.dumps({"sql": sql, "params": params}, default=str, sort_keys=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _cache_lookup(sql: str, params: List[Any]) -> Optional[Dict[str, Any]]:
    cache_key = _cache_key(sql, params)
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT response_json FROM query_cache WHERE cache_key = %s",
                (cache_key,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return row[0]


def _cache_store(sql: str, params: List[Any], response: Dict[str, Any]) -> None:
    cache_key = _cache_key(sql, params)
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO query_cache (cache_key, sql_text, params_json, response_json)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (cache_key) DO UPDATE SET
                  response_json = EXCLUDED.response_json,
                  updated_at = NOW()
                """,
                (cache_key, sql, json.dumps(params, default=str), json.dumps(response, default=str)),
            )


def _render_table(cols: List[str], rows: List[Tuple[Any, ...]]) -> str:
    if not rows:
        return "<p>결과가 없습니다.</p>"
    head = "".join(f"<th>{html.escape(c)}</th>" for c in cols)
    body_rows = []
    for row in rows[:200]:
        cells = "".join(f"<td>{html.escape(str(v))}</td>" for v in row)
        body_rows.append(f"<tr>{cells}</tr>")
    body = "".join(body_rows)
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return """
    <html>
      <head>
        <title>MCP Query</title>
        <style>
          body { font-family: Arial, sans-serif; margin: 24px; }
          textarea { width: 100%; height: 120px; }
          table { border-collapse: collapse; margin-top: 16px; width: 100%; }
          th, td { border: 1px solid #ccc; padding: 6px 8px; text-align: left; }
        </style>
      </head>
      <body>
        <h2>MCP 자연어 질의</h2>
        <form method="post" action="/mapping">
          <textarea name="q" placeholder="예: 2023-08 강남구 월별 트렌드 보여줘"></textarea><br/>
          <button type="submit">쿼리 매핑 파일 생성</button>
        </form>
        <h2>쿼리 매핑 JSON 입력</h2>
        <form method="post" action="/insight_from_mapping">
          <textarea name="mapping_json" placeholder='{"qtype":"trend","level":"sig","spatial_label":"강남구","date_start":"2023-10","date_end":"2023-10"}'></textarea><br/>
          <button type="submit">인사이트 생성</button>
        </form>
      </body>
    </html>
    """


@app.post("/query", response_class=HTMLResponse)
def query(q: str = Form(...)) -> str:
    sql, params, qtype = _build_timeseries_query(q)
    cached = _cache_lookup(sql, params)
    if cached:
        cols = cached.get("cols", [])
        rows = cached.get("rows", [])
        top_cols = cached.get("top_cols", [])
        top_rows = cached.get("top_rows", [])
        geo_cols = cached.get("geo_cols", [])
        geo_rows = cached.get("geo_rows", [])
        narrative = cached.get("narrative", "요약할 데이터가 없습니다.")
    else:
        cols, rows = _execute(sql, params)
        top_sql, top_params = _build_topn_query(q)
        top_cols, top_rows = _execute(top_sql, top_params)
        geo_sql, geo_params = _build_geo_query(q)
        geo_cols, geo_rows = _execute(geo_sql, geo_params)

        narrative = "요약할 데이터가 없습니다."
        if rows:
            narrative = llm_narrate(q, cols, rows, top_cols, top_rows)

        _cache_store(
            sql,
            params,
            {
                "cols": cols,
                "rows": rows,
                "top_cols": top_cols,
                "top_rows": top_rows,
                "geo_cols": geo_cols,
                "geo_rows": geo_rows,
                "narrative": narrative,
            },
        )

    table = _render_table(cols, rows)
    top_table = _render_table(top_cols, top_rows)
    geo_table = _render_table(geo_cols, geo_rows)
    return f"""
    <html>
      <head>
        <title>MCP Query Result</title>
        <style>
          body {{ font-family: Arial, sans-serif; margin: 24px; }}
          table {{ border-collapse: collapse; margin-top: 16px; width: 100%; }}
          th, td {{ border: 1px solid #ccc; padding: 6px 8px; text-align: left; }}
          pre {{ background: #f6f6f6; padding: 8px; }}
        </style>
      </head>
      <body>
        <h2>Query</h2>
        <pre>{html.escape(q)}</pre>
        <h3>SQL</h3>
        <pre>{html.escape(sql)}</pre>
        <h3>요약</h3>
        <pre>{html.escape(narrative)}</pre>
        <h3>Top 10</h3>
        {top_table}
        <h3>GeoJSON (point)</h3>
        {geo_table}
        <h3>Time Series</h3>
        {table}
        <p><a href="/">돌아가기</a></p>
      </body>
    </html>
    """


@app.post("/mapping", response_class=HTMLResponse)
def mapping(q: str = Form(...), background_tasks: BackgroundTasks = None) -> str:
    parser_model = os.getenv("MCP_PARSER_MODEL", "gemini")
    mapping_query, meta = map_request_to_query(
        q,
        parser_model=parser_model,
        return_meta=True,
        store_cache=False,
        validate_schema=False,
    )
    path = dump_mapper_payload(q)
    if (
        background_tasks
        and meta.get("query_generated")
        and meta.get("embedding")
        and meta.get("parser_template")
    ):
        background_tasks.add_task(
            store_query_vector_cache,
            q,
            parser_model,
            meta["parser_template"],
            meta["embedding"],
            mapping_query,
        )
    mapping_json = json.dumps(mapping_query, ensure_ascii=False, indent=2)
    cache_info = f"{meta.get('cache_source')}"
    if meta.get("vector_similarity") is not None:
        cache_info += f" (sim={meta.get('vector_similarity'):.3f})"
    return f"""
    <html>
      <head>
        <title>MCP Mapping Dump</title>
        <style>
          body {{ font-family: Arial, sans-serif; margin: 24px; }}
          pre {{ background: #f6f6f6; padding: 8px; }}
        </style>
      </head>
      <body>
        <h2>쿼리 매핑 파일 생성 완료</h2>
        <p>생성 위치:</p>
        <pre>{html.escape(str(path))}</pre>
        <h3>원문 질의</h3>
        <pre>{html.escape(q)}</pre>
        <h3>캐시</h3>
        <pre>{html.escape(cache_info)}</pre>
        <h3>매핑 결과</h3>
        <pre>{html.escape(mapping_json)}</pre>
        <p><a href="/">돌아가기</a></p>
      </body>
    </html>
    """


@app.post("/mapping.json")
def mapping_json(q: str = Form(...), background_tasks: BackgroundTasks = None) -> JSONResponse:
    parser_model = os.getenv("MCP_PARSER_MODEL", "gemini")
    mapping_query, meta = map_request_to_query(
        q,
        parser_model=parser_model,
        return_meta=True,
        store_cache=False,
        validate_schema=False,
    )
    path = dump_mapper_payload(q)
    if (
        background_tasks
        and meta.get("query_generated")
        and meta.get("embedding")
        and meta.get("parser_template")
    ):
        background_tasks.add_task(
            store_query_vector_cache,
            q,
            parser_model,
            meta["parser_template"],
            meta["embedding"],
            mapping_query,
        )
    meta_public = {
        key: value
        for key, value in meta.items()
        if key not in {"embedding", "parser_template"}
    }
    return JSONResponse(
        {
            "request": q,
            "query": mapping_query,
            "cache": meta_public,
            "dump_path": str(path),
        }
    )


@app.post("/query.json")
def query_json(q: str = Form(...)) -> JSONResponse:
    sql, params, qtype = _build_timeseries_query(q)
    cached = _cache_lookup(sql, params)
    if cached:
        cols = cached.get("cols", [])
        rows = cached.get("rows", [])
        top_cols = cached.get("top_cols", [])
        top_rows = cached.get("top_rows", [])
        geo_cols = cached.get("geo_cols", [])
        geo_rows = cached.get("geo_rows", [])
        narrative = cached.get("narrative")
    else:
        cols, rows = _execute(sql, params)
        top_sql, top_params = _build_topn_query(q)
        top_cols, top_rows = _execute(top_sql, top_params)
        geo_sql, geo_params = _build_geo_query(q)
        geo_cols, geo_rows = _execute(geo_sql, geo_params)
        narrative = llm_narrate(q, cols, rows, top_cols, top_rows)
        _cache_store(
            sql,
            params,
            {
                "cols": cols,
                "rows": rows,
                "top_cols": top_cols,
                "top_rows": top_rows,
                "geo_cols": geo_cols,
                "geo_rows": geo_rows,
                "narrative": narrative,
            },
        )

    geojson = []
    for row in geo_rows:
        row_map = dict(zip(geo_cols, row))
        if row_map.get("lat") is None or row_map.get("lon") is None:
            continue
        geojson.append(
            {
                "type": "Feature",
                "properties": {
                    "spatial_label": row_map.get("spatial_label"),
                    "foot_traffic": row_map.get("foot_traffic"),
                    "sales": row_map.get("sales"),
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [row_map.get("lon"), row_map.get("lat")],
                },
            }
        )

    return JSONResponse(
        {
            "sql": sql,
            "rows": [dict(zip(cols, row)) for row in rows],
            "top": [dict(zip(top_cols, row)) for row in top_rows],
            "narrative": narrative,
            "geojson": {"type": "FeatureCollection", "features": geojson},
        }
    )


@app.post("/insight", response_class=HTMLResponse)
def insight(q: str = Form(...)) -> str:
    context = _build_query_context(q)
    if _is_global_context(q, context):
        sql = (
            "SELECT date, SUM(foot_traffic) AS foot_traffic, "
            "SUM(sales) AS sales FROM gold_activity "
            "WHERE granularity = 'month' GROUP BY date ORDER BY date"
        )
        params: List[Any] = []
        cached = _cache_lookup(sql, params)
        if cached:
            cols = cached.get("cols", [])
            rows = cached.get("rows", [])
            payload = cached.get("insight_payload", {})
            narrative = cached.get("narrative", "요약할 데이터가 없습니다.")
        else:
            cols, rows = _execute(sql, params)
            payload = build_dataset_insight_payload(rows)
            narrative = (
                llm_narrate_insight("전체 데이터 인사이트 요약", payload)
                if rows
                else "요약할 데이터가 없습니다."
            )
            _cache_store(
                sql,
                params,
                {
                    "cols": cols,
                    "rows": rows,
                    "insight_payload": payload,
                    "narrative": narrative,
                },
            )
    else:
        sql, params = _build_insight_query(q)
        cached = _cache_lookup(sql, params)
        if cached:
            cols = cached.get("cols", [])
            rows = cached.get("rows", [])
            payload = cached.get("insight_payload", {})
            narrative = cached.get("narrative", "요약할 데이터가 없습니다.")
        else:
            cols, rows = _execute(sql, params)
            payload = build_insight_payload(cols, rows, context)
            narrative = (
                llm_narrate_insight(q, payload) if rows else "요약할 데이터가 없습니다."
            )
            _cache_store(
                sql,
                params,
                {
                    "cols": cols,
                    "rows": rows,
                    "insight_payload": payload,
                    "narrative": narrative,
                },
            )

    table = _render_table(cols, rows)
    payload_json = json.dumps(payload, ensure_ascii=False, indent=2)
    return f"""
    <html>
      <head>
        <title>MCP Insight Result</title>
        <style>
          body {{ font-family: Arial, sans-serif; margin: 24px; }}
          table {{ border-collapse: collapse; margin-top: 16px; width: 100%; }}
          th, td {{ border: 1px solid #ccc; padding: 6px 8px; text-align: left; }}
          pre {{ background: #f6f6f6; padding: 8px; }}
        </style>
      </head>
      <body>
        <h2>Query</h2>
        <pre>{html.escape(q)}</pre>
        <h3>SQL</h3>
        <pre>{html.escape(sql)}</pre>
        <h3>인사이트 요약</h3>
        <pre>{html.escape(narrative)}</pre>
        <h3>인사이트 후보</h3>
        <pre>{html.escape(payload_json)}</pre>
        <h3>Raw</h3>
        {table}
        <p><a href="/">돌아가기</a></p>
      </body>
    </html>
    """


@app.post("/insight_from_mapping", response_class=HTMLResponse)
def insight_from_mapping(mapping_json: str = Form(...)) -> str:
    try:
        mapping = json.loads(mapping_json)
    except json.JSONDecodeError as exc:
        return f"""
        <html>
          <head><title>MCP Insight Error</title></head>
          <body>
            <h2>JSON 파싱 실패</h2>
            <pre>{html.escape(str(exc))}</pre>
            <p><a href="/">돌아가기</a></p>
          </body>
        </html>
        """

    context = _build_query_context_from_mapping(mapping)
    is_global = not (
        context.get("spatial_label")
        or context.get("spatial_key")
        or context.get("date_start")
        or context.get("date_end")
    )

    if is_global:
        sql = (
            "SELECT date, SUM(foot_traffic) AS foot_traffic, "
            "SUM(sales) AS sales FROM gold_activity "
            "WHERE granularity = 'month' GROUP BY date ORDER BY date"
        )
        params: List[Any] = []
        cols, rows = _execute(sql, params)
        payload = build_dataset_insight_payload(rows)
        narrative = (
            llm_narrate_insight("전체 데이터 인사이트 요약", payload)
            if rows
            else "요약할 데이터가 없습니다."
        )
    else:
        sql, params = _build_insight_query_from_mapping(mapping)
        cols, rows = _execute(sql, params)
        payload = build_insight_payload(cols, rows, context)
        narrative = (
            llm_narrate_insight("쿼리 매핑 기반 인사이트", payload)
            if rows
            else "요약할 데이터가 없습니다."
        )

    table = _render_table(cols, rows)
    payload_json = json.dumps(payload, ensure_ascii=False, indent=2)
    return f"""
    <html>
      <head>
        <title>MCP Insight Result</title>
        <style>
          body {{ font-family: Arial, sans-serif; margin: 24px; }}
          table {{ border-collapse: collapse; margin-top: 16px; width: 100%; }}
          th, td {{ border: 1px solid #ccc; padding: 6px 8px; text-align: left; }}
          pre {{ background: #f6f6f6; padding: 8px; }}
        </style>
      </head>
      <body>
        <h2>쿼리 매핑 JSON</h2>
        <pre>{html.escape(json.dumps(mapping, ensure_ascii=False, indent=2))}</pre>
        <h3>SQL</h3>
        <pre>{html.escape(sql)}</pre>
        <h3>인사이트 요약</h3>
        <pre>{html.escape(narrative)}</pre>
        <h3>인사이트 후보</h3>
        <pre>{html.escape(payload_json)}</pre>
        <h3>Raw</h3>
        {table}
        <p><a href="/">돌아가기</a></p>
      </body>
    </html>
    """


@app.post("/insight.json")
def insight_json(q: str = Form(...)) -> JSONResponse:
    context = _build_query_context(q)
    if _is_global_context(q, context):
        sql = (
            "SELECT date, SUM(foot_traffic) AS foot_traffic, "
            "SUM(sales) AS sales FROM gold_activity "
            "WHERE granularity = 'month' GROUP BY date ORDER BY date"
        )
        params: List[Any] = []
        cached = _cache_lookup(sql, params)
        if cached:
            cols = cached.get("cols", [])
            rows = cached.get("rows", [])
            payload = cached.get("insight_payload", {})
            narrative = cached.get("narrative")
        else:
            cols, rows = _execute(sql, params)
            payload = build_dataset_insight_payload(rows)
            narrative = llm_narrate_insight("전체 데이터 인사이트 요약", payload)
            _cache_store(
                sql,
                params,
                {
                    "cols": cols,
                    "rows": rows,
                    "insight_payload": payload,
                    "narrative": narrative,
                },
            )
    else:
        sql, params = _build_insight_query(q)
        cached = _cache_lookup(sql, params)
        if cached:
            cols = cached.get("cols", [])
            rows = cached.get("rows", [])
            payload = cached.get("insight_payload", {})
            narrative = cached.get("narrative")
        else:
            cols, rows = _execute(sql, params)
            payload = build_insight_payload(cols, rows, context)
            narrative = llm_narrate_insight(q, payload)
            _cache_store(
                sql,
                params,
                {
                    "cols": cols,
                    "rows": rows,
                    "insight_payload": payload,
                    "narrative": narrative,
                },
            )

    return JSONResponse(
        {
            "sql": sql,
            "rows": [dict(zip(cols, row)) for row in rows],
            "insight_payload": payload,
            "narrative": narrative,
        }
    )


@app.get("/dataset", response_class=HTMLResponse)
def dataset_summary() -> str:
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MIN(date), MAX(date) FROM gold_activity")
            date_range = cur.fetchone()
            cur.execute("SELECT COUNT(*), COUNT(DISTINCT spatial_key), COUNT(DISTINCT date) FROM gold_activity")
            totals = cur.fetchone()
            cur.execute("SELECT date, COUNT(*) FROM gold_activity GROUP BY date ORDER BY date")
            month_counts = cur.fetchall()
            cur.execute(
                """
                SELECT date, source, COUNT(*) AS cnt
                FROM gold_activity
                GROUP BY date, source
                ORDER BY date, source
                """
            )
            month_sources = cur.fetchall()
            cur.execute(
                """
                SELECT spatial_label, COUNT(*) AS cnt
                FROM dim_spatial
                WHERE spatial_label IS NOT NULL
                GROUP BY spatial_label
                ORDER BY cnt DESC
                LIMIT 10
                """
            )
            top_spatial = cur.fetchall()
            cur.execute(
                """
                SELECT COALESCE(spatial_type, 'unknown') AS spatial_type, COUNT(*) AS cnt
                FROM dim_spatial
                GROUP BY COALESCE(spatial_type, 'unknown')
                ORDER BY cnt DESC
                """
            )
            spatial_types = cur.fetchall()
            cur.execute(
                """
                SELECT s.sig_name, COUNT(*) AS cnt
                FROM dim_spatial d
                JOIN admin_sig s
                  ON s.sig_code = LEFT(COALESCE(d.emd_code, d.code), 5)
                GROUP BY s.sig_name
                ORDER BY cnt DESC
                LIMIT 10
                """
            )
            top_sig = cur.fetchall()
            cur.execute(
                """
                SELECT s.sido_name, COUNT(*) AS cnt
                FROM dim_spatial d
                JOIN admin_sig s
                  ON s.sig_code = LEFT(COALESCE(d.emd_code, d.code), 5)
                GROUP BY s.sido_name
                ORDER BY cnt DESC
                LIMIT 10
                """
            )
            top_sido = cur.fetchall()
            cur.execute("SELECT COUNT(*) FROM admin_sig")
            sig_count = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM admin_sido")
            sido_count = cur.fetchone()
            cur.execute(
                """
                SELECT source, COUNT(*) AS cnt
                FROM gold_activity
                GROUP BY source
                ORDER BY cnt DESC
                """
            )
            sources = cur.fetchall()
            cur.execute(
                """
                SELECT
                    SUM(COALESCE(foot_traffic, 0)),
                    SUM(COALESCE(sales, 0))
                FROM gold_activity
                """
            )
            totals_sum = cur.fetchone()
            cur.execute(
                """
                SELECT date, SUM(foot_traffic) AS foot_traffic, SUM(sales) AS sales
                FROM gold_activity
                WHERE granularity = 'month'
                GROUP BY date
                ORDER BY date
                """
            )
            total_series = cur.fetchall()

    recent_series = total_series[-12:] if len(total_series) > 12 else total_series
    global_baseline = {
        "period": "recent_12_months",
        "domains": {
            "foot_traffic": build_global_baseline(recent_series, 1, "recent_12_months"),
            "sales": build_global_baseline(recent_series, 2, "recent_12_months"),
        },
    }

    summary = {
        "date_min": str(date_range[0]) if date_range and date_range[0] else None,
        "date_max": str(date_range[1]) if date_range and date_range[1] else None,
        "row_count": int(totals[0]) if totals else 0,
        "spatial_count": int(totals[1]) if totals else 0,
        "date_count": int(totals[2]) if totals else 0,
        "month_counts": [{"date": str(r[0]), "count": r[1]} for r in month_counts],
        "month_sources": [{"date": str(r[0]), "source": r[1], "count": r[2]} for r in month_sources],
        "top_spatial": [{"label": r[0], "count": r[1]} for r in top_spatial],
        "spatial_type_counts": [{"type": r[0], "count": r[1]} for r in spatial_types],
        "top_sig": [{"name": r[0], "count": r[1]} for r in top_sig],
        "top_sido": [{"name": r[0], "count": r[1]} for r in top_sido],
        "sig_count": int(sig_count[0]) if sig_count else 0,
        "sido_count": int(sido_count[0]) if sido_count else 0,
        "sources": [{"source": r[0], "count": r[1]} for r in sources],
        "sum_foot_traffic": float(totals_sum[0]) if totals_sum and totals_sum[0] is not None else 0.0,
        "sum_sales": float(totals_sum[1]) if totals_sum and totals_sum[1] is not None else 0.0,
        "global_baseline": global_baseline,
    }
    narrative = llm_narrate_dataset(summary)
    insight_payload = build_dataset_insight_payload(total_series)
    insight_narrative = llm_narrate_insight("전체 데이터 인사이트 요약", insight_payload)

    return f"""
    <html>
      <head>
        <title>MCP Dataset Summary</title>
        <style>
          body {{ font-family: Arial, sans-serif; margin: 24px; }}
          pre {{ background: #f6f6f6; padding: 8px; }}
        </style>
      </head>
      <body>
        <h2>Dataset Summary</h2>
        <h3>요약</h3>
        <pre>{html.escape(narrative)}</pre>
        <h3>인사이트 요약</h3>
        <pre>{html.escape(insight_narrative)}</pre>
        <h3>인사이트 후보</h3>
        <pre>{html.escape(json.dumps(insight_payload, ensure_ascii=False, indent=2))}</pre>
        <h3>Global Baseline</h3>
        <pre>{html.escape(json.dumps(global_baseline, ensure_ascii=False, indent=2))}</pre>
        <h3>Raw</h3>
        <pre>{html.escape(json.dumps(summary, ensure_ascii=False, indent=2))}</pre>
      </body>
    </html>
    """


@app.get("/dataset.json")
def dataset_summary_json() -> JSONResponse:
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MIN(date), MAX(date) FROM gold_activity")
            date_range = cur.fetchone()
            cur.execute("SELECT COUNT(*), COUNT(DISTINCT spatial_key), COUNT(DISTINCT date) FROM gold_activity")
            totals = cur.fetchone()
            cur.execute("SELECT date, COUNT(*) FROM gold_activity GROUP BY date ORDER BY date")
            month_counts = cur.fetchall()
            cur.execute(
                """
                SELECT date, source, COUNT(*) AS cnt
                FROM gold_activity
                GROUP BY date, source
                ORDER BY date, source
                """
            )
            month_sources = cur.fetchall()
            cur.execute(
                """
                SELECT spatial_label, COUNT(*) AS cnt
                FROM dim_spatial
                WHERE spatial_label IS NOT NULL
                GROUP BY spatial_label
                ORDER BY cnt DESC
                LIMIT 10
                """
            )
            top_spatial = cur.fetchall()
            cur.execute(
                """
                SELECT COALESCE(spatial_type, 'unknown') AS spatial_type, COUNT(*) AS cnt
                FROM dim_spatial
                GROUP BY COALESCE(spatial_type, 'unknown')
                ORDER BY cnt DESC
                """
            )
            spatial_types = cur.fetchall()
            cur.execute(
                """
                SELECT s.sig_name, COUNT(*) AS cnt
                FROM dim_spatial d
                JOIN admin_sig s
                  ON s.sig_code = LEFT(COALESCE(d.emd_code, d.code), 5)
                GROUP BY s.sig_name
                ORDER BY cnt DESC
                LIMIT 10
                """
            )
            top_sig = cur.fetchall()
            cur.execute(
                """
                SELECT s.sido_name, COUNT(*) AS cnt
                FROM dim_spatial d
                JOIN admin_sig s
                  ON s.sig_code = LEFT(COALESCE(d.emd_code, d.code), 5)
                GROUP BY s.sido_name
                ORDER BY cnt DESC
                LIMIT 10
                """
            )
            top_sido = cur.fetchall()
            cur.execute("SELECT COUNT(*) FROM admin_sig")
            sig_count = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM admin_sido")
            sido_count = cur.fetchone()
            cur.execute(
                """
                SELECT source, COUNT(*) AS cnt
                FROM gold_activity
                GROUP BY source
                ORDER BY cnt DESC
                """
            )
            sources = cur.fetchall()
            cur.execute(
                """
                SELECT
                    SUM(COALESCE(foot_traffic, 0)),
                    SUM(COALESCE(sales, 0))
                FROM gold_activity
                """
            )
            totals_sum = cur.fetchone()
            cur.execute(
                """
                SELECT date, SUM(foot_traffic) AS foot_traffic, SUM(sales) AS sales
                FROM gold_activity
                WHERE granularity = 'month'
                GROUP BY date
                ORDER BY date
                """
            )
            total_series = cur.fetchall()

    recent_series = total_series[-12:] if len(total_series) > 12 else total_series
    global_baseline = {
        "period": "recent_12_months",
        "domains": {
            "foot_traffic": build_global_baseline(recent_series, 1, "recent_12_months"),
            "sales": build_global_baseline(recent_series, 2, "recent_12_months"),
        },
    }
    insight_payload = build_dataset_insight_payload(total_series)
    summary = {
        "date_min": str(date_range[0]) if date_range and date_range[0] else None,
        "date_max": str(date_range[1]) if date_range and date_range[1] else None,
        "row_count": int(totals[0]) if totals else 0,
        "spatial_count": int(totals[1]) if totals else 0,
        "date_count": int(totals[2]) if totals else 0,
        "month_counts": [{"date": str(r[0]), "count": r[1]} for r in month_counts],
        "month_sources": [{"date": str(r[0]), "source": r[1], "count": r[2]} for r in month_sources],
        "top_spatial": [{"label": r[0], "count": r[1]} for r in top_spatial],
        "spatial_type_counts": [{"type": r[0], "count": r[1]} for r in spatial_types],
        "top_sig": [{"name": r[0], "count": r[1]} for r in top_sig],
        "top_sido": [{"name": r[0], "count": r[1]} for r in top_sido],
        "sig_count": int(sig_count[0]) if sig_count else 0,
        "sido_count": int(sido_count[0]) if sido_count else 0,
        "sources": [{"source": r[0], "count": r[1]} for r in sources],
        "sum_foot_traffic": float(totals_sum[0]) if totals_sum and totals_sum[0] is not None else 0.0,
        "sum_sales": float(totals_sum[1]) if totals_sum and totals_sum[1] is not None else 0.0,
        "global_baseline": global_baseline,
        "insight_payload": insight_payload,
        "insight_narrative": llm_narrate_insight(
            "전체 데이터 인사이트 요약",
            insight_payload,
        ),
        "narrative": llm_narrate_dataset(
            {
                "date_min": str(date_range[0]) if date_range and date_range[0] else None,
                "date_max": str(date_range[1]) if date_range and date_range[1] else None,
                "row_count": int(totals[0]) if totals else 0,
                "spatial_count": int(totals[1]) if totals else 0,
                "date_count": int(totals[2]) if totals else 0,
                "month_counts": [{"date": str(r[0]), "count": r[1]} for r in month_counts],
                "month_sources": [{"date": str(r[0]), "source": r[1], "count": r[2]} for r in month_sources],
                "top_spatial": [{"label": r[0], "count": r[1]} for r in top_spatial],
                "spatial_type_counts": [{"type": r[0], "count": r[1]} for r in spatial_types],
                "top_sig": [{"name": r[0], "count": r[1]} for r in top_sig],
                "top_sido": [{"name": r[0], "count": r[1]} for r in top_sido],
                "sig_count": int(sig_count[0]) if sig_count else 0,
                "sido_count": int(sido_count[0]) if sido_count else 0,
                "sources": [{"source": r[0], "count": r[1]} for r in sources],
                "sum_foot_traffic": float(totals_sum[0]) if totals_sum and totals_sum[0] is not None else 0.0,
                "sum_sales": float(totals_sum[1]) if totals_sum and totals_sum[1] is not None else 0.0,
            }
        ),
    }
    return JSONResponse(summary)


@app.get("/health.json")
def health() -> JSONResponse:
    return JSONResponse(
        {
            "status": "ok",
            "llm_provider": os.getenv("MCP_LLM_PROVIDER"),
            "llm_enabled": bool(os.getenv("MCP_GEMINI_API_KEY")),
            "gemini_key_set": bool(os.getenv("MCP_GEMINI_API_KEY")),
            "vworld_key_set": bool(os.getenv("MCP_VWORLD_KEY")),
            "vworld_domain_set": bool(os.getenv("MCP_VWORLD_DOMAIN")),
        }
    )
