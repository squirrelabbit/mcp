# MCP Tool API (v1)

MCP Tool API는 DB(View/MV)를 기반으로 구조화된 JSON만 반환한다.
Narrative는 MCP Server 외부에서 LLM이 생성한다.

## 제안서 요약

- Tool은 의미 단위로 정의된 MCP 표준 인터페이스
- DB(View/MV) 계산 결과만 반환, LLM 추론 결과는 포함하지 않음
- 실시간(View) + 배치(MV) 하이브리드로 성능/신뢰성 균형
- 모든 응답은 구조화 JSON + metadata 포함

## 공통 규칙

- 입력 형식: `application/x-www-form-urlencoded`
- 기간 포맷: `YYYY`, `YYYY-MM`, `YYYY-MM-DD`
- 에러 포맷:
  - 400: `{"error":{"code":"invalid_argument","message":"..."}}`
  - 500: `{"error":{"code":"internal_error","message":"..."}}`

## Analysis Endpoints (매핑 + Tool)

사용자 질의 → 매핑(JSON) → Tool 호출까지 한 번에 수행하는 API.

- `POST /api/analysis/compare`
- `POST /api/analysis/rankings`
- `POST /api/analysis/anomaly`
- `POST /api/analysis/advanced`
- `POST /api/analysis/report` (summary 포함)

응답에는 `mapping`, `mapping_cache`, `result(s)`가 함께 제공된다.

### 공통 Metadata

```
{
  "metadata": {
    "source": ["view_or_mv_name"],
    "generated_at": "2025-01-01T00:00:00+00:00",
    "period_from": "2024-01-01",
    "period_to": "2024-12-31"
  }
}
```

## 1) compare_domains

- Endpoint: `POST /api/tools/compare_domains`
- Data source: `v_insight_candidate_all`

### Input

```
region=강남구
period_from=2024-01
period_to=2024-12
domains=population,sales
```

### Output

```
{
  "comparisons": [
    {
      "domain": "population",
      "metric": "foot_traffic",
      "trend": "up|down|flat",
      "change_rate": 0.12,
      "signal": "strong_change|moderate_change|minor_change|insufficient_data"
    }
  ],
  "metadata": {...}
}
```

### Allowed values

- `domains`: `population`, `sales`
- `level`: `norm`, `sig`, `sido` (옵션, 기본 `sig`)

## 2) get_rankings

- Endpoint: `POST /api/tools/get_rankings`
- Data source: `v_insight_candidate_all`

### Input

```
metric=activity_volume
period=2024-12
top_k=10
```

### Output

```
{
  "rankings": [
    {
      "spatial_label": "강남구",
      "metric": "foot_traffic",
      "value": 123456
    }
  ],
  "metadata": {...}
}
```

### Allowed values

- `metric`: `activity_volume`, `foot_traffic`, `sales`
- `top_k`: 1~100
- `level`: `norm`, `sig`, `sido` (옵션, 기본 `sig`)

## 3) detect_anomaly

- Endpoint: `POST /api/tools/detect_anomaly`
- Data source: `v_insight_candidate_all`

### Input

```
region=강남구
domain=sales
period=2024-11
z_threshold=2.0
```

### Output

```
{
  "anomaly": {
    "region": "강남구",
    "metric": "sales",
    "period": "2024-11-01",
    "value": 987654,
    "zscore": -2.4,
    "threshold": 2.0,
    "is_anomaly": true
  },
  "metadata": {...}
}
```

### Allowed values

- `domain`: `population`, `sales`
- `z_threshold`: > 0
- `level`: `norm`, `sig`, `sido` (옵션, 기본 `sig`)

## 4) get_advanced_insight

- Endpoint: `POST /api/tools/get_advanced_insight`
- Data source: `mv_insight_advanced`

### Input

```
region=강남구
period=2024
domains=population,sales
level=sig
```

### Output

```
{
  "advanced_insight": {
    "level": "sig",
    "region": "강남구",
    "period": "2024-12-31",
    "domains": ["population","sales"],
    "correlation": {
      "sales_vs_foot_traffic": 0.62
    },
    "impact": {
      "sales_impact_slope": 1.23,
      "sales_impact_score": 0.88,
      "foot_traffic_impact_score": 0.51
    }
  },
  "metadata": {...}
}
```

### Allowed values

- `domains`: `population`, `sales`
- `level`: `norm`, `sig`, `sido`
