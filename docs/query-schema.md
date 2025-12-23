# MCP Query Schema

## Overview

LLM Parser 는 사용자의 자연어 요청을 아래 Schema 를 따르는 JSON 으로 변환해야 한다.  
MCP Pipeline 은 이 JSON 을 받아 필터/집계/분석을 수행한다.

```json
{
  "target": "population.foot_traffic",
  "aggregations": ["avg", "sum"],
  "filters": {
    "time": {
      "start": "2024-08-01",
      "end": "2024-08-31",
      "weekdays": ["SAT", "SUN"]
    },
    "spatial": ["강남구", "홍대입구"],
    "demographics": {"age": ["20s"], "gender": ["female"]}
  },
  "group_by": ["events.weather", "spatial"],
  "compare": {"type": "yoy", "interval": "1y"}
}
```

## Field Details

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `target` | string | Yes | 분석 대상. `population.foot_traffic`, `economic.sales` 등 MCP 의미축 경로. |
| `aggregations` | array[string] | Optional | `sum`, `avg`, `min`, `max` 등 집계 방식. 지정 없으면 기본값(`avg`). |
| `filters.time.start` / `end` | string (ISO date) | Optional | 분석 기간. 생략 시 전체 기간. |
| `filters.time.month` | int (1-12) | Optional | 특정 월 필터. |
| `filters.time.weekdays` | array[string] | Optional | 요일 필터 (예: `["SAT", "SUN"]`). |
| `filters.spatial` | array[string] | Optional | 분석 대상 지역 리스트. |
| `filters.demographics.age` | array[string] | Optional | `["20s", "30s"]` 등. |
| `filters.demographics.gender` | array[string] | Optional | `["male", "female"]`. |
| `group_by` | array[string] | Optional | 결과를 구분할 축 (`["events.weather", "spatial"]` 등). |
| `compare` | object | Optional | 비교 설정. `{"type": "yoy", "interval": "1y"}` 등. |

## Example Prompts → JSON

- “8월 주말 날씨별 방문객 알려줘”  
  ```json
  {
    "target": "population.foot_traffic",
    "filters": {
      "time": {"month": 8, "weekdays": ["SAT", "SUN"]}
    },
    "group_by": ["events.weather"]
  }
  ```

- “작년과 올해 축제 기간 매출 비교”  
  ```json
  {
    "target": "economic.sales",
    "group_by": ["events.festival"],
    "compare": {"type": "yoy", "interval": "1y"}
  }
  ```
