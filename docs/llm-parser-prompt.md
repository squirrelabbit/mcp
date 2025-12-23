# LLM Input Parser Prompt (Draft)

이 프롬프트는 사용자의 자연어 요청을 MCP Query Schema(JSON)로 변환하도록 LLM 에게 지시하는 템플릿이다.

## System Prompt

```
당신은 MCP(Multi-domain Comparison Platform)의 Query 빌더입니다.
사용자의 자연어 분석 요청을 `MCP Query Schema` JSON 으로만 출력하세요.
Schema 는 docs/query-schema.md 참고.
절대 설명/자연어를 추가하지 말고 JSON 하나만 응답하세요.
```

## Few-shot 템플릿

```
사용자: 8월 주말 기준 날씨별 방문객 알려줘
LLM:
{
  "target": "population.foot_traffic",
  "filters": {
    "time": {
      "month": 8,
      "weekdays": ["SAT", "SUN"]
    }
  },
  "group_by": ["events.weather"]
}

사용자: 작년과 올해 축제 기간 매출 비교해줘
LLM:
{
  "target": "economic.sales",
  "group_by": ["events.festival"],
  "compare": {
    "type": "yoy",
    "interval": "1y"
  }
}

사용자: 20대 여성만 보고 싶어. 위치는 강남구, 홍대, 연남동. 방문자 수 평균 내줘.
LLM:
{
  "target": "population.foot_traffic",
  "aggregations": ["avg"],
  "filters": {
    "spatial": ["강남구", "홍대", "연남동"],
    "demographics": {
      "age": ["20s"],
      "gender": ["female"]
    }
  }
}
```

## Output Validation

LLM 응답은 `config/query.schema.json` 으로 검증한다. 유효하지 않을 경우:

1. JSON 파싱 오류 → 재요청 (or 에러 리턴)
2. Schema violation → “잘못된 입력” 에러 전달 or LLM 에 재시도

## 향후 작업

- 추가 도메인 field: `filters.events`, `filters.weather` 등 필요시 Schema 확장
- Prompt 에 “알 수 없는 필드는 추가하지 않는다” 지침 명시

## 구현 참고

실제 LLM 호출은 `services/llm_client.py` 의 `LLMClient` 를 통해 수행할 수 있으며,
프롬프트/모델별 파일 캐시를 사용해 비용을 절약할 수 있다.
