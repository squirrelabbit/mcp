MCP (Multi-domain Comparison Platform) — Project Purpose & Vision
1. 🎯 프로젝트 목적

MCP는 서로 다른 도메인의 데이터를 하나의 공통 구조로 통합하고,
시간·지역 단위로 비교·조합·분석하여 인사이트를 자동 생성하는 플랫폼이다.

이 플랫폼의 목표는 다음과 같다:

이종 데이터가 들어와도 동일한 분석 파이프라인이 작동할 것

새로운 도메인이 추가되어도 Core를 수정하지 않고 확장될 것

Metrics와 Insight는 도메인 독립적인 형태로 자동 계산될 것

LLM이 이해하기 쉬운 구조로 결과를 제공할 것

하나의 지역·시간 단위에서 cross-domain 시너지 분석이 가능할 것

즉, MCP는 "데이터를 정리해주는 도구"가 아니라
**"모든 데이터를 연결해서 의미 있는 인사이트를 자동 생성하는 엔진"**이다.

2. 🧩 해결하고자 하는 문제

현재 세상 대부분의 데이터 분석은:

도메인마다 스키마가 다르고

시간 단위가 다르고

분석 지표가 다르고

인사이트 로직도 제각각이다

그래서 새로운 데이터가 추가될 때마다:

ETL 필요

스키마 설계 필요

통합 로직 재작성

지표 산출 방식 변경

인사이트 규칙 재정의

이라는 반복 지옥이 발생한다.

MCP의 목표는 이것을 깨는 것이다:

🔑 “도메인이 달라도 동일한 분석을 수행할 수 있는 추상화된 분석 엔진”

3. 🏗 MCP가 제공하는 핵심 전략
✔ 1) Standard Schema (공통축 Normalize)

모든 데이터는 population / economic / behavior / events 4개 축 중 하나로 변환됨.

✔ 2) Domain Module + Metrics Adapter

새 도메인이 들어오면 해야 할 일은 오직 2개:

DomainModule: raw → MCP 구조 변환

MetricsAdapter: 이 도메인에서 어떤 지표로 분석할지 정의

Core는 절대 수정되지 않는다.

✔ 3) Metrics Engine (window-free)

Baseline window, main metric 등은 도메인 config에서 오고
Metrics Core는 domain-agnostic한 상태를 유지한다.

✔ 4) Insight Engine (narrative + analytical)

InsightEngine은 다음을 자동 생성한다:

trend

correlation

demographic shift

impact

narrative

LLM이 그대로 받아서 설명문을 만들 수 있을 정도로 구조화된 JSON 형태 제공.

✔ 5) Cross-domain Join

공간(spatial) × 시간(time) 축 기준으로
다양한 도메인 레코드를 병합하여
cross-domain synergy 분석이 가능하다.

예: 축제 → 유동 증가 → 매출 증가 → 외부 인구 유입 구조 분석

4. ⚙ MCP의 철학 (Codex가 반드시 이해해야 함)

Codex가 가장 지켜야 할 8가지 원칙:

Core 엔진은 절대 도메인 로직을 갖지 않는다.

DomainModule과 MetricsAdapter는 1:1로 매핑된다.

Normalization Layer는 도메인 독립적이어야 한다.

Metrics Engine은 domain-aware 로직을 가질 수 없다.

Insight Engine은 metrics 결과만을 소비해야 한다.

Cross-domain 분석은 CrossDomainFusionEngine 기반으로만 수행한다.

LLM-friendly JSON을 최종 산출물로 제공한다.

확장 시 Core는 절대 건드리지 않는다.

이 원칙 덕분에 MCP는 확장형 엔진으로 유지된다.

5. 📦 최종 출력물의 목적

InsightEngine이 생성한 결과는 결국 LLM이 읽고 보고서를 자동 생성할 수 있도록 하는 것이다.

그래서 출력물에는 다음이 반드시 포함되어야 한다:

structural data (정량)

summary (요약)

insight (trend, correlation, impact)

narrative (자연어)

LLM은 이 데이터들을 결합하여:

요약 리포트

지역 분석서

정책 제안

경향 보고서

등을 자동 생성할 수 있게 된다.

6. 🚀 MCP의 최종 목표

MCP는 다음을 가능하게 하는 범용 인사이트 자동화 플랫폼이다:

축제 vs 상권 vs 관광 vs 기상 vs SNS

통신 유동 데이터 ↔ 소비 매출 데이터

인구 변화 ↔ 이벤트 영향

도메인 10개가 붙어도 Core 수정 없음

분석 자동화 + 인사이트 자동 생성

MCP는 단순 데이터 파이프라인이 아니라,

**“이종 데이터를 결합해 의미를 만들어내는 AI-ready 추론 엔진”**이다.
