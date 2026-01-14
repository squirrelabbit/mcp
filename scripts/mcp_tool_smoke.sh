#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-mcp-postgres}"
LEVEL="${LEVEL:-sig}"

if [[ "${REFRESH_ADVANCED:-0}" == "1" ]]; then
  MCP_MODE=refresh_advanced_insights python main.py
fi

LATEST_DATE="$(docker exec -i "$POSTGRES_CONTAINER" psql -U mcp -d mcp -t -A \
  -c "SELECT date FROM v_insight_candidate_all WHERE sales IS NOT NULL AND level = '${LEVEL}' ORDER BY date DESC LIMIT 1;" | tr -d '[:space:]')"
REGION="$(docker exec -i "$POSTGRES_CONTAINER" psql -U mcp -d mcp -t -A \
  -c "SELECT spatial_label FROM v_insight_candidate_all WHERE sales IS NOT NULL AND level = '${LEVEL}' ORDER BY date DESC LIMIT 1;" | tr -d '[:space:]')"

if [[ -z "$LATEST_DATE" || -z "$REGION" ]]; then
  echo "No data found in v_insight_candidate_all (sales not null)."
  exit 1
fi

echo "Using region=$REGION period=$LATEST_DATE"

echo "compare_domains"
curl -sS -X POST "$BASE_URL/api/tools/compare_domains" \
  --data-urlencode "region=$REGION" \
  -d "period_from=$LATEST_DATE" \
  -d "period_to=$LATEST_DATE" \
  -d "domains=population,sales" \
  -d "level=$LEVEL"
echo

echo "get_rankings"
curl -sS -X POST "$BASE_URL/api/tools/get_rankings" \
  -d "metric=sales" \
  -d "period=$LATEST_DATE" \
  -d "top_k=5" \
  -d "level=$LEVEL"
echo

echo "detect_anomaly"
curl -sS -X POST "$BASE_URL/api/tools/detect_anomaly" \
  --data-urlencode "region=$REGION" \
  -d "domain=sales" \
  -d "period=$LATEST_DATE" \
  -d "z_threshold=2.0" \
  -d "level=$LEVEL"
echo

echo "get_advanced_insight"
curl -sS -X POST "$BASE_URL/api/tools/get_advanced_insight" \
  --data-urlencode "region=$REGION" \
  -d "period=$LATEST_DATE" \
  -d "domains=population,sales" \
  -d "level=$LEVEL"
echo
