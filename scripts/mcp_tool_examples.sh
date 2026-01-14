#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
LEVEL="${LEVEL:-sig}"

echo "compare_domains"
curl -sS -X POST "$BASE_URL/api/tools/compare_domains" \
  -d "region=강남구" \
  -d "period_from=2024-01" \
  -d "period_to=2024-12" \
  -d "domains=population,sales" \
  -d "level=$LEVEL"
echo

echo "get_rankings"
curl -sS -X POST "$BASE_URL/api/tools/get_rankings" \
  -d "metric=activity_volume" \
  -d "period=2024-12" \
  -d "top_k=10" \
  -d "level=$LEVEL"
echo

echo "detect_anomaly"
curl -sS -X POST "$BASE_URL/api/tools/detect_anomaly" \
  -d "region=강남구" \
  -d "domain=sales" \
  -d "period=2024-11" \
  -d "z_threshold=2.0" \
  -d "level=$LEVEL"
echo

echo "get_advanced_insight"
curl -sS -X POST "$BASE_URL/api/tools/get_advanced_insight" \
  -d "region=강남구" \
  -d "period=2024" \
  -d "domains=population,sales" \
  -d "level=$LEVEL"
echo
