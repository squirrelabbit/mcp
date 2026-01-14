import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from jsonschema import ValidationError, validate

from services.summarizer import DEFAULT_RESULT_PATH, InsightSummarizer


SCHEMA_PATH = Path("config/query.schema.json")
RESULT_PATH = DEFAULT_RESULT_PATH
_SUMMARIZER: InsightSummarizer | None = None


def load_schema() -> Dict[str, Any]:
    with SCHEMA_PATH.open() as f:
        return json.load(f)


def validate_query(query: Dict[str, Any], schema: Dict[str, Any]) -> None:
    validate(instance=query, schema=schema)


def run_query(query: Dict[str, Any]) -> Dict[str, Any]:
    summarizer = _get_summarizer()
    return summarizer.summarize(query).to_dict()


def _get_summarizer() -> InsightSummarizer:
    global _SUMMARIZER
    if _SUMMARIZER is None:
        _SUMMARIZER = InsightSummarizer(result_path=RESULT_PATH)
    return _SUMMARIZER


def load_query(path: Path) -> Dict[str, Any]:
    if not path.exists():
        create_default_query(path)
    with path.open() as f:
        return json.load(f)


def create_default_query(path: Path) -> None:
    default_query = {
        "target": "population.foot_traffic",
        "filters": {"time": {"month": datetime.now().month}, "spatial": []},
        "aggregations": ["avg"],
    }
    path.write_text(json.dumps(default_query, ensure_ascii=False, indent=2))
    print(f"기본 예시 쿼리를 생성했습니다: {path}")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Validate MCP query JSON and run pipeline."
    )
    parser.add_argument(
        "query_file", type=Path, help="Path to query JSON file (생성됩니다)"
    )
    args = parser.parse_args()

    schema = load_schema()
    query = load_query(args.query_file)

    try:
        validate_query(query, schema)
    except ValidationError as e:
        print("Invalid query:", e.message)
        return

    result = run_query(query)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
