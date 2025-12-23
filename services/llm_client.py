import hashlib
import json
import os
from pathlib import Path
from typing import Callable, Optional


class LLMClient:
    """
    LLM 호출 래퍼.

    - provider: 실제 LLM 호출을 수행하는 callable(prompt, model) -> str
    - cache_dir: 프롬프트/모델 기반 응답 캐시 저장 위치
    """

    def __init__(
        self,
        provider: Optional[Callable[[str, str], str]] = None,
        cache_dir: Path = Path(".llm_cache"),
    ):
        self.provider = provider or self._default_provider
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(exist_ok=True)

    def call(self, prompt: str, model: str = "mock-llm", use_cache: bool = True) -> str:
        key = self._cache_key(prompt, model)
        cache_path = self.cache_dir / f"{key}.json"

        if use_cache and cache_path.exists():
            return cache_path.read_text(encoding="utf-8")

        response = self.provider(prompt, model)
        if use_cache:
            cache_path.write_text(response, encoding="utf-8")
        return response

    def _cache_key(self, prompt: str, model: str) -> str:
        h = hashlib.sha256()
        h.update(model.encode("utf-8"))
        h.update(prompt.encode("utf-8"))
        return h.hexdigest()

    def _default_provider(self, prompt: str, model: str) -> str:
        """
        실제 LLM 연결 전까지 사용할 Mock Provider.
        환경변수 MOCK_LLM_PREFIX 가 있으면 prefix 를 붙여서 반환한다.
        """
        prefix = os.environ.get("MOCK_LLM_PREFIX", "[MOCK]")
        payload = {
            "model": model,
            "response": f"{prefix} {prompt[:200]}...",
        }
        return json.dumps(payload, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Test LLMClient with caching.")
    parser.add_argument("prompt", help="LLM prompt text")
    parser.add_argument("--model", default="mock-llm")
    parser.add_argument("--no-cache", action="store_true")
    args = parser.parse_args()

    client = LLMClient()
    print(client.call(args.prompt, model=args.model, use_cache=not args.no_cache))
