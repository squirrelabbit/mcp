import os
from typing import List, Optional


class EmbeddingClient:
    """
    Local embedding client using sentence-transformers.

    - model_name: SentenceTransformer model id
    """

    def __init__(self, model_name: Optional[str] = None):
        self.model_name = model_name or os.getenv("MCP_EMBEDDING_MODEL", "BAAI/bge-m3")
        self._model = None

    def _load_model(self):
        if self._model is not None:
            return
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:
            raise RuntimeError(
                "sentence-transformers is required. Run `pip install sentence-transformers`."
            ) from exc
        self._model = SentenceTransformer(self.model_name)

    def embed_text(self, text: str) -> List[float]:
        self._load_model()
        normalized = text.strip()
        if not normalized:
            return []
        vectors = self._model.encode([normalized], normalize_embeddings=True)
        return vectors[0].tolist()
