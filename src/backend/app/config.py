from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class Settings:
    api_base_url: str = os.getenv("ADP_API_BASE_URL", "").strip()
    api_key: str = os.getenv("ADP_API_KEY", "").strip()
    generation_model: str = os.getenv("ADP_GENERATION_MODEL", "qwen/qwen-2.5-coder-32b-instruct").strip()
    explanation_model: str = os.getenv("ADP_EXPLANATION_MODEL", "qwen/qwen-2.5-coder-32b-instruct").strip()
    max_completion_tokens: int = int(os.getenv("ADP_MAX_COMPLETION_TOKENS", "1200"))
    timeout_seconds: int = int(os.getenv("ADP_TIMEOUT_SECONDS", "45"))
    generation_max_attempts: int = int(os.getenv("ADP_GENERATION_MAX_ATTEMPTS", "3"))
    generation_retry_backoff_seconds: float = float(os.getenv("ADP_GENERATION_RETRY_BACKOFF_SECONDS", "1.0"))
    session_ttl_seconds: int = int(os.getenv("ADP_SESSION_TTL_SECONDS", str(2 * 60 * 60)))
    session_root: Path = Path(
        os.getenv("ADP_SESSION_ROOT", Path(tempfile.gettempdir()) / "adp_demo_sessions")
    )

    @property
    def llm_enabled(self) -> bool:
        return bool(self.api_base_url and self.api_key and self.generation_model)


settings = Settings()
