from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Callable

from llm.providers.ollama_autogen import AutogenOllamaClient


@dataclass
class OrchestratorResult:
    content: str
    raw: Dict[str, Any]


class BaseOrchestrator:
    def generate_json(self, system: str, user: str) -> OrchestratorResult:
        raise NotImplementedError


class AutogenOrchestrator(BaseOrchestrator):
    """
    Uses pyautogen with Ollama.
    Falls back to internal orchestration if autogen isn't available.
    """

    def __init__(self, ollama_base_url: str, model: str):
        self.client = AutogenOllamaClient(ollama_base_url=ollama_base_url, model=model)

    def generate_json(self, system: str, user: str) -> OrchestratorResult:
        return self.client.generate_json(system=system, user=user)


class FallbackOrchestrator(BaseOrchestrator):
    """
    Deterministic fallback for environments without Autogen.
    Not as smart as LLM, but keeps pipeline alive without crashing.
    """

    def __init__(self, deterministic_fn: Optional[Callable[[str, str], Dict[str, Any]]] = None):
        self.fn = deterministic_fn

    def generate_json(self, system: str, user: str) -> OrchestratorResult:
        if self.fn is None:
            # Minimal safe response
            raw = {"note": "LLM unavailable; using fallback.", "system": system[:200], "user": user[:200]}
            return OrchestratorResult(content=str(raw), raw=raw)
        raw = self.fn(system, user)
        return OrchestratorResult(content=str(raw), raw=raw)


def build_orchestrator(ollama_base_url: str, model: str) -> BaseOrchestrator:
    # User required Autogen. We attempt it; if import fails, we still provide fallback (never crash).
    try:
        import autogen  # noqa: F401
        return AutogenOrchestrator(ollama_base_url=ollama_base_url, model=model)
    except Exception:
        return FallbackOrchestrator()
