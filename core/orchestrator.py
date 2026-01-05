from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Callable


@dataclass
class OrchestratorResult:
    content: str
    raw: Dict[str, Any]


class BaseOrchestrator:
    def generate_json(self, system: str, user: str) -> OrchestratorResult:
        raise NotImplementedError


class FallbackOrchestrator(BaseOrchestrator):
    """
    Deterministic fallback for environments without Autogen / Ollama.
    Keeps pipeline alive without crashing.
    """

    def __init__(self, deterministic_fn: Optional[Callable[[str, str], Dict[str, Any]]] = None):
        self.fn = deterministic_fn

    def generate_json(self, system: str, user: str) -> OrchestratorResult:
        if self.fn is None:
            raw = {
                "note": "LLM unavailable; using fallback.",
                "system_preview": (system or "")[:200],
                "user_preview": (user or "")[:200],
            }
            return OrchestratorResult(content=str(raw), raw=raw)

        raw = self.fn(system, user)
        return OrchestratorResult(content=str(raw), raw=raw)


class AutogenOrchestrator(BaseOrchestrator):
    """
    Uses Autogen + Ollama.
    Import of AutogenOllamaClient is LAZY to avoid circular imports.
    """

    def __init__(self, ollama_base_url: str, model: str):
        # ✅ Lazy import avoids circular imports + avoids import error at module import time
        from llm.providers.ollama_autogen import AutogenOllamaClient

        self.client = AutogenOllamaClient(ollama_base_url=ollama_base_url, model=model)

    def generate_json(self, system: str, user: str) -> OrchestratorResult:
        return self.client.generate_json(system=system, user=user)


def build_orchestrator(ollama_base_url: str, model: str) -> BaseOrchestrator:
    """
    Try Autogen; if unavailable, fallback.
    Never crash app import-time.
    """
    try:
        # Some environments have 'autogen' import, others don't.
        # We only need to know if it's importable.
        import autogen  # noqa: F401

        return AutogenOrchestrator(ollama_base_url=ollama_base_url, model=model)
    except Exception:
        return FallbackOrchestrator()