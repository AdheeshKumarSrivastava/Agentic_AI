from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Callable
import importlib.util


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

    def __init__(
        self,
        deterministic_fn: Optional[Callable[[str, str], Dict[str, Any]]] = None,
        reason: str = "LLM unavailable",
    ):
        self.fn = deterministic_fn
        self.reason = reason

    def generate_json(self, system: str, user: str) -> OrchestratorResult:
        if self.fn is None:
            raw = {
                "note": "FallbackOrchestrator",
                "reason": self.reason,
                "system_preview": (system or "")[:200],
                "user_preview": (user or "")[:200],
            }
            return OrchestratorResult(content=str(raw), raw=raw)

        raw = self.fn(system, user)
        return OrchestratorResult(content=str(raw), raw=raw)


class AutogenOrchestrator(BaseOrchestrator):
    """
    Uses pyautogen with Ollama via AutogenOllamaClient.
    Everything is lazy-imported to avoid circular imports at module load time.
    """

    def __init__(self, ollama_base_url: str, model: str):
        # Lazy import to avoid circular import issues
        from llm.providers.ollama_autogen import AutogenOllamaClient

        self.client = AutogenOllamaClient(ollama_base_url=ollama_base_url, model=model)

    def generate_json(self, system: str, user: str) -> OrchestratorResult:
        return self.client.generate_json(system=system, user=user)


def build_orchestrator(ollama_base_url: str, model: str) -> BaseOrchestrator:
    """
    Attempts Autogen first. If not available, returns fallback with explicit reason.
    Never crashes import-time.
    """

    # 1) Detect obvious shadowing / missing dependency
    spec = importlib.util.find_spec("autogen")
    if spec is None:
        return FallbackOrchestrator(
            reason=(
                "Python package 'autogen' not found. "
                "Install it OR ensure you did not shadow it with a local 'autogen.py' or 'autogen/' folder."
            )
        )

    # 2) Try creating the real orchestrator (may still fail if autogen is incompatible)
    try:
        # Import inside try to avoid hard failure
        import autogen  # noqa: F401

        return AutogenOrchestrator(ollama_base_url=ollama_base_url, model=model)
    except Exception as e:
        return FallbackOrchestrator(reason=f"Autogen init failed: {type(e).__name__}: {e}")