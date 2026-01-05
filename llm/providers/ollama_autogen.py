from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict
import json


@dataclass
class AutogenResponse:
    content: str
    raw: Dict[str, Any]


class AutogenOllamaClient:
    """
    Uses pyautogen + ollama backend to produce JSON.
    We keep the interaction minimal: one assistant with system + user prompt.
    """

    def __init__(self, ollama_base_url: str, model: str):
        self.ollama_base_url = ollama_base_url
        self.model = model

    def generate_json(self, system: str, user: str) -> AutogenResponse:
        try:
            import autogen
        except Exception:
            # fallback behavior expected upstream
            return AutogenResponse(content="{}", raw={})

        llm_config = {
            "config_list": [
                {
                    "model": self.model,
                    "base_url": self.ollama_base_url,
                    "api_type": "ollama",
                }
            ],
            "temperature": 0.2,
        }

        assistant = autogen.AssistantAgent(
            name="assistant",
            llm_config=llm_config,
            system_message=system,
        )
        user_agent = autogen.UserProxyAgent(
            name="user",
            human_input_mode="NEVER",
            code_execution_config=False,
            max_consecutive_auto_reply=1,
        )

        # Run one round
        user_agent.initiate_chat(assistant, message=user)

        # Extract last message content
        content = ""
        try:
            msgs = assistant.chat_messages[user_agent]
            content = msgs[-1]["content"] if msgs else ""
        except Exception:
            content = ""

        # Parse JSON strictly
        raw = self._safe_parse_json(content)
        return AutogenResponse(content=content, raw=raw)

    def _safe_parse_json(self, text: str) -> Dict[str, Any]:
        if not text:
            return {}
        t = text.strip()

        # If model wraps in markdown fences, strip.
        if t.startswith("```"):
            t = t.strip("`")
            # attempt to remove "json" hint
            t = t.replace("json", "", 1).strip()

        # Find first { ... } block
        start = t.find("{")
        end = t.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return {}
        blob = t[start : end + 1]
        try:
            obj = json.loads(blob)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
