from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict
import json
import re


@dataclass
class AutogenResponse:
    content: str
    raw: Dict[str, Any]


class AutogenOllamaClient:
    """
    Uses Autogen + Ollama backend to produce STRICT JSON outputs.

    Notes:
    - Autogen API/paths differ across versions, so imports are handled defensively.
    - We run a single turn: user -> assistant.
    - We attempt multiple ways to extract the assistant's last message content.
    """

    def __init__(self, ollama_base_url: str, model: str):
        self.ollama_base_url = ollama_base_url
        self.model = model

    def generate_json(self, system: str, user: str) -> AutogenResponse:
        # 1) Import autogen + agents in a version-tolerant way
        try:
            import autogen  # noqa: F401
        except Exception:
            return AutogenResponse(content="", raw={})

        AssistantAgent, UserProxyAgent = self._resolve_agents()

        if AssistantAgent is None or UserProxyAgent is None:
            # autogen installed but API changed too much / missing agentchat
            return AutogenResponse(content="", raw={})

        # 2) Build llm_config for Ollama
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

        # 3) Create agents
        assistant = AssistantAgent(
            name="assistant",
            llm_config=llm_config,
            system_message=system,
        )
        user_agent = UserProxyAgent(
            name="user",
            human_input_mode="NEVER",
            code_execution_config=False,
            max_consecutive_auto_reply=1,
        )

        # 4) Run one round
        try:
            user_agent.initiate_chat(assistant, message=user)
        except Exception:
            return AutogenResponse(content="", raw={})

        # 5) Extract last message robustly
        content = self._extract_last_assistant_content(assistant=assistant, user_agent=user_agent)

        # 6) Parse JSON strictly
        raw = self._safe_parse_json(content)
        return AutogenResponse(content=content, raw=raw)

    # -----------------------------
    # Autogen version compatibility
    # -----------------------------
    def _resolve_agents(self):
        """
        Try multiple import styles:
        - Classic: autogen.AssistantAgent, autogen.UserProxyAgent
        - Newer:  autogen.agentchat.AssistantAgent, autogen.agentchat.UserProxyAgent
        """
        try:
            import autogen
            AssistantAgent = getattr(autogen, "AssistantAgent", None)
            UserProxyAgent = getattr(autogen, "UserProxyAgent", None)
            if AssistantAgent and UserProxyAgent:
                return AssistantAgent, UserProxyAgent
        except Exception:
            pass

        try:
            from autogen.agentchat import AssistantAgent, UserProxyAgent  # type: ignore
            return AssistantAgent, UserProxyAgent
        except Exception:
            return None, None

    def _extract_last_assistant_content(self, assistant: Any, user_agent: Any) -> str:
        # Strategy A: assistant.chat_messages[user_agent]
        try:
            msgs = assistant.chat_messages.get(user_agent, [])  # type: ignore[attr-defined]
            if msgs:
                c = msgs[-1].get("content", "")
                if isinstance(c, str) and c.strip():
                    return c
        except Exception:
            pass

        # Strategy B: assistant.last_message() (some versions)
        try:
            last = assistant.last_message()  # type: ignore[attr-defined]
            if isinstance(last, dict):
                c = last.get("content", "")
                if isinstance(c, str) and c.strip():
                    return c
        except Exception:
            pass

        # Strategy C: user_agent.chat_messages[assistant]
        try:
            msgs = user_agent.chat_messages.get(assistant, [])  # type: ignore[attr-defined]
            if msgs:
                c = msgs[-1].get("content", "")
                if isinstance(c, str) and c.strip():
                    return c
        except Exception:
            pass

        return ""

    # -----------------------------
    # JSON parsing helpers
    # -----------------------------
    def _safe_parse_json(self, text: str) -> Dict[str, Any]:
        if not text or not isinstance(text, str):
            return {}

        t = text.strip()

        # Remove markdown fences safely
        t = self._strip_code_fences(t)

        # Try direct json first
        obj = self._try_json(t)
        if isinstance(obj, dict):
            return obj

        # Extract first {...} block
        blob = self._extract_first_json_object(t)
        if blob:
            obj = self._try_json(blob)
            if isinstance(obj, dict):
                return obj

        return {}

    def _strip_code_fences(self, t: str) -> str:
        # ```json ... ``` or ``` ... ```
        if t.startswith("```"):
            # remove first fence line
            t = re.sub(r"^```[a-zA-Z]*\s*", "", t)
            # remove ending fence
            t = re.sub(r"\s*```$", "", t.strip())
        return t.strip()

    def _extract_first_json_object(self, t: str) -> str:
        start = t.find("{")
        if start == -1:
            return ""
        # Find matching closing brace by scanning
        depth = 0
        for i in range(start, len(t)):
            ch = t[i]
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return t[start : i + 1]
        return ""

    def _try_json(self, s: str):
        try:
            return json.loads(s)
        except Exception:
            return None