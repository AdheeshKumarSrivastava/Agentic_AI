from __future__ import annotations

import re


def redact_connection_string(s: str) -> str:
    if not s:
        return s
    # naive redaction for pwd=... or Password=...
    s = re.sub(r"(Pwd=)([^;]+)", r"\1***", s, flags=re.IGNORECASE)
    s = re.sub(r"(Password=)([^;]+)", r"\1***", s, flags=re.IGNORECASE)
    s = re.sub(r"(Uid=)([^;]+)", r"\1***", s, flags=re.IGNORECASE)
    return s
