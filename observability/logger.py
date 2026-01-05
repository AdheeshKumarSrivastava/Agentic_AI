from __future__ import annotations

import logging
import json
from pathlib import Path
from typing import Any, Dict

from config import Settings


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "level": record.levelname,
            "name": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(settings: Settings) -> None:
    Path(settings.LOG_DIR).mkdir(parents=True, exist_ok=True)
    log_path = Path(settings.LOG_DIR) / "app.log"

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # console
    ch = logging.StreamHandler()
    ch.setFormatter(JsonFormatter())
    root.addHandler(ch)

    # file
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(JsonFormatter())
    root.addHandler(fh)
