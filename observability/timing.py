from __future__ import annotations

from contextlib import contextmanager
import time
import logging

log = logging.getLogger("timing")


@contextmanager
def timed_block(name: str):
    t0 = time.time()
    try:
        yield
    finally:
        dt = time.time() - t0
        log.info(f"{name} took {dt:.4f}s")
