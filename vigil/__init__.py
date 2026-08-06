"""
Vigil — plain-English error monitoring, Python SDK.

The server counterpart to the browser and Node SDKs, sending the same event envelope to
/api/ingest. Standard library only: no dependencies to audit, nothing to conflict with the
host app's own pins.

Prime directive, same as every Vigil SDK: never take the host process down. The uncaught
hook chains to whatever excepthook was already installed, so we observe the crash rather
than change it.

    import vigil
    vigil.init(key="vg_pub_...", endpoint="https://your-vigil/api/ingest")

    try:
        risky()
    except Exception:
        vigil.capture_exception()
        raise
"""
from __future__ import annotations

import atexit
import json
import os
import platform
import queue
import socket
import sys
import threading
import time
import traceback
import urllib.request
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

__version__ = "0.1.0"

_SDK_NAME = "vigil-python"
_BATCH_MAX = 20
_FLUSH_INTERVAL = 5.0


class _Client:
    def __init__(self, key: str, endpoint: str, environment: str,
                 release: Optional[str], tags: Optional[Dict[str, str]], debug: bool):
        self.key = key
        self.endpoint = endpoint
        self.environment = environment
        self.release = release
        self.tags = tags or {}
        self.debug = debug
        self._queue: "queue.Queue[dict]" = queue.Queue()
        self._dead = threading.Event()
        self._failures = 0
        # Daemon thread: it must never keep the process alive on its own.
        self._worker = threading.Thread(target=self._loop, name="vigil-flush", daemon=True)
        self._worker.start()
        self._prev_excepthook = sys.excepthook
        sys.excepthook = self._excepthook
        atexit.register(self.flush)

    def _log(self, *args: Any) -> None:
        if self.debug:
            try:
                print("[vigil]", *args, file=sys.stderr)
            except Exception:
                pass

    def _excepthook(self, exc_type, exc_value, tb):
        # Record the crash, then hand control back to the original hook so the process
        # behaves exactly as it would have without us.
        try:
            self._enqueue(self._event_from_exc(exc_type, exc_value, tb))
            self.flush()
        except Exception as e:  # a monitoring tool must not mask the real crash
            self._log("excepthook failed", e)
        finally:
            self._prev_excepthook(exc_type, exc_value, tb)

    def _base_context(self, extra_tags: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        tags = {
            "runtime": "python-" + platform.python_version(),
            "server_name": socket.gethostname(),
            **self.tags,
            **(extra_tags or {}),
        }
        ctx: Dict[str, Any] = {"environment": self.environment, "tags": tags}
        if self.release:
            ctx["release"] = self.release
        return ctx

    def _event_from_exc(self, exc_type, exc_value, tb, tags=None) -> Dict[str, Any]:
        frames: List[Dict[str, Any]] = []
        for frame in traceback.extract_tb(tb)[-40:]:
            frames.append({
                "file": frame.filename,
                "function": frame.name,
                "line": frame.lineno,
            })
        message = f"{exc_type.__name__}: {exc_value}"
        return {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "type": "error",
            "level": "error",
            "message": message[:4096],
            "exception": {
                "type": exc_type.__name__,
                "value": str(exc_value)[:4096],
                "stacktrace": frames,
            },
            "context": self._base_context(tags),
        }

    def _enqueue(self, event: Dict[str, Any]) -> None:
        if self._dead.is_set():
            return
        self._queue.put(event)

    def capture_exception(self, exc: Optional[BaseException] = None,
                          tags: Optional[Dict[str, str]] = None) -> None:
        if exc is None:
            exc_type, exc_value, tb = sys.exc_info()
            if exc_type is None:
                return
        else:
            exc_type, exc_value, tb = type(exc), exc, exc.__traceback__
        self._enqueue(self._event_from_exc(exc_type, exc_value, tb, tags))

    def capture_message(self, message: str, level: str = "info",
                        tags: Optional[Dict[str, str]] = None) -> None:
        self._enqueue({
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "type": "message",
            "level": level,
            "message": str(message)[:4096],
            "context": self._base_context(tags),
        })

    def _drain(self) -> List[dict]:
        batch: List[dict] = []
        while len(batch) < _BATCH_MAX:
            try:
                batch.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return batch

    def flush(self) -> None:
        if self._dead.is_set():
            return
        batch = self._drain()
        if not batch:
            return
        body = json.dumps({
            "sdk": {"name": _SDK_NAME, "version": __version__, "runtime": "python"},
            "project_key": self.key,
            "events": batch,
        }).encode("utf-8")
        req = urllib.request.Request(
            self.endpoint, data=body,
            headers={"Content-Type": "application/json"}, method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                if 200 <= resp.status < 300 or resp.status == 429:
                    self._failures = 0
                else:
                    self._trip()
        except Exception as e:
            self._log("flush failed", e)
            self._trip()

    def _trip(self) -> None:
        # An unhealthy endpoint must not become an unbounded retry loop.
        self._failures += 1
        if self._failures >= 3:
            self._log("circuit breaker tripped")
            self.close()

    def _loop(self) -> None:
        while not self._dead.is_set():
            time.sleep(_FLUSH_INTERVAL)
            try:
                self.flush()
            except Exception as e:
                self._log("loop flush failed", e)

    def close(self) -> None:
        self._dead.set()


_client: Optional[_Client] = None


def init(key: Optional[str] = None, endpoint: Optional[str] = None,
         environment: Optional[str] = None, release: Optional[str] = None,
         tags: Optional[Dict[str, str]] = None, debug: bool = False) -> None:
    """Start Vigil. Registers an excepthook and a background flush thread.

    ``key`` falls back to the ``VIGIL_KEY`` environment variable when omitted; an explicit
    argument always wins. ``endpoint`` defaults to the hosted ``https://vigill.dev/api/ingest``
    and never needs setting (it also honours ``VIGIL_ENDPOINT``, which Vigil's own dev/CI use).
    With no key from either source the SDK stays disabled rather than raising — it must
    never break the host process.
    """
    global _client
    if _client is not None:
        return
    key = key or os.environ.get("VIGIL_KEY")
    if not key:
        if debug:
            print("[vigil] no key supplied and VIGIL_KEY is unset — disabled")
        return
    endpoint = endpoint or os.environ.get("VIGIL_ENDPOINT") or "https://vigill.dev/api/ingest"
    env = environment or os.environ.get("ENV") or os.environ.get("ENVIRONMENT") or "production"
    _client = _Client(key, endpoint, env, release, tags, debug)


def capture_exception(exc: Optional[BaseException] = None,
                      tags: Optional[Dict[str, str]] = None) -> None:
    """Capture the current (or given) exception. Call inside an except block."""
    if _client is not None:
        _client.capture_exception(exc, tags)


def capture_message(message: str, level: str = "info",
                    tags: Optional[Dict[str, str]] = None) -> None:
    if _client is not None:
        _client.capture_message(message, level, tags)


def flush() -> None:
    if _client is not None:
        _client.flush()


def close() -> None:
    global _client
    if _client is not None:
        _client.close()
        _client = None
