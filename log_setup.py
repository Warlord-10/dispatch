"""Logging for every process type: api, worker, gateway.

One line per event, always carrying which process wrote it and what it was
working on, so a single grep follows one request or one ride across processes:

  2026-10-01 14:02:11,305 INFO    worker pid=4120 ctx=ride=7f3a… services.workers.dispatch_worker: round 1: offered to 3 of 3 nearby driver(s)

`ctx` is a request id in the API, a ride id in the worker and a socket owner in
the gateway. Set it with `log_context.set(...)` where the unit of work starts.

ponytail: plain text, stdlib only. JSON logs and trace ids that follow a ride
across the queue hop are Phase 13.
"""

import contextvars
import logging
import os

log_context: contextvars.ContextVar[str] = contextvars.ContextVar("log_context", default="-")

_service = "-"


class _AddContext(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.service = _service
        record.ctx = log_context.get()
        return True


def setup_logging(service: str) -> None:
    """Idempotent: safe to call again, e.g. once the gateway knows its id."""
    global _service
    _service = service
    handler = logging.StreamHandler()
    handler.addFilter(_AddContext())
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)-7s %(service)s pid=%(process)d ctx=%(ctx)s %(name)s: %(message)s"
    ))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    # Library chatter (every connect, every channel) drowns the events that matter.
    for noisy in ("aio_pika", "aiormq", "httpx"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
