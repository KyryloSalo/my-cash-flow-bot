"""Process-wide Telegram URL redaction; install before HTTP clients/logging config.

A record factory also covers subsequently-created handlers and exception text.
Keep identical in bot and admin contexts (each is an isolated Docker build).
"""
from __future__ import annotations
import logging
import re
import traceback

_TELEGRAM_URL = re.compile(r"(?i)(/bot)[^/\s?\#\"']+")
_BARE_TOKEN = re.compile(r"\b[0-9]{5,}:[A-Za-z0-9_-]{20,}\b")


def redact(value: object) -> str:
    return _BARE_TOKEN.sub("[REDACTED]", _TELEGRAM_URL.sub(r"\1[REDACTED]", str(value)))


def _redact_arg(value: object) -> object:
    rendered = str(value)
    safe = redact(rendered)
    return safe if safe != rendered else value


def install_log_safety() -> None:
    for name in ("httpx", "httpcore"):
        logging.getLogger(name).setLevel(logging.WARNING)
    previous = logging.getLogRecordFactory()
    if getattr(previous, "_telegram_redaction", False):
        return

    def safe_record(*args, **kwargs):
        record = previous(*args, **kwargs)
        record.msg = redact(record.msg)
        if isinstance(record.args, tuple):
            record.args = tuple(_redact_arg(value) for value in record.args)
        elif isinstance(record.args, dict):
            record.args = {key: _redact_arg(value) for key, value in record.args.items()}
        if record.exc_info:
            record.exc_text = redact("".join(traceback.format_exception(*record.exc_info)))
            record.exc_info = None
        if record.stack_info:
            record.stack_info = redact(record.stack_info)
        return record

    safe_record._telegram_redaction = True
    logging.setLogRecordFactory(safe_record)
