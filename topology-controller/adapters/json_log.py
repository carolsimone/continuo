"""Writes every log record as one JSON object on one line.

A line carries `time` (UTC, millisecond precision), `level`, `logger` and `msg`,
then every field a caller passed in `extra=`, then `exception` and `stack` when
the record has them. A field a caller passes never replaces one of the four
core keys, so a log collector can always rely on them.
"""
import json
import logging
import sys
from datetime import datetime, timezone
from typing import TextIO

# The attributes a bare LogRecord has. Any other attribute of a record came in
# through `extra=` and is printed as a field of the line.
_RESERVED = frozenset(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {"message", "asctime"}

# The Go services print `WARN` and `ERROR`; the two levels Python names
# differently take those names, so one query reads the logs of every service.
_LEVEL_NAMES = {"WARNING": "WARN", "CRITICAL": "ERROR"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "time": datetime.fromtimestamp(record.created, timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": _LEVEL_NAMES.get(record.levelname, record.levelname),
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for name, value in vars(record).items():
            if name not in _RESERVED:
                payload.setdefault(name, value)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        elif record.exc_text:
            payload["exception"] = record.exc_text
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)
        try:
            return json.dumps(payload, default=str, ensure_ascii=False)
        except (TypeError, ValueError):
            # A value `default=str` cannot reach (a dict keyed by tuples, a
            # structure that contains itself) is printed as its text, so a
            # field the encoder refuses never costs the line.
            return json.dumps(
                {name: value if isinstance(value, (str, int, float, bool, type(None))) else str(value)
                 for name, value in payload.items()},
                default=str,
                ensure_ascii=False,
            )


def json_handler(stream: TextIO) -> logging.Handler:
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    return handler


def configure_logging(level: int) -> None:
    """Sends the root logger's records to stderr as JSON lines at `level`.

    `basicConfig` does nothing when the root logger already has a handler, so
    the level is set on the root logger again either way.
    """
    logging.basicConfig(handlers=[json_handler(sys.stderr)], level=level)
    logging.getLogger().setLevel(level)
