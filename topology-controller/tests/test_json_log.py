import io
import json
import logging
import re
import sys
from pathlib import Path

from adapters.json_log import JsonFormatter, json_handler


# The record's own level is `at_level`, so an `extra` named `level` reaches the record as a field.
def _record(msg="hello %s", args=("world",), at_level=logging.INFO, exc_info=None, **extra):
    return logging.getLogger("service.x").makeRecord(
        "service.x", at_level, __file__, 1, msg, args, exc_info, extra=extra)


def _line(record):
    return json.loads(JsonFormatter().format(record))


def test_a_line_carries_time_level_logger_message_and_every_extra_field():
    line = _line(_record(release_id="rel-1", count=2))
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", line["time"])
    assert (line["level"], line["logger"], line["msg"]) == ("INFO", "service.x", "hello world")
    assert (line["release_id"], line["count"]) == ("rel-1", 2)


def test_warning_and_critical_take_the_go_services_level_names():
    assert _line(_record(at_level=logging.WARNING))["level"] == "WARN"
    assert _line(_record(at_level=logging.CRITICAL))["level"] == "ERROR"


def test_an_exception_is_printed_with_its_traceback():
    try:
        raise ValueError("boom")
    except ValueError:
        info = sys.exc_info()
    exception = _line(_record(exc_info=info))["exception"]
    assert "Traceback" in exception and "ValueError: boom" in exception


def test_an_extra_never_replaces_a_core_key():
    line = _line(_record(**{"level": "x", "time": "y", "logger": "z"}))
    assert (line["level"], line["logger"]) == ("INFO", "service.x")


def test_a_detail_with_newlines_escapes_and_unencodable_values_stays_one_line():
    detail = "Invalid expression\n  select fro\x1b[4mm\x1b[0m x é"
    text = JsonFormatter().format(_record(msg="Published manifest.loaded.candidate failed", args=(),
                                          detail=detail, raw=b"\xff", ids={"a"}, path=Path("/tmp/x")))
    assert "\n" not in text
    line = json.loads(text)
    assert (line["detail"], line["path"], line["raw"]) == (detail, "/tmp/x", "b'\\xff'")


def test_a_field_the_encoder_refuses_is_printed_as_text_and_the_line_survives():
    cycle = {}
    cycle["self"] = cycle
    text = JsonFormatter().format(_record(msg="kept", args=(), grouped={("a", 1): "x"}, cycle=cycle, count=3))
    assert "\n" not in text
    line = json.loads(text)
    assert (line["msg"], line["count"]) == ("kept", 3)
    assert line["grouped"] == str({("a", 1): "x"})
    assert line["cycle"] == str(cycle)


def test_the_handler_writes_one_json_object_per_record():
    stream = io.StringIO()
    logger = logging.getLogger("tests.json_handler")
    logger.propagate = False
    handler = json_handler(stream)
    logger.addHandler(handler)
    try:
        logger.warning("first", extra={"a": 1})
        logger.warning("second")
    finally:
        logger.removeHandler(handler)
    lines = [json.loads(text) for text in stream.getvalue().splitlines()]
    assert [(line["msg"], line.get("a")) for line in lines] == [("first", 1), ("second", None)]
