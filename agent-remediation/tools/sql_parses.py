"""Parses one dbt model's SQL with sqlglot, in the warehouse's dialect.

agent-remediation runs this script on every SQL file a draft fix writes, with
the file on stdin and the install's dialect in --dialect, and reads the one
JSON line it prints on stdout:

    {"errors": [{"message": str, "line": int}], "skipped": str}

Before parsing, the dbt Jinja is reduced to plain SQL:

1. every ``{{ config(...) }}`` call is removed, across lines;
2. if a ``{%`` statement tag remains, the file is not parsed and ``skipped``
   says why: only dbt can render a statement;
3. every other ``{{ ... }}`` expression becomes the name ``__jinja__``.

Each removed or replaced span keeps its newlines, so a reported line is the
line of the file. Diagnostics go to stderr through logging; stdout carries only
the result line. A dialect sqlglot does not know exits non-zero.
"""

import argparse
import json
import logging
import re
import sys

import sqlglot
from sqlglot.dialects.dialect import Dialect
from sqlglot.errors import ParseError, TokenError

log = logging.getLogger("sql_parses")

PLACEHOLDER = "__jinja__"
STATEMENT_SKIP = "jinja statements: only dbt can render them"

_CONFIG_CALL = re.compile(r"\{\{-?\s*config\s*\(.*?\)\s*-?\}\}", re.DOTALL)
_EXPRESSION = re.compile(r"\{\{.*?\}\}", re.DOTALL)

# sqlglot's tokenizer quotes the text from 50 characters before the point it
# failed at to 50 characters after it, and gives the quoted span's offsets.
_TOKEN_CONTEXT = 50


def _newlines_of(text: str) -> str:
    return "\n" * text.count("\n")


def reduce_jinja(text: str) -> tuple[str, str]:
    """Returns the SQL left once the Jinja is reduced, and the reason the file
    is skipped (empty when it is not)."""
    text = _CONFIG_CALL.sub(lambda m: _newlines_of(m.group(0)), text)
    if "{%" in text:
        return "", STATEMENT_SKIP
    text = _EXPRESSION.sub(lambda m: PLACEHOLDER + _newlines_of(m.group(0)), text)
    return text, ""


def _line_at(text: str, offset: int) -> int:
    return text.count("\n", 0, max(0, min(offset, len(text)))) + 1


def _token_error_line(text: str, err: TokenError) -> int:
    """The line the tokenizer failed on, read from the offsets of the span its
    message quotes; line 1 when the error carries none."""
    if err.start is None or err.end is None:
        return 1
    if err.start > 0:
        failed_at = err.start + _TOKEN_CONTEXT
    elif err.end < len(text) - 1:
        failed_at = err.end - _TOKEN_CONTEXT
    else:
        failed_at = len(text)
    return _line_at(text.rstrip("\n"), failed_at)


def parse_errors(text: str, dialect: str) -> list[dict]:
    """Parses text in dialect and returns each error with its line."""
    try:
        sqlglot.parse(text, read=dialect)
    except ParseError as err:
        errors = [
            {"message": str(e.get("description") or err), "line": int(e.get("line") or 1)}
            for e in err.errors
        ]
        return errors or [{"message": str(err), "line": 1}]
    except TokenError as err:
        return [{"message": str(err), "line": _token_error_line(text, err)}]
    return []


def check(text: str, dialect: str) -> dict:
    """The result line for one file."""
    sql, skipped = reduce_jinja(text)
    if skipped:
        return {"errors": [], "skipped": skipped}
    return {"errors": parse_errors(sql, dialect), "skipped": ""}


def main(argv: list[str]) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Parse dbt model SQL from stdin with sqlglot.")
    parser.add_argument("--dialect", required=True, help="the sqlglot dialect of the warehouse")
    args = parser.parse_args(argv)
    if not args.dialect:
        log.error("--dialect is empty")
        return 2
    try:
        Dialect.get_or_raise(args.dialect)
    except ValueError as err:
        log.error("dialect %r is not one sqlglot knows: %s", args.dialect, err)
        return 2
    result = check(sys.stdin.read(), args.dialect)
    sys.stdout.write(json.dumps(result) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
