"""Parses one dbt model's SQL with sqlglot, in the warehouse's dialect.

agent-remediation runs this script on every SQL file a draft fix writes, with
the file on stdin and the install's dialect in --dialect, and reads the one
JSON line it prints on stdout:

    {"errors": [{"message": str, "line": int}], "skipped": str}

Before parsing, the dbt Jinja is reduced to plain SQL:

1. every ``{# ... #}`` comment is removed;
2. every ``{{ config(...) }}`` call is removed, across lines, its end found by
   a scanner that skips quoted text and nested ``( )`` and ``{{ }}``, so a hook
   holding ``{{ ref(...) }}`` or a ``)`` inside a quoted argument is removed
   whole;
3. if a ``{%`` statement tag remains, the file is not parsed and ``skipped``
   says why: only dbt can render a statement;
4. every other ``{{ ... }}`` expression becomes the name ``__jinja__``.

Each removed or replaced span keeps its newlines, so a reported line is the
line of the file. Every error message is one line: whitespace in it, the
newlines of the text sqlglot quotes included, becomes one space. Diagnostics go to stderr through logging; stdout carries only
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

_COMMENT = re.compile(r"\{#.*?#\}", re.DOTALL)
_CONFIG_START = re.compile(r"\{\{-?\s*config\s*\(")
_CONFIG_CLOSE = re.compile(r"\s*-?\}\}")
_EXPRESSION = re.compile(r"\{\{.*?\}\}", re.DOTALL)

# sqlglot's tokenizer quotes the text from 50 characters before the point it
# failed at to 50 characters after it, and gives the quoted span's offsets.
_TOKEN_CONTEXT = 50


def _newlines_of(text: str) -> str:
    return "\n" * text.count("\n")


def _call_end(text: str, start: int) -> int | None:
    """The offset just past the ``)`` that closes the call whose ``(`` is at
    start - 1, or None when the text ends first. Quoted text is skipped whole,
    a backslash escaping the character after it, and ``( )`` and ``{{ }}``
    outside quotes nest."""
    parens, braces, quote = 1, 0, ""
    i = start
    while i < len(text):
        ch = text[i]
        if quote:
            if ch == "\\":
                i += 2
                continue
            if ch == quote:
                quote = ""
        elif ch in "'\"":
            quote = ch
        elif text.startswith("{{", i):
            braces += 1
            i += 2
            continue
        elif text.startswith("}}", i) and braces:
            braces -= 1
            i += 2
            continue
        elif ch == "(":
            parens += 1
        elif ch == ")":
            parens -= 1
            if parens == 0 and braces == 0:
                return i + 1
        i += 1
    return None


def _remove_config_calls(text: str) -> str:
    """text with every ``{{ config(...) }}`` call replaced by its newlines. A
    call whose end cannot be found is left as it is."""
    out, pos = [], 0
    while (m := _CONFIG_START.search(text, pos)) is not None:
        end = _call_end(text, m.end())
        close = _CONFIG_CLOSE.match(text, end) if end is not None else None
        if close is None:
            out.append(text[pos : m.end()])
            pos = m.end()
            continue
        out.append(text[pos : m.start()])
        out.append(_newlines_of(text[m.start() : close.end()]))
        pos = close.end()
    out.append(text[pos:])
    return "".join(out)


def reduce_jinja(text: str) -> tuple[str, str]:
    """Returns the SQL left once the Jinja is reduced, and the reason the file
    is skipped (empty when it is not)."""
    text = _COMMENT.sub(lambda m: _newlines_of(m.group(0)), text)
    text = _remove_config_calls(text)
    if "{%" in text:
        return "", STATEMENT_SKIP
    text = _EXPRESSION.sub(lambda m: PLACEHOLDER + _newlines_of(m.group(0)), text)
    return text, ""


def _line_at(text: str, offset: int) -> int:
    return text.count("\n", 0, max(0, min(offset, len(text)))) + 1


def _token_error_line(text: str, err: TokenError) -> int:
    """The line the tokenizer failed on, read from the offsets of the span its
    message quotes; line 1 when the error carries none."""
    start, end = getattr(err, "start", None), getattr(err, "end", None)
    if start is None or end is None:
        return 1
    if start > 0:
        failed_at = start + _TOKEN_CONTEXT
    elif end < len(text) - 1:
        failed_at = end - _TOKEN_CONTEXT
    else:
        failed_at = len(text)
    return _line_at(text.rstrip("\n"), failed_at)


def _one_line(message: object) -> str:
    """message with every run of whitespace, newlines included, made one
    space: sqlglot quotes the text around an error as the file holds it."""
    return " ".join(str(message).split())


def parse_errors(text: str, dialect: str) -> list[dict]:
    """Parses text in dialect and returns each error, its message on one line,
    with its line."""
    try:
        sqlglot.parse(text, read=dialect)
    except ParseError as err:
        errors = [
            {"message": _one_line(e.get("description") or err), "line": int(e.get("line") or 1)}
            for e in err.errors
        ]
        return errors or [{"message": _one_line(err), "line": 1}]
    except TokenError as err:
        return [{"message": _one_line(err), "line": _token_error_line(text, err)}]
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
