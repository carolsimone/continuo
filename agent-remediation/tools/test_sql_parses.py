"""Tests of sql_parses.py, run as agent-remediation runs it: the script on
stdin, under the interpreter running these tests, in isolated mode, with one
JSON result line read from its stdout."""

import json
import subprocess
import sys
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("sql_parses.py")


def parse(sql: str, dialect: str = "postgres") -> dict:
    """Runs the script on sql and returns its decoded result line."""
    proc = subprocess.run(
        [sys.executable, "-I", str(SCRIPT), "--dialect", dialect],
        input=sql,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    if proc.returncode != 0:
        raise AssertionError(f"exit {proc.returncode}: {proc.stderr}")
    lines = proc.stdout.splitlines()
    if len(lines) != 1:
        raise AssertionError(f"want one stdout line, got {proc.stdout!r}")
    return json.loads(lines[0])


class SQLParsesTest(unittest.TestCase):
    def test_valid_postgres_parses(self):
        result = parse("select id, total::numeric as amount\nfrom analytics.orders\nwhere id > 1\n")
        self.assertEqual(result, {"errors": [], "skipped": ""})

    def test_config_block_is_removed(self):
        sql = "{{ config(\n    materialized='table',\n    tags=['daily']\n) }}\nselect 1 as x\n"
        self.assertEqual(parse(sql), {"errors": [], "skipped": ""})

    def test_config_with_a_nested_expression_in_a_hook_is_removed(self):
        sql = (
            "{{ config(\n"
            "    materialized='table',\n"
            "    post_hook=\"insert into {{ ref('audit') }} select 1\"\n"
            ") }}\n"
            "select 1 as x\n"
        )
        self.assertEqual(parse(sql), {"errors": [], "skipped": ""})

    def test_config_with_a_parenthesis_in_a_quoted_argument_is_removed(self):
        sql = "{{ config(alias='orders) }}x', tags=[\"a(b\"]) }}\nselect 1 as x\n"
        self.assertEqual(parse(sql), {"errors": [], "skipped": ""})

    def test_config_removal_keeps_the_line_numbers_after_a_nested_expression(self):
        sql = "{{ config(\n  post_hook=\"grant select on {{ this }} to r\"\n) }}\nselect id\nfrom where\n"
        result = parse(sql)
        self.assertTrue(result["errors"], result)
        self.assertEqual(result["errors"][0]["line"], 5)

    def test_jinja_comment_is_removed(self):
        for dialect in ("postgres", "trino"):
            with self.subTest(dialect=dialect):
                self.assertEqual(parse("{# don't touch #}\nselect 1\n", dialect), {"errors": [], "skipped": ""})

    def test_jinja_comment_keeps_the_line_numbers(self):
        result = parse("{# a comment\n   that isn't short #}\nselect id\nfrom where\n")
        self.assertTrue(result["errors"], result)
        self.assertEqual(result["errors"][0]["line"], 4)

    def test_jinja_comment_holding_a_statement_tag_does_not_skip(self):
        self.assertEqual(parse("{# {% if x %} #}\nselect 1\n"), {"errors": [], "skipped": ""})

    def test_expression_becomes_a_placeholder_name(self):
        result = parse("select * from {{ ref('orders') }}")
        self.assertEqual(result, {"errors": [], "skipped": ""})

    def test_statement_tag_skips_the_parse(self):
        sql = "select\n{% for c in ['a', 'b'] %}\n  {{ c }},\n{% endfor %}\n  1 from t\n"
        result = parse(sql)
        self.assertEqual(result["errors"], [])
        self.assertEqual(result["skipped"], "jinja statements: only dbt can render them")

    def test_broken_sql_reports_its_line(self):
        # The config block spans two lines: removing it keeps the line numbers
        # of the file, so the error is reported on the file's own line 4.
        sql = "{{ config(\n  materialized='view') }}\nselect id\nfrom where\n"
        result = parse(sql)
        self.assertEqual(result["skipped"], "")
        self.assertTrue(result["errors"], result)
        for err in result["errors"]:
            self.assertIsInstance(err["message"], str)
            self.assertTrue(err["message"])
        self.assertEqual(result["errors"][0]["line"], 4)

    def test_unterminated_string_reports_its_line(self):
        result = parse("select 1\nfrom t\nwhere x = 'abc\n")
        self.assertEqual(len(result["errors"]), 1, result)
        self.assertEqual(result["errors"][0]["line"], 3)

    def test_every_error_message_is_one_line(self):
        # sqlglot quotes the text around a tokenizer error, newlines included;
        # a finding is shown to the model as one line of a list.
        for sql in ("select 1\nfrom t\nwhere x = 'abc\n", "select a,\n  from\n where", "select (\n1\n"):
            with self.subTest(sql=sql):
                result = parse(sql)
                self.assertTrue(result["errors"], result)
                for err in result["errors"]:
                    self.assertNotIn("\n", err["message"])
                    self.assertNotIn("\r", err["message"])
                    self.assertEqual(err["message"], " ".join(err["message"].split()))

    def test_token_error_message_keeps_the_quoted_text_on_one_line(self):
        result = parse("select 1\nfrom t\nwhere x = 'abc\n")
        self.assertEqual(result["errors"][0]["message"], "Error tokenizing 'select 1 from t where x = 'abc'")

    def test_a_clause_emitting_macro_does_not_parse_once_reduced(self):
        # A macro that renders a whole clause becomes a name where no name can
        # stand, so the reduced file does not parse; the check therefore judges
        # an edit only when the file it replaces parses under the same reduction.
        for sql in (
            "select a, count(*) from {{ ref('x') }} where a > 1 {{ dbt_utils.group_by(1) }}",
            "select * from {{ ref('x') }} as x {{ incremental_where() }}",
        ):
            with self.subTest(sql=sql):
                result = parse(sql)
                self.assertEqual(result["skipped"], "")
                self.assertTrue(result["errors"], result)

    def test_trino_dialect_is_used(self):
        sql = "select json_query(payload, 'lax $.items' with array wrapper) as items from raw.events"
        self.assertEqual(parse(sql, "trino"), {"errors": [], "skipped": ""})
        self.assertTrue(parse(sql, "postgres")["errors"])

    def test_token_error_without_offsets_reports_line_one(self):
        import sql_parses  # the tools directory is on sys.path under discover

        class BareTokenError(Exception):
            pass

        self.assertEqual(sql_parses._token_error_line("select 1\nfrom t", BareTokenError("boom")), 1)

    def test_unknown_dialect_fails(self):
        proc = subprocess.run(
            [sys.executable, "-I", str(SCRIPT), "--dialect", "no-such-dialect"],
            input="select 1",
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proc.stdout, "")
        self.assertIn("no-such-dialect", proc.stderr)


if __name__ == "__main__":
    unittest.main()
