"""Guard: no SQL dialect literal in manifest-controller's service code.

The dialect is a property of the operator's warehouse, resolved from
WAREHOUSE_ENGINE at the composition root and injected downward. A literal baked
into service code works for whoever wrote it and silently reads or emits the
wrong engine's SQL everywhere else — the exact failure this service shipped
before the engine reached it. Tests may name a dialect; production code may not.
"""
import re
from pathlib import Path

SERVICE_DIR = Path(__file__).parent.parent / "service"

# Matches an assignment of a string literal, e.g. dialect="postgres".
# dialect=dialect and the `dialect: str` annotations do not match.
_LITERAL_DIALECT = re.compile(r"""dialect\s*=\s*["']""")


def test_no_hardcoded_sql_dialect_in_service_code():
    offenders = []
    for path in sorted(SERVICE_DIR.glob("*.py")):
        for lineno, line in enumerate(path.read_text().splitlines(), start=1):
            if _LITERAL_DIALECT.search(line):
                offenders.append(f"{path.name}:{lineno}: {line.strip()}")

    assert not offenders, (
        "SQL dialect hardcoded in service code — take it from the injected "
        "dialect instead:\n" + "\n".join(offenders)
    )


def test_guard_would_catch_a_regression(tmp_path):
    """The guard's pattern actually matches the shape it is meant to reject."""
    assert _LITERAL_DIALECT.search('parsed = sqlglot.parse_one(sql, dialect="postgres")')
    assert _LITERAL_DIALECT.search("return parsed.sql(dialect='trino')")
    assert not _LITERAL_DIALECT.search("parsed = sqlglot.parse_one(sql, dialect=dialect)")
    assert not _LITERAL_DIALECT.search("    dialect: str,")
