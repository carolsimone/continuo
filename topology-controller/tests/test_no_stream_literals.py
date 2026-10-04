"""Guard: no production module spells a versioned stream name.

A Redis stream is named once, in pkg/streams/contract.yaml, and reaches Python
through the generated streams_contract module. A literal such as
"release.requested:v1" in a log line or an error message keeps the old name
after a rename — and an error message ends up in the dead letter's error field.
This guard AST-scans every production module for string constants shaped like a
versioned stream name. Docstrings are exempt, and so are the generated
streams_contract module and the tests (a test names a stream through the
constant too, but it is the production code this guard protects).
"""
import ast
import re
from pathlib import Path

ROOT = Path(__file__).parent.parent
_VERSIONED_STREAM = re.compile(r"[a-z][a-z0-9_.]*:v[0-9]+")
_DOCSTRING_HOLDERS = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
_EXCLUDED_DIRS = {".venv", "tests", "__pycache__"}


def _stream_literals(source: str) -> list[str]:
    """Return every versioned-stream-shaped string constant in a Python source
    string, f-string literal parts included and docstrings left out."""
    tree = ast.parse(source)
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, _DOCSTRING_HOLDERS) and node.body:
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                docstrings.add(id(first.value))
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
        and _VERSIONED_STREAM.search(node.value)
    ]


def _production_modules() -> list[Path]:
    return sorted(
        path for path in ROOT.rglob("*.py")
        if not _EXCLUDED_DIRS.intersection(path.relative_to(ROOT).parts)
        and path.relative_to(ROOT) != Path("streams_contract.py")
    )


def test_no_production_module_spells_a_versioned_stream_name():
    modules = _production_modules()
    assert modules, "the scan found no production modules: the guard is broken"

    offenders = [
        f"{path.relative_to(ROOT)}: {literal!r}"
        for path in modules
        for literal in _stream_literals(path.read_text())
    ]

    assert not offenders, (
        "a versioned stream name is spelled inline — use the streams_contract "
        "constant instead:\n" + "\n".join(offenders)
    )


def test_the_scan_covers_the_composition_root_and_the_adapters():
    names = {str(path.relative_to(ROOT)) for path in _production_modules()}
    assert "main.py" in names
    assert "adapters/redis/consumer.py" in names
    assert "streams_contract.py" not in names
    assert not any(name.startswith("tests/") for name in names)


def test_the_detector_flags_the_shapes_it_is_meant_to_reject():
    assert _stream_literals('x = "release.requested:v1"') == ["release.requested:v1"]
    assert _stream_literals('raise ValueError("release.requested:v1 payload missing")') == [
        "release.requested:v1 payload missing"
    ]
    assert _stream_literals('msg = f"{x} on query.model:v2 failed"') == [" on query.model:v2 failed"]
    assert _stream_literals('f(stream="manifest.loaded.candidate:v1")') == ["manifest.loaded.candidate:v1"]


def test_the_detector_leaves_docstrings_and_unrelated_strings_alone():
    documented = '"""Module reading release.requested:v1."""\n' \
        'class A:\n    """Handles query.model:v1."""\n' \
        '    def f(self):\n        """Emits run.finalized:v1."""\n'
    assert _stream_literals(documented) == []
    # Only the first expression of a body is a docstring: a later bare string is code.
    assert _stream_literals('def f():\n    """doc"""\n    "query.model:v1"\n') == ["query.model:v1"]
    assert _stream_literals('x = "topology-controller"') == []
    assert _stream_literals('x = "no version here"') == []
    assert _stream_literals('x = "12:30"') == []
    assert _stream_literals("# release.requested:v1 in a comment\nx = 1") == []
