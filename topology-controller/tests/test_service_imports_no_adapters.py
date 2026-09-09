"""Guard: no module under service/ imports an adapters.* module.

The application layer depends on ports it owns (service/ports.py), never on the
concrete adapters that implement them, so the dependency arrow runs
adapter -> port. A service module that reaches into adapters/* inverts that
arrow — the drift this guard exists to catch. The Go services are guarded by
TestServiceHandlersDoNotImportAdapters (pkg/streams/handler_imports_test.go);
this is the Python side's equivalent, and its absence is why the Python layer
was free to drift.
"""
import ast
from pathlib import Path

SERVICE_DIR = Path(__file__).parent.parent / "service"


def _adapters_imports(source: str) -> list[str]:
    """Return the adapters.* module names imported by a Python source string."""
    offenders: list[str] = []
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "adapters" or alias.name.startswith("adapters."):
                    offenders.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            # node.level > 0 is a relative import; service/ is flat, but a
            # future `from ..adapters import x` should still be caught.
            module = node.module or ""
            if module == "adapters" or module.startswith("adapters."):
                offenders.append(module)
    return offenders


def test_no_service_module_imports_adapters():
    offenders = []
    for path in sorted(SERVICE_DIR.rglob("*.py")):
        for name in _adapters_imports(path.read_text()):
            offenders.append(f"{path.relative_to(SERVICE_DIR.parent)}: imports {name}")

    assert not offenders, (
        "application layer imports concrete adapters — depend on a port in "
        "service/ports.py instead:\n" + "\n".join(offenders)
    )


def test_guard_catches_a_violation():
    """The detector actually flags the import shapes it is meant to reject."""
    assert _adapters_imports("from adapters.sources import ManifestSource") == [
        "adapters.sources"
    ]
    assert _adapters_imports("import adapters.redis.candidate_publisher") == [
        "adapters.redis.candidate_publisher"
    ]
    assert _adapters_imports("from adapters import x") == ["adapters"]
    # A same-layer or domain import is not a violation.
    assert _adapters_imports("from service.ports import CandidatePublisherPort") == []
    assert _adapters_imports("from domain.model import NodeRegistry") == []
    # A name that merely starts with the letters "adapters" is not the package.
    assert _adapters_imports("from adapters_helper import x") == []
