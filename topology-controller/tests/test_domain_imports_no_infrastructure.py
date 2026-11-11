"""Guard: no module under domain/ imports infrastructure or an outer layer.

The domain holds the release's vocabulary and rules (nodes, table references,
ingested nodes, failure kinds) and depends on the standard library and on other
domain modules only. The SQL parser, the install configuration, the service
layer, the adapters, Redis, S3, Prometheus, the process entry point and the
bench harness all sit outside it, so a domain module that names one of them
ties the rules to a technology or reaches inward from a layer above it.

The stream contract is on the list too: test_domain_imports_no_streams_contract
guards it on its own and names the vocabulary module to use instead.
"""
import ast
from pathlib import Path

DOMAIN_DIR = Path(__file__).parent.parent / "domain"

FORBIDDEN_TOP_LEVEL_MODULES = frozenset({
    "adapters",
    "bench",
    "boto3",
    "botocore",
    "config",
    "main",
    "prometheus_client",
    "redis",
    "service",
    "sqlglot",
    "streams_contract",
})


def _forbidden_imports(source: str) -> list[str]:
    """Return the forbidden modules a source string imports, by the name it
    imports them under. A relative import (`from .model import X`) stays inside
    the domain package and is never one."""
    offenders: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.level == 0:
            names = [node.module or ""]
        else:
            continue
        offenders.extend(name for name in names if name.split(".")[0] in FORBIDDEN_TOP_LEVEL_MODULES)
    return offenders


def test_no_domain_module_imports_infrastructure_or_an_outer_layer():
    inspected = 0
    offenders = []
    for path in sorted(DOMAIN_DIR.rglob("*.py")):
        inspected += 1
        for name in _forbidden_imports(path.read_text()):
            offenders.append(f"{path.relative_to(DOMAIN_DIR.parent)}: imports {name}")

    assert inspected, "guard inspected no domain modules — the discovery is broken"
    assert not offenders, (
        "domain code imports infrastructure or an outer layer — keep it to the "
        "standard library and other domain modules:\n" + "\n".join(offenders)
    )


def test_guard_catches_a_violation():
    """The detector flags every import shape it is meant to reject."""
    assert _forbidden_imports("import sqlglot") == ["sqlglot"]
    assert _forbidden_imports("import sqlglot.expressions as exp") == ["sqlglot.expressions"]
    assert _forbidden_imports("from sqlglot import exp") == ["sqlglot"]
    assert _forbidden_imports("from config.config import _SYSTEM_RELATIONS") == ["config.config"]
    assert _forbidden_imports("from service.topology_artifact import canonical_topology_json") == [
        "service.topology_artifact"
    ]
    assert _forbidden_imports("from adapters.sources import ManifestSource") == ["adapters.sources"]
    assert _forbidden_imports("import redis\nimport boto3\nfrom botocore.exceptions import ClientError") == [
        "redis", "boto3", "botocore.exceptions",
    ]
    assert _forbidden_imports("from prometheus_client import Counter") == ["prometheus_client"]
    assert _forbidden_imports("import main") == ["main"]
    assert _forbidden_imports("from bench import harness") == ["bench"]
    assert _forbidden_imports("from streams_contract import ParseFailureKind") == ["streams_contract"]
    # A function-level import is an import all the same.
    assert _forbidden_imports("def f():\n    import sqlglot\n") == ["sqlglot"]


def test_guard_allows_the_standard_library_and_the_domain():
    assert _forbidden_imports("import re\nfrom dataclasses import dataclass\nfrom enum import StrEnum") == []
    assert _forbidden_imports("from domain.model import ManifestNode") == []
    assert _forbidden_imports("import domain.exceptions") == []
    assert _forbidden_imports("from .model import ManifestNode") == []
    assert _forbidden_imports("from . import model") == []
    assert _forbidden_imports("from .config import something") == []
    # A name that merely starts with the same letters is not the module.
    assert _forbidden_imports("import services_helper\nfrom mainframe import x\nimport redis_om") == []
