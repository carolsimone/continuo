"""Guard: a module under domain/ imports the standard library and domain modules only.

The domain holds the release's vocabulary and rules (nodes, table references,
ingested nodes, failure kinds). Anything else it imports ties the rules to a
technology or reaches inward from a layer above it: the SQL parser, YAML,
gRPC and protobuf, the generated `proto` stubs, the install configuration, the
service layer, the adapters, Redis, S3, Prometheus, the process entry point and
the bench harness all sit outside it. The guard therefore allows a closed set
of top-level names (the standard library, `domain` and `__future__`) instead of
listing the names to refuse, so a new runtime dependency cannot slip into the
domain unseen.

The stream contract is outside that set too: test_domain_imports_no_streams_contract
guards it on its own and names the vocabulary module to use instead.
"""
import ast
import sys
from pathlib import Path

DOMAIN_DIR = Path(__file__).parent.parent / "domain"

ALLOWED_TOP_LEVEL_MODULES = sys.stdlib_module_names | {"domain", "__future__"}


def _disallowed_imports(source: str) -> list[tuple[int, str]]:
    """Return the (line, module) of every import in a source string whose
    top-level name is outside ALLOWED_TOP_LEVEL_MODULES, by the name it is
    imported under. A relative import (`from .model import X`) stays inside the
    domain package and is never one."""
    offenders: list[tuple[int, str]] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.level == 0:
            names = [node.module or ""]
        else:
            continue
        offenders.extend(
            (node.lineno, name) for name in names if name.split(".")[0] not in ALLOWED_TOP_LEVEL_MODULES
        )
    return offenders


def test_no_domain_module_imports_anything_but_the_standard_library_and_the_domain():
    inspected = 0
    offenders = []
    for path in sorted(DOMAIN_DIR.rglob("*.py")):
        inspected += 1
        for line, name in _disallowed_imports(path.read_text()):
            offenders.append(f"{path.relative_to(DOMAIN_DIR.parent)}:{line}: imports {name}")

    assert inspected, "guard inspected no domain modules — the discovery is broken"
    assert not offenders, (
        "domain code imports something outside the standard library and the domain — "
        "infrastructure and the outer layers stay out of it:\n" + "\n".join(offenders)
    )


def test_guard_catches_a_violation():
    """The detector flags every import shape it is meant to reject, with its line."""
    assert _disallowed_imports("import sqlglot") == [(1, "sqlglot")]
    assert _disallowed_imports("import sqlglot.expressions as exp") == [(1, "sqlglot.expressions")]
    assert _disallowed_imports("from sqlglot import exp") == [(1, "sqlglot")]
    assert _disallowed_imports("from config.config import _SYSTEM_RELATIONS") == [(1, "config.config")]
    assert _disallowed_imports("from service.topology_artifact import canonical_topology_json") == [
        (1, "service.topology_artifact")
    ]
    assert _disallowed_imports("from adapters.sources import ManifestSource") == [(1, "adapters.sources")]
    assert _disallowed_imports("import redis\nimport boto3\nfrom botocore.exceptions import ClientError") == [
        (1, "redis"), (2, "boto3"), (3, "botocore.exceptions"),
    ]
    assert _disallowed_imports("from prometheus_client import Counter") == [(1, "prometheus_client")]
    assert _disallowed_imports("import main") == [(1, "main")]
    assert _disallowed_imports("from bench import harness") == [(1, "bench")]
    assert _disallowed_imports("from streams_contract import ParseFailureKind") == [(1, "streams_contract")]
    # The service's other runtime dependencies are outside the domain too.
    assert _disallowed_imports("import grpc") == [(1, "grpc")]
    assert _disallowed_imports("import yaml") == [(1, "yaml")]
    assert _disallowed_imports("from google.protobuf import json_format") == [(1, "google.protobuf")]
    assert _disallowed_imports("from proto import release_pb2") == [(1, "proto")]
    # A function-level import is an import all the same.
    assert _disallowed_imports("def f():\n    import sqlglot\n") == [(2, "sqlglot")]
    # A name that merely starts with an allowed name is not that module.
    assert _disallowed_imports("import domain_helpers\nfrom regex import compile\nimport osx") == [
        (1, "domain_helpers"), (2, "regex"), (3, "osx"),
    ]


def test_guard_allows_the_standard_library_and_the_domain():
    assert _disallowed_imports("import re\nfrom dataclasses import dataclass\nfrom enum import StrEnum") == []
    assert _disallowed_imports("from __future__ import annotations") == []
    assert _disallowed_imports("import collections.abc\nfrom os.path import join") == []
    assert _disallowed_imports("from domain.model import ManifestNode") == []
    assert _disallowed_imports("import domain.exceptions") == []
    assert _disallowed_imports("from .model import ManifestNode") == []
    assert _disallowed_imports("from . import model") == []
    assert _disallowed_imports("from .config import something") == []
