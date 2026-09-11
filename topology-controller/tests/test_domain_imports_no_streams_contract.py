"""Guard: no module under domain/ imports streams_contract.

streams_contract holds Redis stream and consumer-group names — transport, not
domain vocabulary. The closed value sets domain code does name (the contract
`vocabularies:` block) are generated into domain/contract_vocabulary.py, so a
domain module spells ParseFailureKind without depending on the messaging
contract. The Go counterpart is TestDomainPackagesDoNotImportStreams in
pkg/streams/domain_imports_test.go.
"""
import ast
from pathlib import Path

DOMAIN_DIR = Path(__file__).parent.parent / "domain"


def _streams_contract_imports(source: str) -> list[str]:
    """Return the streams_contract module names imported by a source string."""
    offenders: list[str] = []
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "streams_contract" or alias.name.startswith("streams_contract."):
                    offenders.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module == "streams_contract" or module.startswith("streams_contract."):
                offenders.append(module)
    return offenders


def test_no_domain_module_imports_streams_contract():
    inspected = 0
    offenders = []
    for path in sorted(DOMAIN_DIR.rglob("*.py")):
        inspected += 1
        for name in _streams_contract_imports(path.read_text()):
            offenders.append(f"{path.relative_to(DOMAIN_DIR.parent)}: imports {name}")

    assert inspected, "guard inspected no domain modules — the discovery is broken"
    assert not offenders, (
        "domain code imports the stream contract — name a value from "
        "domain/contract_vocabulary.py instead:\n" + "\n".join(offenders)
    )


def test_guard_catches_a_violation():
    """The detector actually flags the import shapes it is meant to reject."""
    assert _streams_contract_imports("from streams_contract import ParseFailureKind") == [
        "streams_contract"
    ]
    assert _streams_contract_imports("import streams_contract") == ["streams_contract"]
    # The domain vocabulary module and a same-layer import are not violations.
    assert _streams_contract_imports("from domain.contract_vocabulary import ParseFailureKind") == []
    assert _streams_contract_imports("from domain.model import NodeRegistry") == []
    # A name that merely starts with the same letters is not the module.
    assert _streams_contract_imports("from streams_contract_helper import x") == []
