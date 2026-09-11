"""Verify the generated domain vocabulary module matches contract.yaml.

The `vocabularies:` block of pkg/streams/contract.yaml is generated into
domain/contract_vocabulary.py (Python) and pkg/domain/model/vocabulary.gen.go
(Go). Both halves are pinned to the YAML so a regenerated contract that drops,
reorders, or re-flags a value fails here rather than in production.
"""

from pathlib import Path

import yaml

from domain import contract_vocabulary


def _find_contract_yaml() -> Path:
    """Walk up from this file until pkg/streams/contract.yaml is found.

    Locally the test runs from the host and the file lives at the repo root.
    In CI the test runs inside the topology-controller container, where the
    repo's pkg/ is mounted at /app/pkg. Either way, walking ancestors finds it.
    """
    start = Path(__file__).resolve()
    for parent in [start.parent, *start.parents]:
        candidate = parent / "pkg" / "streams" / "contract.yaml"
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"contract.yaml not found in any ancestor of {start}")


def _screaming(s: str) -> str:
    out = []
    for i, ch in enumerate(s):
        if i > 0 and ch.isupper():
            out.append("_")
        out.append(ch.upper())
    return "".join(out)


def _vocabularies() -> list[dict]:
    contract = yaml.safe_load(_find_contract_yaml().read_text())
    vocabularies = contract.get("vocabularies") or []
    assert vocabularies, "contract.yaml declares no vocabularies"
    return vocabularies


def test_every_vocabulary_is_generated_in_declaration_order():
    for vocab in _vocabularies():
        enum = getattr(contract_vocabulary, vocab["const"], None)
        assert enum is not None, f"contract_vocabulary missing {vocab['const']}"
        assert [m.value for m in enum] == [v["value"] for v in vocab["values"]]
        assert [m.name for m in enum] == [v["value"].upper() for v in vocab["values"]]


def test_healable_frozensets_match_the_contract_flags():
    for vocab in _vocabularies():
        enum = getattr(contract_vocabulary, vocab["const"])
        healable = getattr(contract_vocabulary, _screaming(vocab["const"]) + "_HEALABLE")
        expected = {enum(v["value"]) for v in vocab["values"] if v.get("healable")}
        assert healable == expected


def test_parse_failure_kind_healable_set():
    """The two SQL kinds are fixable by a source change; the other two are not."""
    kind = contract_vocabulary.ParseFailureKind
    assert contract_vocabulary.PARSE_FAILURE_KIND_HEALABLE == {
        kind.INVALID_SQL,
        kind.UNQUALIFIED_REFERENCE,
    }
