"""Verify the generated streams_contract module matches contract.yaml."""

from pathlib import Path

import yaml

import streams_contract


def _find_contract_yaml() -> Path:
    """Walk up from this file until pkg/streams/contract.yaml is found.

    Locally the test runs from the host and the file lives at the repo root.
    In CI the test runs inside the manifest-controller container, where the
    repo's pkg/ is mounted at /app/pkg. Either way, walking ancestors finds it.
    """
    start = Path(__file__).resolve()
    for parent in [start.parent, *start.parents]:
        candidate = parent / "pkg" / "streams" / "contract.yaml"
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"contract.yaml not found in any ancestor of {start}"
    )


def test_streams_contract_matches_yaml():
    contract_path = _find_contract_yaml()
    contract = yaml.safe_load(contract_path.read_text())

    expected = {}
    for stream in contract["streams"]:
        involves_mc = "manifest-controller" in (stream.get("producers") or [])
        consumers = stream.get("consumers") or []
        if not involves_mc:
            involves_mc = any(c["service"] == "manifest-controller" for c in consumers)
        if not involves_mc:
            continue
        expected[_screaming(stream["const"])] = stream["name"]
        for c in consumers:
            if c["service"] == "manifest-controller":
                expected[_screaming(c["const"])] = c["group"]

    for name, value in expected.items():
        assert hasattr(streams_contract, name), f"streams_contract missing {name}"
        assert getattr(streams_contract, name) == value, (
            f"{name}: expected {value!r}, got {getattr(streams_contract, name)!r}"
        )


def _screaming(s: str) -> str:
    out = []
    for i, ch in enumerate(s):
        if i > 0 and ch.isupper():
            out.append("_")
        out.append(ch.upper())
    return "".join(out)
