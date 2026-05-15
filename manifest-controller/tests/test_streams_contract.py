"""Verify the generated streams_contract module matches contract.yaml."""

from pathlib import Path

import yaml

import streams_contract


def test_streams_contract_matches_yaml():
    repo_root = Path(__file__).resolve().parents[2]
    contract_path = repo_root / "pkg" / "streams" / "contract.yaml"
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
