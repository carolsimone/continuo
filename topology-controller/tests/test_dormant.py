"""The dormant rule: a node a release builds that has no schedule.

fixtures/dormant_cases.json is a copy of the fixture pkg/domain/model's tests
read, so the Go and the Python rule answer the same questions.
"""
import json
from pathlib import Path

import pytest

from domain.contract_vocabulary import NodeType
from domain.dormant import is_dormant

_CASES = json.loads((Path(__file__).parent / "fixtures" / "dormant_cases.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("case", _CASES, ids=lambda c: f"{c['node_type']}-{c['schedule'] or 'untagged'}")
def test_is_dormant_follows_the_shared_fixture(case):
    # A StrEnum member hashes and compares as its value, so the fixture's plain
    # string, undeclared ones included, asks the question a NodeType would.
    assert is_dormant(case["node_type"], case["schedule"]) is case["dormant"]


def test_the_shared_fixture_has_a_case_for_every_declared_node_type():
    covered = {case["node_type"] for case in _CASES}
    assert {nt.value for nt in NodeType} <= covered
