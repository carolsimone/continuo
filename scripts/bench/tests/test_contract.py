import re

import pytest

import contract

FIXTURE = """streams:
  - name: fixture.alpha:v0
    const: FixtureAlphaV0
    description: first
  - name: fixture.beta:v3
    const: FixtureBetaV3
"""


def test_resolves_constant_from_contract_text(tmp_path):
    path = tmp_path / "contract.yaml"
    path.write_text(FIXTURE)
    assert contract.stream_by_const("FixtureBetaV3", path) == "fixture.beta:v3"


def test_unknown_constant_raises(tmp_path):
    path = tmp_path / "contract.yaml"
    path.write_text(FIXTURE)
    with pytest.raises(KeyError):
        contract.stream_by_const("Missing", path)


def test_real_contract_resolves_the_promotion_stream():
    assert re.fullmatch(r"[a-z0-9._]+:v\d+", contract.stream_by_const("ReleasePromotedV1"))
