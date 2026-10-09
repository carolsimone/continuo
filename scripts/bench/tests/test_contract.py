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
    assert re.fullmatch(r"[a-z0-9._]+:v\d+", contract.stream_by_const("ReleasePromotedV2"))


VOCAB_FIXTURE = FIXTURE + """
vocabularies:
  - name: node_type
    const: NodeType
    values:
      - value: dbt-model
        const: DbtModel
      - value: dbt-test
        const: DbtTest
  - name: reject_reason
    const: RejectReason
    values:
      - value: other-test
        const: DbtTest
"""


def test_vocabulary_value_is_read_from_its_own_vocabulary(tmp_path):
    path = tmp_path / "contract.yaml"
    path.write_text(VOCAB_FIXTURE)
    assert contract.vocabulary_value("node_type", "DbtTest", path) == "dbt-test"
    assert contract.vocabulary_value("reject_reason", "DbtTest", path) == "other-test"
    with pytest.raises(KeyError):
        contract.vocabulary_value("node_type", "Missing", path)


def test_stream_lookup_ignores_vocabulary_entries(tmp_path):
    path = tmp_path / "contract.yaml"
    path.write_text(VOCAB_FIXTURE)
    with pytest.raises(KeyError):
        contract.stream_by_const("NodeType", path)


def test_real_contract_names_the_test_node_type():
    assert contract.vocabulary_value("node_type", "DbtTest") == "dbt-test"
