import json
from pathlib import Path

import pytest

from domain.exceptions import MalformedContractError
from domain.secret_ref import validate_secret_ref


def _cases() -> dict:
    start = Path(__file__).resolve()
    for parent in [start.parent, *start.parents]:
        candidate = parent / "pkg" / "domain" / "model" / "testdata" / "secret_ref_cases.json"
        if candidate.exists():
            return json.loads(candidate.read_text())
    raise FileNotFoundError("secret_ref_cases.json not found in any ancestor")


@pytest.mark.parametrize("ref", _cases()["valid"])
def test_shared_valid_names_pass(ref):
    assert validate_secret_ref(ref, "L") == ref


@pytest.mark.parametrize("ref", _cases()["invalid"])
def test_shared_invalid_names_fail(ref):
    with pytest.raises(MalformedContractError, match="secret_ref"):
        validate_secret_ref(ref, "L")


@pytest.mark.parametrize("ref", [None, 7, ["continuo-api-fx"]])
def test_non_string_fails(ref):
    with pytest.raises(MalformedContractError, match="secret_ref"):
        validate_secret_ref(ref, "L")
