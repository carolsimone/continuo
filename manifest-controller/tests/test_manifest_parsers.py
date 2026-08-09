import json

from domain.exceptions import MalformedContractError
from domain.model import ManifestKind
from service.manifest_parsers import parser_for
from service.parser import parse_manifest
from service.python_contract_parser import parse_python_contract


def test_each_kind_resolves_to_its_own_parser_and_failure_vocabulary():
    dbt = parser_for(ManifestKind.DBT)
    python = parser_for(ManifestKind.PYTHON)

    assert dbt.parse is parse_manifest
    assert dbt.error_class == "MalformedManifest"
    assert json.JSONDecodeError in dbt.permanent_errors

    assert python.parse is parse_python_contract
    assert python.error_class == "MalformedContract"
    assert python.permanent_errors == (MalformedContractError,)


def test_a_plain_string_resolves_the_same_as_the_enum_member():
    """main.py passes the raw wire value through, so lookups must work on str."""
    assert parser_for("python") is parser_for(ManifestKind.PYTHON)


def test_an_unrecognized_kind_resolves_to_nothing():
    assert parser_for("spark") is None
    assert parser_for("") is None
