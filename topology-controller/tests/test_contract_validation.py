import copy
import importlib.metadata
import json
from pathlib import Path

import continuo_contracts
from continuo_contracts import dbt, validate, yaml_strict
from continuo_contracts.validate import Issue

from service import contract_validation, contracts_library
from service.contract_validation import ContractValidator
from tests.odcs_fixtures import demo_text

KPIS_PATH = "contracts/py_daily_kpis.odcs.yaml"
KPIS = yaml_strict.load(demo_text("py_daily_kpis"))


_DBT_VECTORS = Path(continuo_contracts.__file__).parent / "vectors" / "dbt_contracts.json"
_DBT_CASES = {case["name"]: case for case in json.loads(_DBT_VECTORS.read_text())}
DBT_DOC = _DBT_CASES["dormant_model"]["generated"]


def _dbt_doc(i):
    """A valid dbt contract of its own: the library's dormant model, renamed to `analytics.dormant_<i>`."""
    doc = copy.deepcopy(DBT_DOC)
    table = f"dormant_{i}"
    doc["id"] = f"analytics.{table}"
    doc["name"] = table
    doc["schema"][0]["name"] = table
    doc["schema"][0]["physicalName"] = table
    return doc


def _generated(case_name):
    case = _DBT_CASES[case_name]
    return dbt.generate(case["node"], case["tests"], manifest=case["manifest"]).document


def _kpis_with(**changes):
    doc = copy.deepcopy(KPIS)
    doc.update(changes)
    return doc


def _kpis_of_service(service):
    doc = copy.deepcopy(KPIS)
    doc["customProperties"].append({"vendor": "continuo", "property": "service", "value": service})
    return doc


def test_the_schema_check_is_cached_and_the_rules_are_not():
    v, hits = ContractValidator(), []
    v.check(KPIS, path=KPIS_PATH, service="service-py", on_cache=hits.append)
    assert v.check(KPIS, path="contracts/kpis.odcs.yaml", service="service-py", on_cache=hits.append)[0].reason == "file_name"
    assert hits == [False, True]


def test_a_valid_contract_is_checked_against_the_schema_once():
    v, hits = ContractValidator(), []
    first = v.check(KPIS, path=KPIS_PATH, service="service-py", on_cache=hits.append)
    second = v.check(KPIS, path=KPIS_PATH, service="service-py", on_cache=hits.append)
    assert (first, second, hits) == ([], [], [False, True])


def test_a_contract_that_fails_the_schema_gets_the_same_issues_from_the_cache():
    broken = {"apiVersion": "v3.2.0", "stray": 1}
    v, hits = ContractValidator(), []
    first = v.check(broken, path=KPIS_PATH, service="service-py", on_cache=hits.append)
    second = v.check(broken, path="contracts/other.odcs.yaml", service="other", on_cache=hits.append)
    assert first and first == second
    assert {issue.reason for issue in first} == {"odcs_schema"}
    assert hits == [False, True]


def test_the_rules_see_the_service_of_every_check():
    doc, v = _kpis_of_service("service-py"), ContractValidator()
    assert v.check(doc, path=KPIS_PATH, service="service-py", on_cache=lambda hit: None) == []
    (issue,) = v.check(doc, path=KPIS_PATH, service="other", on_cache=lambda hit: None)
    assert issue.reason == "service"


def test_the_cache_keys_on_what_the_document_says_not_on_the_object():
    v, hits = ContractValidator(), []
    v.check(KPIS, path=KPIS_PATH, service="service-py", on_cache=hits.append)
    v.check(copy.deepcopy(KPIS), path=KPIS_PATH, service="service-py", on_cache=hits.append)
    v.check(_kpis_with(name="other"), path=KPIS_PATH, service="service-py", on_cache=hits.append)
    assert hits == [False, True, False]


def test_a_caller_that_changes_the_returned_list_does_not_change_the_cache():
    broken = {"apiVersion": "v3.2.0", "stray": 1}
    v = ContractValidator()
    first = v.check(broken, path=KPIS_PATH, service="service-py", on_cache=lambda hit: None)
    expected = list(first)
    first.clear()
    assert v.check(broken, path=KPIS_PATH, service="service-py", on_cache=lambda hit: None) == expected


def test_the_cache_holds_a_bounded_number_of_documents_and_forgets_the_oldest():
    v, hits = ContractValidator(max_entries=2), []
    docs = [_kpis_with(name=f"n{i}") for i in range(3)]
    for doc in docs:
        v.check(doc, path=KPIS_PATH, service="service-py", on_cache=hits.append)
    v.check(docs[2], path=KPIS_PATH, service="service-py", on_cache=hits.append)
    v.check(docs[0], path=KPIS_PATH, service="service-py", on_cache=hits.append)
    assert hits == [False, False, False, True, False]


def test_an_issue_is_the_librarys_issue():
    v = ContractValidator()
    (issue,) = v.check(KPIS, path="contracts/kpis.odcs.yaml", service="service-py", on_cache=lambda hit: None)
    assert isinstance(issue, Issue)


def test_the_process_runs_on_the_installed_library_version():
    assert contracts_library.LIBRARY_VERSION == importlib.metadata.version("continuo-contracts")


def test_check_dbt_runs_the_schema_once_per_document(monkeypatch):
    schema_checks = []
    real_schema_issues = contract_validation.schema_issues
    monkeypatch.setattr(
        contract_validation, "schema_issues", lambda doc: schema_checks.append(1) or real_schema_issues(doc)
    )
    v, hits = ContractValidator(), []
    first = v.check_dbt(DBT_DOC, on_cache=hits.append)
    second = v.check_dbt(copy.deepcopy(DBT_DOC), on_cache=hits.append)
    v.check_dbt(_dbt_doc(1), on_cache=hits.append)
    assert (first, second) == ([], [])
    assert (hits, len(schema_checks)) == ([False, True, False], 2)


def test_check_dbt_reports_the_dbt_rules():
    case = _DBT_CASES["bad_criticality"]
    doc = _generated("bad_criticality")
    v, hits = ContractValidator(), []
    first = v.check_dbt(doc, on_cache=hits.append)
    second = v.check_dbt(doc, on_cache=hits.append)
    assert hits == [False, True]
    assert first == second == validate.validate_dbt_contract(doc)
    assert [{"path": issue.path, "reason": issue.reason} for issue in first] == case["issues"]
    assert first[0].path.endswith(".value")
    assert isinstance(first[0], Issue)


def test_check_dbt_gives_the_schema_issues_of_a_document_the_schema_refuses():
    broken = {"apiVersion": "v3.2.0", "stray": 1}
    v, hits = ContractValidator(), []
    first = v.check_dbt(broken, on_cache=hits.append)
    second = v.check_dbt(broken, on_cache=hits.append)
    assert first and first == second == validate.validate_dbt_contract(broken)
    assert {issue.reason for issue in first} == {"odcs_schema"}
    assert hits == [False, True]


def test_a_caller_that_changes_the_list_check_dbt_returns_does_not_change_the_cache():
    broken = {"apiVersion": "v3.2.0", "stray": 1}
    v = ContractValidator()
    first = v.check_dbt(broken, on_cache=lambda hit: None)
    expected = list(first)
    first.clear()
    assert v.check_dbt(broken, on_cache=lambda hit: None) == expected


def test_a_document_is_checked_against_the_schema_once_whichever_family_asks(monkeypatch):
    schema_checks = []
    real_schema_issues = contract_validation.schema_issues
    monkeypatch.setattr(
        contract_validation, "schema_issues", lambda doc: schema_checks.append(1) or real_schema_issues(doc)
    )
    v, hits = ContractValidator(), []
    v.check_dbt(DBT_DOC, on_cache=hits.append)
    v.check(DBT_DOC, path=KPIS_PATH, service="service-py", on_cache=hits.append)
    v.check(KPIS, path=KPIS_PATH, service="service-py", on_cache=hits.append)
    v.check_dbt(KPIS, on_cache=hits.append)
    assert (hits, len(schema_checks)) == ([False, True, False, True], 2)


def test_dbt_contracts_never_evict_a_python_contract():
    v = ContractValidator()
    hits = []
    v.check(KPIS, path=KPIS_PATH, service="service-py", on_cache=hits.append)
    for i in range(20_000):
        v.check_dbt(_dbt_doc(i), on_cache=lambda hit: None)
    v.check(KPIS, path=KPIS_PATH, service="service-py", on_cache=hits.append)
    assert hits == [False, True]
