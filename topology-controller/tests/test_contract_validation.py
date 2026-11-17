import copy
import importlib.metadata

from continuo_contracts import yaml_strict
from continuo_contracts.validate import Issue

from service import contracts_library
from service.contract_validation import ContractValidator
from tests.odcs_fixtures import demo_text

KPIS_PATH = "contracts/py_daily_kpis.odcs.yaml"
KPIS = yaml_strict.load(demo_text("py_daily_kpis"))


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


def test_the_process_holds_one_validator_and_the_library_version_it_runs_on():
    assert contracts_library.LIBRARY_VERSION == importlib.metadata.version("continuo-contracts")
    assert isinstance(contracts_library.VALIDATOR, ContractValidator)
