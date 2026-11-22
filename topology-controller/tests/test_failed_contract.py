"""A failed contract is worded and counted the same way whichever parse found it invalid."""
from continuo_contracts.validate import Issue

from service.failed_contract import count_and_describe
from service.release_timings import ReleaseTimings
from tests.fakes import FakeClock, ScriptedMemoryProbe


def _timings() -> ReleaseTimings:
    return ReleaseTimings("rel-1", FakeClock(), ScriptedMemoryProbe(0))


def test_the_detail_lists_each_issue_as_its_path_and_message():
    detail = count_and_describe(_timings(), [Issue("schema[0].name", "value", "is empty"), Issue("$", "kind_rule", "no")])

    assert detail == "schema[0].name: is empty; $: no"


def test_each_issue_is_counted_under_validate_by_its_own_reason():
    timings = _timings()

    count_and_describe(timings, [Issue("a", "value", "x"), Issue("b", "value", "y"), Issue("c", "kind_rule", "z")])

    assert timings.contract_failures == {("validate", "value"): 2, ("validate", "kind_rule"): 1}


def test_the_header_messages_follow_the_issues_and_are_counted_one_each():
    timings = _timings()

    detail = count_and_describe(
        timings, [Issue("a", "value", "x")], ["id is in the CSV header but not declared", "name is declared but not in the CSV header"])

    assert detail == "a: x; id is in the CSV header but not declared; name is declared but not in the CSV header"
    assert timings.contract_failures == {("validate", "value"): 1, ("validate", "seed_header"): 2}


def test_no_problem_is_an_empty_detail_and_counts_nothing():
    timings = _timings()

    assert count_and_describe(timings, []) == ""
    assert timings.contract_failures == {}
