import cli_json


def test_has_schedule_finds_listed_name():
    doc = {"schedules": [{"schedule_name": "daily"}, {"schedule_name": "bench-dag-500"}]}
    assert cli_json.has_schedule(doc, "bench-dag-500")
    assert not cli_json.has_schedule(doc, "bench-dag-2000")


def test_has_schedule_on_empty_output():
    assert not cli_json.has_schedule({}, "bench-dag-500")


def listing(**entry):
    return {"schedules": [{"schedule_name": "other", "last_run_id": "r-x", "has_active_run": True, "last_run_status": "running"},
                          {"schedule_name": "s", **entry}]}


def test_run_state_reports_the_listed_run_even_when_older():
    doc = listing(last_run_id="r-old", has_active_run=False, last_run_status="succeeded")
    assert cli_json.run_state(doc, "s") == ("r-old", False, "succeeded")


def test_run_state_reads_the_named_schedule_only():
    doc = listing(last_run_id="r-1", has_active_run=True, last_run_status="cancelling")
    assert cli_json.run_state(doc, "s") == ("r-1", True, "cancelling")
    assert cli_json.run_state(doc, "other") == ("r-x", True, "running")


def test_run_state_of_a_schedule_that_never_ran():
    assert cli_json.run_state(listing(has_active_run=False), "s") == ("", False, "")


def test_run_state_of_a_schedule_the_list_lacks_is_none():
    assert cli_json.run_state(listing(), "missing") is None
    assert cli_json.run_state({}, "s") is None


def test_schedule_names_are_sorted():
    assert cli_json.schedule_names({"schedules": [{"schedule_name": "b"}, {"schedule_name": "a"}]}) == ["a", "b"]


def test_same_schedules_ignores_order_and_other_fields():
    a = {"schedules": [{"schedule_name": "a", "has_active_run": True}, {"schedule_name": "b"}]}
    b = {"schedules": [{"schedule_name": "b"}, {"schedule_name": "a", "has_active_run": False}]}
    assert cli_json.same_schedules(a, b)
    assert not cli_json.same_schedules(a, {"schedules": [{"schedule_name": "a"}]})
