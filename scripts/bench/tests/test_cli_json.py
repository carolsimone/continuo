import cli_json


def test_has_schedule_finds_listed_name():
    doc = {"schedules": [{"schedule_name": "daily"}, {"schedule_name": "bench-dag-500"}]}
    assert cli_json.has_schedule(doc, "bench-dag-500")
    assert not cli_json.has_schedule(doc, "bench-dag-2000")


def test_has_schedule_on_empty_output():
    assert not cli_json.has_schedule({}, "bench-dag-500")


def test_run_state_reports_the_listed_run_even_when_older():
    doc = {"schedule_name": "s", "run_id": "r-old", "is_running": False, "last_run_status": "succeeded"}
    assert cli_json.run_state(doc) == ("r-old", False, "succeeded")


def test_schedule_names_are_sorted():
    assert cli_json.schedule_names({"schedules": [{"schedule_name": "b"}, {"schedule_name": "a"}]}) == ["a", "b"]


def test_same_schedules_ignores_order_and_other_fields():
    a = {"schedules": [{"schedule_name": "a", "is_running": True}, {"schedule_name": "b"}]}
    b = {"schedules": [{"schedule_name": "b"}, {"schedule_name": "a", "is_running": False}]}
    assert cli_json.same_schedules(a, b)
    assert not cli_json.same_schedules(a, {"schedules": [{"schedule_name": "a"}]})
