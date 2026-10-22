import pytest

from domain.node_timeout import MAX_TIMEOUT_MINUTES, timeout_seconds


@pytest.mark.parametrize(
    ("minutes", "seconds"),
    [(1, 60), (120, 7200), (10080, 604800)],
)
def test_minutes_become_seconds(minutes, seconds):
    assert timeout_seconds(minutes) == seconds


def test_the_longest_timeout_is_one_week():
    assert MAX_TIMEOUT_MINUTES == 7 * 24 * 60


def test_an_absent_timeout_is_none():
    assert timeout_seconds(None) is None


@pytest.mark.parametrize("value", [0, 10081, -5, "2h", "120", 1.5, True, []])
def test_a_bad_value_is_refused(value):
    with pytest.raises(ValueError) as excinfo:
        timeout_seconds(value)
    assert str(excinfo.value) == (
        f"must be an integer from 1 to 10080 minutes, got {value!r}"
    )
