"""A node's task timeout, declared by its domain team in minutes.

A node's contract declares it as `timeoutMinutes`: a Python node's in its ODCS
file, a dbt node's from its `meta.timeout_minutes`. The topology carries the
value in seconds, so the one conversion and its bounds live here, for both.
"""

MAX_TIMEOUT_MINUTES = 10_080  # one week


def timeout_seconds(value: object) -> int | None:
    """The timeout in seconds for a declared `timeout_minutes`, None when
    nothing is declared.

    Only a whole number of minutes from 1 to MAX_TIMEOUT_MINUTES is valid. A
    bool is refused although it is an int subclass, and so are strings and
    floats: a quoted or fractional value is a typo, not a duration to guess at.
    """
    if value is None:
        return None
    if type(value) is int and 1 <= value <= MAX_TIMEOUT_MINUTES:
        return value * 60
    raise ValueError(
        f"must be an integer from 1 to {MAX_TIMEOUT_MINUTES} minutes, got {value!r}"
    )
