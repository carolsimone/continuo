"""What one invalid contract contributes to a release's rejection.

A dbt node's contract and a Python node's contract are checked by the same library, and
each that fails becomes one failed node whose detail lists every problem found and whose
problems are each counted by their reason. This is the one place that does both, so the
two parses count and word a failed contract alike.
"""
from collections.abc import Sequence

from continuo_contracts.validate import Issue

from service.release_timings import REASON_SEED_HEADER, FailureStage, ReleaseTimings


def count_and_describe(timings: ReleaseTimings, issues: Sequence[Issue], header_issues: Sequence[str] = ()) -> str:
    """Count what is wrong with a failed contract, and return its detail.

    Each of `issues` is counted as a failure of the `validate` stage by its own reason, and
    each of `header_issues` (the messages of a seed whose CSV header and declared columns
    differ, as the library words them) as `seed_header`. The detail lists the issues, each
    `<path>: <message>`, and then the header messages, all joined with `; `."""
    for issue in issues:
        timings.add_contract_failure(FailureStage.VALIDATE, issue.reason)
    for _ in header_issues:
        timings.add_contract_failure(FailureStage.VALIDATE, REASON_SEED_HEADER)
    return "; ".join([*(str(issue) for issue in issues), *header_issues])
