"""Domain types for the candidate-parse flow.

The candidate flow parses dbt manifests from a per-release S3 prefix on
behalf of release-controller. CandidateParseFailure is the structured
signal raised by the handler when parsing or dep resolution fails for a
reason that re-delivery cannot fix (malformed manifest, unresolvable
ref). The candidate handler converts the failure to a
manifest.loaded.candidate:v1 message with status=failed and ACKs.
"""

from __future__ import annotations


class CandidateParseFailure(Exception):
    """Raised internally by the candidate handler when parse/resolve fails
    in a way that should be reported back to release-controller as
    status=failed (not retried)."""

    def __init__(self, error_class: str, error_detail: str) -> None:
        self.error_class = error_class
        self.error_detail = error_detail
        super().__init__(f"{error_class}: {error_detail}")
