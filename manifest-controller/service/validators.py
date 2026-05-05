"""Publish-boundary validation for manifest.loaded:v1 batches.

A batch is rejected entirely if any node has an empty image_tag. Rejection
surfaces as ManifestValidationError; the publish-boundary handler catches
it, logs at ERROR with structured fields, and skips the publish. The
triggering update.graph:v1 message is ACKed regardless — redelivery
cannot help because S3 hasn't changed.

Mirrors the orchestrator's validateTopologyNodes (Go) so error shapes
stay symmetric across services.
"""

from __future__ import annotations

from domain.model import ManifestNode

# MAX_OFFENDERS_IN_ERROR caps the offender list shown in the error message.
# Mirrors maxOffendersInError in orchestrator/service/handlers/ingest_topology.go
# — a log-readability bound, not a correctness constraint.
MAX_OFFENDERS_IN_ERROR = 10


class ManifestValidationError(Exception):
    """Raised when a manifest batch fails publish-boundary validation."""

    def __init__(self, offenders: list[str], total_missing: int) -> None:
        # offenders is the truncated list (capped at MAX_OFFENDERS_IN_ERROR);
        # total_missing is the full count, used by the caller for metrics.
        self.offenders = offenders
        self.total_missing = total_missing
        suffix = ""
        if total_missing > MAX_OFFENDERS_IN_ERROR:
            suffix = f", ...and {total_missing - MAX_OFFENDERS_IN_ERROR} more"
        super().__init__(
            f"image_tag empty for {total_missing} node(s): "
            f"{', '.join(offenders)}{suffix}"
        )


def validate_manifest_batch(nodes: list[ManifestNode]) -> None:
    """Return None if every node has a non-empty image_tag.

    Raise ManifestValidationError with a sorted, capped offender list if any
    node has an empty image_tag. Empty input returns None — an empty batch
    is a degenerate but valid snapshot.
    """
    bad = [
        f"{n.service_name}/{n.schema_name}/{n.table_name}"
        for n in nodes
        if not n.image_tag
    ]
    if not bad:
        return None
    bad.sort()
    offenders = bad[:MAX_OFFENDERS_IN_ERROR]
    raise ManifestValidationError(offenders=offenders, total_missing=len(bad))
