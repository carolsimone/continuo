"""The runtime manifest descriptor: what binds a partial parse to its release.

A descriptor travels beside the artifact it describes and is the only thing a
consumer reads before deciding whether it may hydrate that artifact. It is
validated where it is written, so a malformed descriptor never reaches S3.
"""
import re

# The layout of the artifact a descriptor describes: a dbt partial-parse msgpack
# file. A consumer that does not understand this identifier must reject the
# artifact rather than guess at its contents.
FORMAT = "dbt-partial-parse-msgpack-v1"

# Fields every descriptor carries. All are required and none may be empty: a
# half-filled descriptor is a contract violation, not a degraded mode.
REQUIRED_FIELDS: tuple[str, ...] = (
    "format",
    "service_name",
    "release_id",
    "image_tag",
    "artifact_uri",
    "sha256",
    "dbt_core_version",
    "adapter_type",
    "parse_context_sha256",
)

# Digests are compared verbatim across services, so an uppercase or truncated
# spelling of the same value would read as a different artifact.
_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")

_DIGEST_FIELDS: tuple[str, ...] = ("sha256", "parse_context_sha256")


def validate_descriptor(
    descriptor: dict,
    *,
    expected_service: str | None = None,
    expected_image_tag: str | None = None,
    expected_sha256: str | None = None,
) -> None:
    """Raise RuntimeError unless descriptor is complete and well-formed.

    The expectations are optional because the two sides of the contract know
    different things. A producer writes a descriptor from the compile it just
    ran and has nothing to compare it against, so it passes none of them. A
    consumer is pinned to one artifact and passes what it was pinned to, which
    is what stops it hydrating an artifact meant for another pool.
    """
    for field in REQUIRED_FIELDS:
        if field not in descriptor:
            raise RuntimeError(f"runtime manifest descriptor: {field} is missing")
        if not descriptor[field]:
            raise RuntimeError(f"runtime manifest descriptor: {field} is empty")

    if descriptor["format"] != FORMAT:
        raise RuntimeError(
            f"runtime manifest descriptor: unsupported format "
            f"{descriptor['format']!r}, expected {FORMAT!r}"
        )

    for field in _DIGEST_FIELDS:
        if not _SHA256_HEX.match(descriptor[field]):
            raise RuntimeError(
                f"runtime manifest descriptor: {field} must be lowercase SHA-256 hex"
            )

    if not descriptor["artifact_uri"].startswith("s3://"):
        raise RuntimeError("runtime manifest descriptor: artifact_uri must be s3://")

    expectations = (
        ("service_name", expected_service),
        ("image_tag", expected_image_tag),
        ("sha256", expected_sha256),
    )
    for field, expected in expectations:
        if expected is not None and descriptor[field] != expected:
            raise RuntimeError(
                f"runtime manifest descriptor: {field} is {descriptor[field]!r}, "
                f"expected {expected!r}"
            )
