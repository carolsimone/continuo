"""Validation of the runtime manifest descriptor that travels beside a manifest.

A descriptor is the only thing read before a pre-built dbt partial parse is
accepted into a release, so every field is checked here: a descriptor that is
merely plausible would let a worker hydrate an artifact belonging to another
service, another release, or another dbt version.
"""
import re

from domain.exceptions import MalformedRuntimeManifestError
from domain.model import RuntimeManifestRef

# The layout of the artifact a descriptor describes: a dbt partial-parse msgpack
# file. A descriptor stamped with anything else describes contents this service
# cannot reason about, so it is rejected rather than guessed at.
FORMAT_V1 = "dbt-partial-parse-msgpack-v1"

# The only warehouse adapter the platform runs; an artifact parsed against a
# different adapter is not interchangeable with one parsed against postgres.
ADAPTER_TYPE = "postgres"

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

_DIGEST_FIELDS: tuple[str, ...] = ("sha256", "parse_context_sha256")

# Digests are compared verbatim across services, so an uppercase or truncated
# spelling of the same value would read as a different artifact.
_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")


def parse_descriptor(
    descriptor: dict,
    expected_service: str,
    expected_artifact_uri: str,
) -> RuntimeManifestRef:
    """Validate a descriptor and narrow it to the ref that travels on events.

    Raises MalformedRuntimeManifestError if the descriptor is not a v1 descriptor
    for expected_service describing the artifact at expected_artifact_uri.
    """
    if not isinstance(descriptor, dict):
        raise MalformedRuntimeManifestError(
            f"runtime manifest descriptor: expected a JSON object, got {type(descriptor).__name__}"
        )

    for field in REQUIRED_FIELDS:
        if field not in descriptor:
            raise MalformedRuntimeManifestError(
                f"runtime manifest descriptor: {field} is missing"
            )
        if not isinstance(descriptor[field], str):
            raise MalformedRuntimeManifestError(
                f"runtime manifest descriptor: {field} must be a string"
            )
        if not descriptor[field]:
            raise MalformedRuntimeManifestError(
                f"runtime manifest descriptor: {field} is empty"
            )

    if descriptor["format"] != FORMAT_V1:
        raise MalformedRuntimeManifestError(
            f"runtime manifest descriptor: unsupported format {descriptor['format']!r},"
            f" expected {FORMAT_V1!r}"
        )

    if descriptor["service_name"] != expected_service:
        raise MalformedRuntimeManifestError(
            f"runtime manifest descriptor: service_name {descriptor['service_name']!r}"
            f" does not match declared service {expected_service!r}"
        )

    if descriptor["artifact_uri"] != expected_artifact_uri:
        raise MalformedRuntimeManifestError(
            f"runtime manifest descriptor: artifact_uri {descriptor['artifact_uri']!r}"
            f" is not the sibling artifact {expected_artifact_uri!r}"
        )

    for field in _DIGEST_FIELDS:
        if not _SHA256_HEX.fullmatch(descriptor[field]):
            raise MalformedRuntimeManifestError(
                f"runtime manifest descriptor: {field} must be lowercase SHA-256 hex"
            )

    if descriptor["adapter_type"] != ADAPTER_TYPE:
        raise MalformedRuntimeManifestError(
            f"runtime manifest descriptor: adapter_type {descriptor['adapter_type']!r}"
            f" is not {ADAPTER_TYPE!r}"
        )

    return RuntimeManifestRef(
        uri=descriptor["artifact_uri"],
        sha256=descriptor["sha256"],
        dbt_version=descriptor["dbt_core_version"],
        parse_context_sha256=descriptor["parse_context_sha256"],
    )
