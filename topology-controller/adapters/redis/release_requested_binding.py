"""Decoding and validation of a release.requested:v1 stream message.

The consumer hands the handler the raw stream fields. This module turns them
into the value the use case consumes, or raises PermanentMessageError for a
message no redelivery can repair, which the consumer dead-letters at once.
"""

import json
from dataclasses import dataclass

from adapters.sources.s3_uri import parse_s3_uri
from domain.model import ManifestKind, ManifestRequest
from service.errors import PermanentMessageError
from streams_contract import RELEASE_REQUESTED_V1


@dataclass(frozen=True)
class ReleaseRequested:
    """A well-formed release.requested:v1 message: the release to parse, the
    artifact to fetch for each service, the one bucket they all live in, and
    the image tag each service's nodes run in. image_tags is None when the
    message carries none."""
    release_id: str
    requests: list[ManifestRequest]
    bucket: str
    image_tags: dict[str, str] | None = None


def _decode_field(fields: dict, name: str) -> str | None:
    raw = fields.get(name.encode()) or fields.get(name)
    if raw is None:
        return None
    return raw.decode() if isinstance(raw, bytes) else raw


def _manifest_request(entry: object) -> tuple[str, ManifestRequest]:
    """Validate one manifest_keys entry; return its bucket and the request.

    An entry must be an object carrying a non-empty string "service" and a
    string "s3_uri". Any other shape is a permanent malformed-payload error, so
    the service-mismatch and empty-manifest validation in the use case cannot
    be silently bypassed.
    """
    if not isinstance(entry, dict):
        raise PermanentMessageError(
            f"{RELEASE_REQUESTED_V1} manifest_keys entry must be an object, got {type(entry).__name__}"
        )
    svc = entry.get("service")
    if not svc:
        raise PermanentMessageError(
            f"{RELEASE_REQUESTED_V1} manifest_keys entry missing or empty 'service' field"
        )
    if not isinstance(svc, str):
        raise PermanentMessageError(
            f"{RELEASE_REQUESTED_V1} manifest_keys entry 'service' must be a string, got {type(svc).__name__}"
        )
    s3_uri = entry.get("s3_uri")
    if not isinstance(s3_uri, str):
        raise PermanentMessageError(
            f"{RELEASE_REQUESTED_V1} manifest_keys entry missing 's3_uri' or it is not a string"
        )
    try:
        bucket, key = parse_s3_uri(s3_uri)
    except ValueError as exc:
        raise PermanentMessageError(
            f"{RELEASE_REQUESTED_V1} manifest_keys entry has an invalid s3_uri: {exc}"
        ) from exc
    # parse_s3_uri appends a trailing slash to all non-empty paths; strip it
    # because object keys never end with "/" in S3.
    # Only an ABSENT kind defaults to dbt — that is the compatibility path for
    # producers that predate python support. Any value the producer actually
    # set is passed through verbatim, empty string included, so the use case
    # reports it as a permanent UnknownManifestKind failure the operator sees.
    # Defaulting a present-but-invalid value would parse a python contract with
    # the dbt parser and misreport it as MalformedManifest, sending the
    # operator to the wrong artifact.
    return bucket, ManifestRequest(
        service=svc,
        key=key.rstrip("/"),
        kind=entry.get("kind", ManifestKind.DBT),
    )


def _image_tags(raw: object) -> dict[str, str]:
    """Validate image_tags: an object mapping each service name to the image
    tag its nodes run in, both strings. Any other shape is a permanent
    malformed-payload error."""
    if not isinstance(raw, dict) or not all(
        isinstance(service, str) and isinstance(tag, str) for service, tag in raw.items()
    ):
        raise PermanentMessageError(
            f"{RELEASE_REQUESTED_V1} image_tags must be an object mapping service names to image tag strings"
        )
    return dict(raw)


def parse_release_requested(fields: dict, default_bucket: str) -> ReleaseRequested:
    """Decode the stream fields of a release.requested:v1 message.

    All entries must share a single bucket, derived from their URIs, so a
    misrouted multi-bucket payload is caught here. A message that lists no
    manifests names no bucket, and default_bucket stands in.
    """
    try:
        payload_raw = _decode_field(fields, "payload")
    except UnicodeDecodeError as exc:
        raise PermanentMessageError(f"{RELEASE_REQUESTED_V1} payload is not valid UTF-8: {exc}") from exc
    if not payload_raw:
        raise PermanentMessageError(f"{RELEASE_REQUESTED_V1} message missing payload")
    try:
        payload = json.loads(payload_raw)
    except json.JSONDecodeError as exc:
        raise PermanentMessageError(f"{RELEASE_REQUESTED_V1} payload not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise PermanentMessageError(
            f"{RELEASE_REQUESTED_V1} payload must be a JSON object, got {type(payload).__name__}",
        )
    release_id = payload.get("release_id")
    manifest_keys_raw = payload.get("manifest_keys")
    if not release_id or manifest_keys_raw is None:
        raise PermanentMessageError(
            f"{RELEASE_REQUESTED_V1} payload missing release_id or manifest_keys",
        )
    if not isinstance(release_id, str):
        raise PermanentMessageError(
            f"{RELEASE_REQUESTED_V1} release_id must be a string, got {type(release_id).__name__}",
        )
    if not isinstance(manifest_keys_raw, list):
        raise PermanentMessageError(
            f"{RELEASE_REQUESTED_V1} manifest_keys must be a list, got {type(manifest_keys_raw).__name__}",
        )
    buckets: list[str] = []
    requests: list[ManifestRequest] = []
    for entry in manifest_keys_raw:
        bucket, request = _manifest_request(entry)
        buckets.append(bucket)
        requests.append(request)
    if len(set(buckets)) > 1:
        raise PermanentMessageError(
            f"{RELEASE_REQUESTED_V1} manifest_keys span multiple buckets: {set(buckets)}"
        )
    image_tags_raw = payload.get("image_tags")
    return ReleaseRequested(
        release_id=release_id,
        requests=requests,
        bucket=buckets[0] if buckets else default_bucket,
        image_tags=None if image_tags_raw is None else _image_tags(image_tags_raw),
    )
