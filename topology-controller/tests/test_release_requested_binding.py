"""The release.requested:v1 wire message is decoded and validated by the
adapter, not the composition root. Every malformed message raises the permanent
error the consumer dead-letters at once; a well-formed one parses into the
value the use case consumes."""

import json

import pytest

from adapters.redis.error_class import ErrorClass, classify
from adapters.redis.release_requested_binding import ReleaseRequested, parse_release_requested
from domain.model import ManifestKind, ManifestRequest
from service.errors import PermanentMessageError
from streams_contract import RELEASE_REQUESTED_V1

_DEFAULT_BUCKET = "default-bucket"


def _fields(payload: dict) -> dict:
    return {b"payload": json.dumps(payload).encode()}


def test_a_well_formed_payload_parses_into_a_release_requested():
    got = parse_release_requested(
        _fields({
            "release_id": "rel-77",
            "manifest_keys": [
                {"service": "service-1", "s3_uri": "s3://continuo/service-1/rel-77/manifest.json"},
                {"service": "service-2", "s3_uri": "s3://continuo/service-2/rel-77/manifest.json"},
            ],
        }),
        _DEFAULT_BUCKET,
    )

    assert got == ReleaseRequested(
        release_id="rel-77",
        requests=[
            ManifestRequest(service="service-1", key="service-1/rel-77/manifest.json"),
            ManifestRequest(service="service-2", key="service-2/rel-77/manifest.json"),
        ],
        bucket="continuo",
    )


def test_the_payload_field_is_read_under_a_str_key_too():
    got = parse_release_requested(
        {"payload": json.dumps({
            "release_id": "r",
            "manifest_keys": [{"service": "s", "s3_uri": "s3://b/s/r/manifest.json"}],
        })},
        _DEFAULT_BUCKET,
    )

    assert got.release_id == "r"
    assert got.bucket == "b"


def test_an_empty_manifest_list_falls_back_to_the_default_bucket():
    got = parse_release_requested(_fields({"release_id": "r", "manifest_keys": []}), _DEFAULT_BUCKET)

    assert got == ReleaseRequested(release_id="r", requests=[], bucket=_DEFAULT_BUCKET)


def test_the_object_key_loses_the_trailing_slash_a_prefix_uri_gains():
    got = parse_release_requested(
        _fields({
            "release_id": "r",
            "manifest_keys": [{"service": "s", "s3_uri": "s3://b/s/r/dir"}],
        }),
        _DEFAULT_BUCKET,
    )

    assert [r.key for r in got.requests] == ["s/r/dir"]


def test_kind_defaults_to_dbt_and_is_threaded_when_present():
    """release-controller does not send kind yet, so an entry without it must
    parse as dbt; an entry that carries it must reach the source verbatim."""
    got = parse_release_requested(
        _fields({
            "release_id": "rel-1",
            "manifest_keys": [
                {"service": "service-1", "s3_uri": "s3://continuo/service-1/rel-1/manifest.json"},
                {"service": "marketing-py", "kind": "python",
                 "s3_uri": "s3://continuo/marketing-py/rel-1/contract.yaml"},
            ],
        }),
        _DEFAULT_BUCKET,
    )

    assert [(r.service, r.kind) for r in got.requests] == [
        ("service-1", ManifestKind.DBT), ("marketing-py", ManifestKind.PYTHON),
    ]


def test_an_explicitly_empty_kind_is_not_defaulted_to_dbt():
    """Only an absent kind defaults. A producer that set the field to "" chose a
    value, and it is not a kind this build can parse — passing it through keeps
    the handler's UnknownManifestKind failure available. Defaulting it would
    parse a python contract with the dbt parser and misreport the release as
    MalformedManifest, pointing the operator at the wrong artifact."""
    got = parse_release_requested(
        _fields({
            "release_id": "rel-1",
            "manifest_keys": [
                {"service": "service-1", "kind": "",
                 "s3_uri": "s3://continuo/service-1/rel-1/manifest.json"},
            ],
        }),
        _DEFAULT_BUCKET,
    )

    assert [r.kind for r in got.requests] == [""]


def test_image_tags_are_parsed_when_present():
    """release-controller assembles one image tag per service when it activates
    the release; the handler joins them onto the nodes."""
    got = parse_release_requested(
        _fields({
            "release_id": "rel-1",
            "manifest_keys": [],
            "image_tags": {"service-1": "reg/service-1:abc", "service-2": "reg/service-2:def"},
        }),
        _DEFAULT_BUCKET,
    )

    assert got.image_tags == {"service-1": "reg/service-1:abc", "service-2": "reg/service-2:def"}


@pytest.mark.parametrize("payload", [
    {"release_id": "r", "manifest_keys": []},
    {"release_id": "r", "manifest_keys": [], "image_tags": None},
], ids=["absent", "null"])
def test_absent_or_null_image_tags_parse_as_none(payload):
    """No tags is not malformed: the handler turns it into a rejected release
    the operator sees, rather than a dead letter that leaves it parsing."""
    assert parse_release_requested(_fields(payload), _DEFAULT_BUCKET).image_tags is None


def test_an_empty_image_tags_object_is_kept_as_an_empty_mapping():
    got = parse_release_requested(
        _fields({"release_id": "r", "manifest_keys": [], "image_tags": {}}), _DEFAULT_BUCKET,
    )

    assert got.image_tags == {}


_MALFORMED_PAYLOADS = {
    "missing payload": {},
    "invalid json": {b"payload": b"not json {{{"},
    "missing release_id": {b"payload": json.dumps({"manifest_keys": []}).encode()},
    "missing manifest_keys": {b"payload": json.dumps({"release_id": "x"}).encode()},
    "entry without service": {b"payload": json.dumps({
        "release_id": "x",
        "manifest_keys": [{"s3_uri": "s3://continuo/s1/x/manifest.json"}],
    }).encode()},
    "entry with an invalid s3_uri": {b"payload": json.dumps({
        "release_id": "x",
        "manifest_keys": [{"service": "s1", "s3_uri": "http://continuo/s1/x/manifest.json"}],
    }).encode()},
    "entry without s3_uri": {b"payload": json.dumps({
        "release_id": "x",
        "manifest_keys": [{"service": "s1"}],
    }).encode()},
    "entry with a non-string s3_uri": {b"payload": json.dumps({
        "release_id": "x",
        "manifest_keys": [{"service": "s1", "s3_uri": 42}],
    }).encode()},
    "entry with a non-string service": {b"payload": json.dumps({
        "release_id": "x",
        "manifest_keys": [{"service": ["s1"], "s3_uri": "s3://continuo/s1/x/manifest.json"}],
    }).encode()},
    "entry that is not an object": {b"payload": json.dumps({
        "release_id": "x",
        "manifest_keys": ["s3://continuo/s1/x/manifest.json"],
    }).encode()},
    "payload that is a JSON array": {b"payload": b"[1, 2]"},
    "payload that is a JSON string": {b"payload": b'"release"'},
    "payload that is JSON null": {b"payload": b"null"},
    "manifest_keys that is not iterable": {b"payload": json.dumps({
        "release_id": "x", "manifest_keys": 5,
    }).encode()},
    "manifest_keys that is an object": {b"payload": json.dumps({
        "release_id": "x", "manifest_keys": {"service": "s1"},
    }).encode()},
    "release_id that is not a string": {b"payload": json.dumps({
        "release_id": 7, "manifest_keys": [],
    }).encode()},
    "payload that is not UTF-8": {b"payload": b"\xff\xfe{}"},
    "image_tags that is a list": {b"payload": json.dumps({
        "release_id": "x", "manifest_keys": [], "image_tags": ["reg/s1:abc"],
    }).encode()},
    "image_tags with a non-string tag": {b"payload": json.dumps({
        "release_id": "x", "manifest_keys": [], "image_tags": {"s1": 7},
    }).encode()},
    "entries spanning buckets": {b"payload": json.dumps({
        "release_id": "x",
        "manifest_keys": [
            {"service": "s1", "s3_uri": "s3://bucket-a/s1/x/manifest.json"},
            {"service": "s2", "s3_uri": "s3://bucket-b/s2/x/manifest.json"},
        ],
    }).encode()},
}


@pytest.mark.parametrize("fields", _MALFORMED_PAYLOADS.values(), ids=_MALFORMED_PAYLOADS.keys())
def test_malformed_release_requested_is_a_permanent_error(fields):
    """No redelivery can repair a malformed payload, so the parser raises the
    error the consumer dead-letters at once."""
    with pytest.raises(PermanentMessageError) as raised:
        parse_release_requested(fields, _DEFAULT_BUCKET)
    assert classify(raised.value) is ErrorClass.PERMANENT


@pytest.mark.parametrize("fields", _MALFORMED_PAYLOADS.values(), ids=_MALFORMED_PAYLOADS.keys())
def test_every_error_message_names_the_stream_through_the_contract_constant(fields):
    """The message lands in the dead letter's error field, so it must carry the
    stream name the contract declares now, not a spelling that can go stale."""
    with pytest.raises(PermanentMessageError) as raised:
        parse_release_requested(fields, _DEFAULT_BUCKET)
    assert str(raised.value).startswith(f"{RELEASE_REQUESTED_V1} ")


@pytest.mark.parametrize("fields, fragment", [
    ({}, "missing payload"),
    ({b"payload": b"not json {{{"}, "not valid JSON"),
    ({b"payload": json.dumps({"manifest_keys": []}).encode()}, "missing release_id or manifest_keys"),
    ({b"payload": json.dumps({"release_id": "x"}).encode()}, "missing release_id or manifest_keys"),
    ({b"payload": json.dumps({
        "release_id": "x",
        "manifest_keys": [{"s3_uri": "s3://continuo/s1/x/manifest.json"}],
    }).encode()}, "missing or empty 'service' field"),
    ({b"payload": b"\xff\xfe{}"}, "not valid UTF-8"),
])
def test_the_message_says_what_is_wrong_with_the_payload(fields, fragment):
    with pytest.raises(PermanentMessageError, match=fragment):
        parse_release_requested(fields, _DEFAULT_BUCKET)
