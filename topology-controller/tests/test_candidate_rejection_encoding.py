"""A rejection whose text UTF-8 cannot encode is published as an invalid artifact that names the release
and shows the first such text escaped. The event is encoded as UTF-8, and a lone surrogate (a JSON
escape in the release request or the manifest) makes that encode raise: the message would be retried,
then dead-lettered, and the release would get no answer."""
import json
from unittest.mock import MagicMock

import pytest

from adapters.redis.candidate_publisher import CandidateManifestPublisher
from domain.contract_vocabulary import NodeType, ParseFailureKind
from domain.model import FailedNode, ManifestFile, ParsedArtifact
from service import candidate_manifest_handler
from service.contracts_library import LIBRARY_VERSION
from service.manifest_parsers import ManifestParser
from streams_contract import RELEASE_PARSED_V1
from tests.fakes import build_handler, make_uploader, source_of

_LONE_SURROGATE = "\ud800"


def _publishing(redis):
    """The real publisher over a recording redis client."""
    return CandidateManifestPublisher(
        redis, RELEASE_PARSED_V1, producer="topology-controller", continuo_contracts=LIBRARY_VERSION)


def _published(redis) -> dict:
    """The one entry the release published, as redis-py sends it: every field encoded as UTF-8, which
    raises for text that holds a lone surrogate. Returns its payload."""
    redis.xadd.assert_called_once()
    _, fields = redis.xadd.call_args.args
    for value in fields.values():
        value.encode("utf-8")
    return json.loads(fields["payload"])


def _failed(**over) -> FailedNode:
    fields = {"node_id": "analytics.orders", "kind": ParseFailureKind.INVALID_CONTRACT, "project": "service-1",
              "file_path": "models/orders.sql", "node_type": NodeType.DBT_MODEL, "detail": "a problem"}
    return FailedNode(**(fields | over))


def _release_whose_parse_finds(tmp_path, monkeypatch, *failed: FailedNode):
    """A release of one dbt manifest whose parse reports `failed`, published through the real publisher.
    Returns the payload that reached redis."""
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")

    def parse(mf, *, skipped=None, progress, timings, validator):
        return ParsedArtifact(nodes=[], shared_code={}, failed=tuple(failed))

    monkeypatch.setattr(
        candidate_manifest_handler, "parser_for",
        lambda kind: ManifestParser(parse=parse, permanent_errors=(), empty_detail="empty"))
    redis = MagicMock()
    source = source_of(ManifestFile(path=str(manifest), image_tag="", declared_project="service-1"))

    build_handler(source, _publishing(redis), make_uploader()).handle(release_id="rel-1")

    return _published(redis)


@pytest.mark.parametrize("field, text", [
    ("node id", "analytics.orders" + _LONE_SURROGATE),
    ("project", "project" + _LONE_SURROGATE),
    ("file path", "models/orders" + _LONE_SURROGATE + ".sql"),
    ("detail", "the column " + _LONE_SURROGATE + " is missing"),
])
def test_a_failed_node_holding_unencodable_text_is_published_as_an_invalid_artifact(
        tmp_path, monkeypatch, field, text):
    name = {"node id": "node_id", "project": "project", "file path": "file_path", "detail": "detail"}[field]
    unencodable = {"node_id": "analytics.customers"} | {name: text}

    payload = _release_whose_parse_finds(tmp_path, monkeypatch, _failed(), _failed(**unencodable))

    assert payload["status"] == "failed"
    assert payload["failure_kind"] == "invalid_artifact"
    assert payload["failed_nodes"] == []
    assert payload["detail"].startswith("release rel-1: the failure holds text UTF-8 cannot encode (")
    assert r"\ud800" in payload["detail"], "the text is shown escaped"


def test_the_text_that_is_shown_is_the_first_one_that_cannot_be_encoded_and_is_written_escaped(tmp_path, monkeypatch):
    payload = _release_whose_parse_finds(
        tmp_path, monkeypatch, _failed(file_path="models/orders" + _LONE_SURROGATE + ".sql"))

    assert payload["detail"] == r"release rel-1: the failure holds text UTF-8 cannot encode (models/orders\ud800.sql)"


@pytest.mark.parametrize("kind", [ParseFailureKind.INVALID_SQL, ParseFailureKind.INVALID_CONTRACT])
def test_a_rejection_that_could_be_published_keeps_its_failed_nodes(tmp_path, monkeypatch, kind):
    """Text that UTF-8 can encode, however far from ASCII, is published as it is."""
    payload = _release_whose_parse_finds(
        tmp_path, monkeypatch,
        _failed(kind=kind, node_id="analytics.commandes_é", project="service-é", file_path="models/日本語.sql",
                detail="colonne manquante 🚫"))

    assert payload["failure_kind"] == kind.value
    assert [(n["node_id"], n["project"], n["file_path"], n["detail"]) for n in payload["failed_nodes"]] == [
        ("analytics.commandes_é", "service-é", "models/日本語.sql", "colonne manquante 🚫")]


def test_a_declared_project_holding_unencodable_text_gets_a_rejection_not_a_retry(tmp_path):
    """The project name comes from the release request. A manifest with no node is rejected with a detail
    that names the project, which here cannot be encoded."""
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"nodes": {}}))
    redis = MagicMock()
    source = source_of(ManifestFile(
        path=str(manifest), image_tag="", declared_project="project" + _LONE_SURROGATE))

    build_handler(source, _publishing(redis), make_uploader()).handle(release_id="rel-1")

    payload = _published(redis)
    assert (payload["status"], payload["failure_kind"], payload["failed_nodes"]) == ("failed", "invalid_artifact", [])
    assert payload["detail"] == (
        r"release rel-1: the failure holds text UTF-8 cannot encode (project\ud800: manifest contains no model/seed nodes)")


def test_the_quoted_part_of_an_unencodable_detail_is_capped(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"nodes": {}}))
    redis = MagicMock()
    source = source_of(ManifestFile(
        path=str(manifest), image_tag="", declared_project=_LONE_SURROGATE + "s" * 5000))

    build_handler(source, _publishing(redis), make_uploader()).handle(release_id="rel-1")

    detail = _published(redis)["detail"]
    assert detail.startswith("release rel-1: the failure holds text UTF-8 cannot encode (")
    assert len(detail) < 600, "what is shown of the text is cut"

