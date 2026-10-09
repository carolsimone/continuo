"""The topology artifact topology-controller writes once per release.

Its canonical JSON is pinned byte for byte to pkg/topologyartifact by
fixtures/topology_v1.json, a copy of the fixture the Go package reads, so the
two writers produce the same document. The candidate objects the builders write
are pinned to the keys release-controller derives by
fixtures/candidate_object_keys_v1.json.
"""
import gzip
import hashlib
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from adapters.candidate_spec_uploader import CandidateSpecUploader
from adapters.candidate_sql_uploader import CandidateSqlUploader
from adapters.topology_artifact_writer import TopologyArtifactWriter
from domain.contract_vocabulary import NODE_TYPE_RUNTIME
from domain.model import ManifestNode, NodeType, Runtime, TopologyArtifactRef
from service.candidate_artifacts import DbtSqlArtifactBuilder, PythonSpecArtifactBuilder, RewriteContext
from service.topology_artifact import canonical_topology_json, topology_artifact_key

_FIXTURES = Path(__file__).parent / "fixtures"


def _node(unique_id, **overrides):
    node = {
        "unique_id": unique_id,
        "schema_name": unique_id.split(".")[0],
        "table_name": unique_id.split(".")[-1],
        "resolved_relation_id": unique_id,
        "service_name": "service-1",
        "node_type": "dbt-model",
        "test_count": 0,
        "content_hash": "sha256:aa",
        "image_tag": "reg/service-1:abc",
        "original_file_path": f"models/{unique_id}.sql",
        "upstream_unique_ids": [],
        "schedule": "daily",
    }
    node.update(overrides)
    return node


def _decode(raw: bytes) -> dict:
    return json.loads(raw.decode("utf-8"))


# --- canonical JSON -----------------------------------------------------------

def test_canonical_json_matches_the_shared_fixture_byte_for_byte():
    fixture = json.loads((_FIXTURES / "topology_v1.json").read_text(encoding="utf-8"))
    document = fixture["input"]

    got = canonical_topology_json(
        tenant_id=document["tenant_id"],
        release_id=document["release_id"],
        nodes=document["nodes"],
    )

    assert got == fixture["canonical"].encode("utf-8")


def test_nodes_are_sorted_by_unique_id_and_keys_are_sorted_and_compact():
    raw = canonical_topology_json(
        tenant_id="default", release_id="rel-1",
        nodes=[_node("b.second"), _node("a.first")],
    )

    text = raw.decode("utf-8")
    assert text.startswith('{"nodes":[{"content_hash":')
    assert ", " not in text and ": " not in text
    assert [n["unique_id"] for n in _decode(raw)["nodes"]] == ["a.first", "b.second"]
    assert not text.endswith("\n")


def test_the_document_header_carries_schema_version_tenant_and_release():
    document = _decode(canonical_topology_json(tenant_id="default", release_id="rel-1", nodes=[]))

    assert document == {"nodes": [], "release_id": "rel-1", "schema_version": 1, "tenant_id": "default"}


def test_a_node_keeps_exactly_the_artifact_fields_with_go_zero_value_defaults():
    """The Go document is a typed struct: a key outside its node shape cannot
    exist there, and a missing field is its zero value. The Python writer must
    produce the same bytes, so it drops unknown keys (candidate_artifact_uri
    above all — release-controller derives that reference) and fills the
    defaults the Go zero values carry."""
    sparse = {"unique_id": "a.x", "candidate_artifact_uri": "s3://b/candidate-sql/rel-1/candidate_a.x.sql"}

    (node,) = _decode(canonical_topology_json(tenant_id="default", release_id="rel-1", nodes=[sparse]))["nodes"]

    assert node == {
        "unique_id": "a.x", "schema_name": "", "table_name": "", "resolved_relation_id": "",
        "service_name": "", "node_type": "", "test_count": 0, "content_hash": "",
        "image_tag": "", "original_file_path": "", "upstream_unique_ids": [], "schedule": "",
    }


def test_null_upstreams_become_an_empty_list_and_an_empty_secret_ref_is_left_out():
    raw = canonical_topology_json(
        tenant_id="default", release_id="rel-1",
        nodes=[_node("a.x", upstream_unique_ids=None, secret_ref=""), _node("b.y", secret_ref="continuo-api-fx")],
    )

    first, second = _decode(raw)["nodes"]
    assert first["upstream_unique_ids"] == []
    assert "secret_ref" not in first
    assert second["secret_ref"] == "continuo-api-fx"


def test_upstreams_are_sorted():
    """Edges are a set: the canonical form sorts each node's upstreams, as Go's
    CanonicalJSON does, so resolution order never changes the bytes."""
    raw = canonical_topology_json(
        tenant_id="default", release_id="rel-1",
        nodes=[_node("a.x", upstream_unique_ids=["z.last", "a.first"])],
    )

    assert _decode(raw)["nodes"][0]["upstream_unique_ids"] == ["a.first", "z.last"]


def test_non_ascii_and_html_characters_are_written_raw_as_utf8():
    raw = canonical_topology_json(
        tenant_id="default", release_id="rel-1",
        nodes=[_node("a.x", original_file_path="models/zürich/ørders <&>.sql")],
    )

    assert "models/zürich/ørders <&>.sql".encode("utf-8") in raw
    escaped = ("\\" + "u00fc").encode("ascii"), ("\\" + "u003c").encode("ascii")
    assert all(e not in raw for e in escaped)


def test_line_and_paragraph_separators_are_escaped_as_go_escapes_them():
    """Go's encoding/json always escapes U+2028 and U+2029; leaving them raw
    would make the Python and Go writers' bytes differ for one document."""
    raw = canonical_topology_json(
        tenant_id="default", release_id="rel-1",
        nodes=[_node("a.x", original_file_path="a" + chr(0x2028) + "b" + chr(0x2029) + "c")],
    )

    escaped = "a" + "\\" + "u2028b" + "\\" + "u2029c"
    assert escaped.encode("ascii") in raw
    assert _decode(raw)["nodes"][0]["original_file_path"] == "a" + chr(0x2028) + "b" + chr(0x2029) + "c"


def test_a_node_type_enum_member_is_written_as_its_value():
    raw = canonical_topology_json(
        tenant_id="default", release_id="rel-1",
        nodes=[_node("a.x", node_type=NodeType.PYTHON_API)],
    )

    assert _decode(raw)["nodes"][0]["node_type"] == "python-api"


def test_the_artifact_key_is_under_the_tenant_prefix():
    assert topology_artifact_key("default", "rel-1") == "tenants/default/topologies/rel-1/topology.json.gz"


# --- the writer ---------------------------------------------------------------

def test_the_writer_stores_the_gzipped_canonical_document_and_returns_its_reference():
    s3 = MagicMock()
    nodes = [_node("b.y"), _node("a.x")]

    ref = TopologyArtifactWriter(s3, "continuo").write(tenant_id="default", release_id="rel-1", nodes=nodes)

    s3.put_object.assert_called_once()
    put = s3.put_object.call_args.kwargs
    assert put["Bucket"] == "continuo"
    assert put["Key"] == "tenants/default/topologies/rel-1/topology.json.gz"
    assert put["ContentType"] == "application/gzip"
    assert "ContentEncoding" not in put, "a Content-Encoding header would let an HTTP client gunzip it in transit"
    body = put["Body"]
    assert gzip.decompress(body) == canonical_topology_json(tenant_id="default", release_id="rel-1", nodes=nodes)
    assert ref == TopologyArtifactRef(
        uri="s3://continuo/tenants/default/topologies/rel-1/topology.json.gz",
        sha256=hashlib.sha256(body).hexdigest(),
        node_count=2,
    )


def test_the_same_release_always_produces_the_same_bytes():
    """A redelivered release.requested rewrites the object: identical bytes keep
    the checksum an earlier event may already carry valid."""
    first, second = MagicMock(), MagicMock()
    nodes = [_node("a.x"), _node("b.y")]

    TopologyArtifactWriter(first, "continuo").write(tenant_id="default", release_id="rel-1", nodes=nodes)
    TopologyArtifactWriter(second, "continuo").write(tenant_id="default", release_id="rel-1", nodes=list(reversed(nodes)))

    assert first.put_object.call_args.kwargs["Body"] == second.put_object.call_args.kwargs["Body"]


def test_the_gzip_header_carries_no_timestamp_and_no_file_name():
    s3 = MagicMock()

    TopologyArtifactWriter(s3, "continuo").write(tenant_id="default", release_id="rel-1", nodes=[])

    body = s3.put_object.call_args.kwargs["Body"]
    assert body[:2] == b"\x1f\x8b"
    assert body[4:8] == b"\x00\x00\x00\x00", "MTIME must be zero"
    assert body[3] & 0x08 == 0, "FNAME must not be set"


def test_a_put_failure_reaches_the_caller():
    s3 = MagicMock()
    s3.put_object.side_effect = RuntimeError("bucket gone")

    with pytest.raises(RuntimeError, match="bucket gone"):
        TopologyArtifactWriter(s3, "continuo").write(tenant_id="default", release_id="rel-1", nodes=[])


# --- candidate objects land where release-controller looks --------------------

def _builder_for(node_type: NodeType, s3) -> object:
    if NODE_TYPE_RUNTIME[node_type] is Runtime.PYTHON:
        return PythonSpecArtifactBuilder(CandidateSpecUploader(s3, "continuo"))
    return DbtSqlArtifactBuilder(CandidateSqlUploader(s3, "continuo"))


def test_every_candidate_object_lands_at_the_key_release_controller_derives():
    """release-controller derives each node's candidate-object URI from the
    release id, unique_id and node_type (pkg/topologyartifact.CandidateObjectKey)
    instead of reading it from the topology. This drives the production builders
    and uploaders — a seed has no compiled SQL, as the dbt parser leaves it — and
    checks every object lands at the fixture's key, or that none is written
    where the key is empty."""
    cases = json.loads((_FIXTURES / "candidate_object_keys_v1.json").read_text(encoding="utf-8"))
    known = {str(t) for t in NodeType}
    assert known <= {c["node_type"] for c in cases}, "the fixture must cover every node type"

    for case in cases:
        if case["node_type"] not in known:
            continue  # a type topology-controller cannot parse has no builder; Go derives no key for it
        node_type = NodeType(case["node_type"])
        s3 = MagicMock()
        node = ManifestNode(
            table_name="t", schema_name="s", service_name="svc", owner="team",
            schedule_name="daily", criticality="CORE",
            candidate_sql="" if node_type is NodeType.DBT_SEED else "select 1",
            node_type=node_type, runtime=NODE_TYPE_RUNTIME[node_type],
            identity=case["unique_id"],
        )
        ctx = RewriteContext(
            release_id=case["release_id"], registry={},
            candidate_schema="_candidate_x", dialect="postgres",
        )

        _builder_for(node_type, s3).build(node, ctx)

        written = s3.put_object.call_args.kwargs["Key"] if s3.put_object.called else ""
        assert written == case["key"], case
