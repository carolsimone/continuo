"""The contract bundle writer: one gzipped canonical document per release, written once,
and the reference of the bundle the store holds, with that bundle's counts by family."""
import gzip
import hashlib
import json
import tracemalloc
from pathlib import Path

import botocore.exceptions
import pytest
from continuo_contracts.canonical import canonical_json
from continuo_contracts.errors import CanonicalJsonError

import adapters.contract_bundle_writer as writer_module
from adapters.contract_bundle_writer import ContractBundleWriter
from domain.model import ContractBundleRef, ContractFamily
from service.contract_bundle import StoredContract, build_contract_bundle, contract_bundle_key
from service.errors import UnreadableContractBundleError
from tests.fakes import FakeS3, client_error

KEY = contract_bundle_key("default", "r1")
URI = f"s3://b/{KEY}"


def _contract(unique_id: str, family: ContractFamily, document: dict | None = None) -> StoredContract:
    return StoredContract(
        unique_id=unique_id, family=family, service="svc", contract_hash="sha256:" + "a" * 64,
        content_hash="sha256:" + "b" * 64, source_path=f"{unique_id}.sql", source_sha256="c" * 64,
        document=document if document is not None else {"id": unique_id, "name": "é"},
    )


def _bundle(*contracts: StoredContract, timings: dict | None = None) -> dict:
    """A bundle document as the handler builds it, holding `contracts`."""
    return build_contract_bundle(
        tenant_id="default", release_id="r1", library_version="0.3.1", contracts=contracts, provenance={},
        timings=timings if timings is not None else {"stages_ms": {"dbt_parse": 12}},
    )


def _python_only() -> dict:
    return _bundle(*(_contract(f"analytics.py_{n}", ContractFamily.PYTHON) for n in range(3)))


def _dbt_and_python() -> dict:
    return _bundle(_contract("analytics.orders", ContractFamily.DBT), _contract("analytics.users", ContractFamily.DBT),
                   *(_contract(f"analytics.py_{n}", ContractFamily.PYTHON) for n in range(3)))


def _gzipped(document: object, *, mtime: int = 0) -> bytes:
    return gzip.compress(canonical_json(document), mtime=mtime)


def _another_writer_stores(body: bytes):
    """A before_put hook: another writer's put of the same key lands between this
    writer's read (which found nothing) and its own put."""

    def store(s3: FakeS3) -> None:
        s3.objects.setdefault(("b", KEY), body)

    return store


@pytest.fixture
def fake_s3() -> FakeS3:
    return FakeS3()


def _write(s3: FakeS3, document: dict, **kwargs) -> ContractBundleRef:
    return ContractBundleWriter(s3, "b", **kwargs).write(tenant_id="default", release_id="r1", document=document)


def test_the_writer_stores_the_gzipped_canonical_document_and_returns_its_reference(fake_s3):
    document = _dbt_and_python()

    ref = _write(fake_s3, document)

    [put] = fake_s3.puts
    assert (put["Bucket"], put["Key"], put["ContentType"]) == ("b", KEY, "application/gzip")
    assert put["IfNoneMatch"] == "*", "the put must create the object only when no other writer has"
    assert "ContentEncoding" not in put, "a Content-Encoding header would let an HTTP client gunzip it in transit"
    assert put["Body"] == gzip.compress(canonical_json(document), mtime=0)
    assert gzip.decompress(put["Body"]) == canonical_json(document)
    assert ref == ContractBundleRef(
        uri=URI, sha256=hashlib.sha256(put["Body"]).hexdigest(), by_family={"dbt": 2, "python": 3})
    assert len(fake_s3.gets) == 1


def test_the_gzip_header_carries_no_timestamp_and_no_file_name(fake_s3):
    _write(fake_s3, _bundle())

    body = fake_s3.puts[0]["Body"]
    assert body[:2] == b"\x1f\x8b"
    assert body[4:8] == b"\x00\x00\x00\x00", "MTIME must be zero"
    assert body[3] & 0x08 == 0, "FNAME must not be set"


def test_a_second_write_of_the_same_release_keeps_the_first_bytes_and_their_counts(fake_s3):
    first = _write(fake_s3, _python_only())
    second = _write(fake_s3, _dbt_and_python())

    assert second == first and first.by_family == {"python": 3}
    assert gzip.decompress(fake_s3.get("b", KEY)) == canonical_json(_python_only())
    assert len(fake_s3.puts) == 1


def test_an_existing_bundle_gives_its_own_checksum_and_counts_from_the_one_read(fake_s3):
    """Another parse of the release (another version of topology-controller, say) stored a bundle
    without the dbt contracts: the reference names that bundle and what it holds, read from the bytes
    the writer's first read fetched, with no second read."""
    stored = _gzipped(_python_only(), mtime=5)
    fake_s3.objects[("b", KEY)] = stored

    ref = _write(fake_s3, _dbt_and_python())

    assert ref == ContractBundleRef(uri=URI, sha256=hashlib.sha256(stored).hexdigest(), by_family={"python": 3})
    assert fake_s3.puts == []
    assert len(fake_s3.gets) == 1


def test_another_release_is_written_even_when_one_exists(fake_s3):
    _write(fake_s3, _python_only())

    other = ContractBundleWriter(fake_s3, "b").write(tenant_id="default", release_id="r2", document=_bundle())

    assert other.uri == f"s3://b/{contract_bundle_key('default', 'r2')}"
    assert other.by_family == {}
    assert len(fake_s3.puts) == 2


@pytest.mark.parametrize(
    ("code", "status"), [("NoSuchKey", 404), ("404", 404), ("NotFound", 404)], ids=["no-such-key", "404", "not-found"])
def test_every_answer_for_a_missing_object_means_write_it(code, status):
    s3 = FakeS3(get_error=client_error(code, status, "GetObject"))

    _write(s3, _bundle())

    assert len(s3.puts) == 1


@pytest.mark.parametrize(
    "error",
    [
        botocore.exceptions.EndpointConnectionError(endpoint_url="http://minio:9000"),
        client_error("SlowDown", 503, "GetObject"),
        client_error("AccessDenied", 403, "GetObject"),
    ],
    ids=["unreachable", "503", "403"],
)
def test_a_read_failure_other_than_a_missing_object_reaches_the_caller_and_writes_nothing(error):
    s3 = FakeS3(get_error=error)

    with pytest.raises(type(error)) as raised:
        _write(s3, _bundle())

    assert raised.value is error
    assert s3.puts == []


def test_a_writer_that_loses_the_race_returns_the_reference_and_counts_of_the_winners_bytes():
    winner = _gzipped(_python_only())
    s3 = FakeS3(before_put=_another_writer_stores(winner))

    ref = _write(s3, _dbt_and_python())

    assert ref == ContractBundleRef(uri=URI, sha256=hashlib.sha256(winner).hexdigest(), by_family={"python": 3})
    assert s3.get("b", KEY) == winner, "the losing writer must not replace the stored bytes"
    assert len(s3.puts) == 1


def test_a_writer_whose_own_bytes_the_winner_stored_reads_no_summary(monkeypatch):
    """Two parses that build the same bytes: the counts are the document's, and the stored bytes are
    read only as far as the checksum needs, as before."""
    document = _dbt_and_python()
    s3 = FakeS3(before_put=_another_writer_stores(_gzipped(document)))
    monkeypatch.setattr(writer_module, "_stored_by_family", lambda *_: pytest.fail("the summary was read"))

    ref = _write(s3, document)

    assert ref.by_family == {"dbt": 2, "python": 3}
    assert ref.sha256 == hashlib.sha256(_gzipped(document)).hexdigest()
    assert (len(s3.gets), len(s3.puts)) == (2, 1)


def test_a_conflicting_conditional_write_reads_the_object_the_other_writer_stored():
    winner = _gzipped(_python_only())

    def conflict_then_land(s3: FakeS3) -> None:
        s3.objects.setdefault(("b", KEY), winner)

    s3 = FakeS3(
        put_errors=[client_error("ConditionalRequestConflict", 409, "PutObject")],
        before_put=conflict_then_land,
    )

    ref = _write(s3, _dbt_and_python(), sleep=lambda _: None)

    assert ref == ContractBundleRef(uri=URI, sha256=hashlib.sha256(winner).hexdigest(), by_family={"python": 3})
    assert len(s3.puts) == 1


def test_a_conflict_whose_object_is_not_there_yet_is_tried_again():
    s3 = FakeS3(put_errors=[client_error("ConditionalRequestConflict", 409, "PutObject")])
    waits: list[float] = []

    ref = _write(s3, _dbt_and_python(), sleep=waits.append)

    assert len(s3.puts) == 2 and all(p["IfNoneMatch"] == "*" for p in s3.puts)
    assert ref.sha256 == hashlib.sha256(s3.get("b", KEY)).hexdigest()
    assert ref.by_family == {"dbt": 2, "python": 3}
    assert len(waits) == 1 and waits[0] > 0


def test_conflicts_that_never_settle_reach_the_caller():
    conflict = client_error("ConditionalRequestConflict", 409, "PutObject")
    s3 = FakeS3(put_error=conflict)

    with pytest.raises(botocore.exceptions.ClientError) as raised:
        _write(s3, _bundle(), sleep=lambda _: None)

    assert raised.value is conflict
    assert 1 < len(s3.puts) <= 10


@pytest.mark.parametrize(
    "error",
    [
        botocore.exceptions.EndpointConnectionError(endpoint_url="http://minio:9000"),
        client_error("SlowDown", 503, "PutObject"),
        client_error("AccessDenied", 403, "PutObject"),
    ],
    ids=["unreachable", "503", "403"],
)
def test_a_put_failure_other_than_a_lost_race_reaches_the_caller_unchanged(error):
    s3 = FakeS3(put_error=error)

    with pytest.raises(type(error)) as raised:
        _write(s3, _bundle())

    assert raised.value is error
    assert len(s3.puts) == 1


def test_a_put_failure_reaches_the_caller():
    s3 = FakeS3(put_error=RuntimeError("bucket gone"))

    with pytest.raises(RuntimeError, match="bucket gone"):
        _write(s3, _bundle())


# --- the counts of a bundle another parse stored ----------------------------------------

def _decoy(by_family: dict) -> dict:
    """A contract document whose free-form property values hold keys named `summary`, as
    a team's meta can: none of them is the bundle's summary."""
    return {
        "id": "analytics.decoy",
        "customProperties": [
            {"property": "meta", "value": {"summary": {"by_family": by_family, "contracts": 7}}},
            {"property": 'a"summary', "value": 'a string that says "summary":{"by_family":{}}'},
            {"property": "notes", "value": [{"summary": {"by_family": by_family}}]},
        ],
    }


def _stored_with(document: dict) -> FakeS3:
    s3 = FakeS3()
    s3.objects[("b", KEY)] = _gzipped(document, mtime=9)
    return s3


def _no_whole_read(monkeypatch) -> None:
    """Fail the test if the stored bundle is decompressed or decoded as a whole."""
    monkeypatch.setattr(gzip, "decompress", lambda *_: pytest.fail("the stored bundle was decompressed whole"))
    monkeypatch.setattr(json, "loads", lambda *_: pytest.fail("the stored bundle was decoded whole"))


def test_keys_named_summary_inside_the_contracts_do_not_hide_the_bundles_summary(monkeypatch):
    stored = _bundle(_contract("analytics.decoy", ContractFamily.DBT, _decoy({"dbt": 99, "python": 99})),
                     _contract("analytics.py_kpis", ContractFamily.PYTHON))
    s3 = _stored_with(stored)
    _no_whole_read(monkeypatch)

    ref = _write(s3, _bundle())

    assert ref.by_family == {"dbt": 1, "python": 1}


def test_the_summary_of_a_bundle_larger_than_the_end_kept_is_found_at_its_end(monkeypatch):
    big = {"id": "analytics.big", "blob": "x" * (3 * writer_module._END_BYTES)}
    s3 = _stored_with(_bundle(_contract("analytics.big", ContractFamily.DBT, big)))
    _no_whole_read(monkeypatch)

    assert _write(s3, _bundle()).by_family == {"dbt": 1}


@pytest.mark.parametrize("text", [
    # A key after `timings`, which a newer bundle could add, holding a key named `summary`.
    canonical_json({**_python_only(), "zz_extra": {"summary": {"by_family": {"dbt": 9}}}}),
    # A key after `timings` whose own name ends in `"summary"` (an escaped quote, then `summary`): the
    # last `"summary":` in the bytes is inside that name, after a backslash, not after a `{` or a `,`.
    canonical_json({**_python_only(), 'zz"summary': {"by_family": {"dbt": 9}}}),
    # A bundle written with spaces, as no canonical writer does.
    json.dumps(_python_only(), indent=1).encode(),
], ids=["key-after-timings", "key-ending-in-summary", "not-canonical"])
def test_a_bundle_of_another_layout_is_read_whole(text):
    s3 = FakeS3()
    s3.objects[("b", KEY)] = gzip.compress(text, mtime=0)

    assert _write(s3, _bundle()).by_family == {"python": 3}


@pytest.mark.parametrize("stored", [
    b"not gzip at all",
    gzip.compress(b'{"summary": {"by_family": {"dbt"', mtime=0),
    gzip.compress(canonical_json(_python_only()), mtime=0)[:-12],
    gzip.compress(b'{"written":"by another pod"}', mtime=0),
    gzip.compress(b'[1, 2]', mtime=0),
    _gzipped({**_python_only(), "summary": {"contracts": 3}}),
    _gzipped({**_python_only(), "summary": {"by_family": {"python": -1}}}),
    _gzipped({**_python_only(), "summary": {"by_family": {"python": True}}}),
    _gzipped({**_python_only(), "summary": {"by_family": {"python": 1.5}}}),
    _gzipped({**_python_only(), "summary": {"by_family": ["python"]}}),
    _gzipped({**_python_only(), "summary": "3 contracts"}),
], ids=["not-gzip", "not-json", "cut-short", "no-summary", "not-an-object", "no-by-family", "negative-count",
        "bool-count", "float-count", "by-family-not-a-mapping", "summary-not-a-mapping"])
def test_a_stored_bundle_that_cannot_be_read_raises_and_writes_nothing(stored):
    s3 = FakeS3()
    s3.objects[("b", KEY)] = stored

    with pytest.raises(UnreadableContractBundleError, match=f"the contract bundle {URI} is stored and"):
        _write(s3, _dbt_and_python())

    assert s3.puts == []


# --- the bytes are written a piece at a time ----------------------------------------------

FIXTURES = Path(__file__).parent / "fixtures"

# Two pairs of characters whose code point order and UTF-16 code unit order differ: a character
# beyond the BMP is written as a surrogate pair (U+D800 to U+DFFF), so it sorts before U+E000 to
# U+FFFF in RFC 8785 and after them by code point.
_ASTRAL, _HIGH_BMP = "\U0001f600", "Ａ"


def _fixture_bundle() -> tuple[dict, bytes]:
    fixture = json.loads((FIXTURES / "contracts_v1.json").read_text())
    given = fixture["input"]
    document = build_contract_bundle(
        tenant_id=given["tenant_id"], release_id=given["release_id"], library_version=given["library_version"],
        contracts=[StoredContract(**c) for c in given["contracts"]], provenance=given["provenance"],
        timings=given["timings"])
    return document, fixture["canonical"].encode()


def _columns(count: int, seed: int) -> list[dict]:
    return [{"name": f"column_{seed}_{n:02d}", "logicalType": "string", "physicalType": "varchar(255)",
             "description": f"Column {n} of contract {seed}, ünïcödé and {_ASTRAL} included.",
             "quality": [{"id": f"not_null_{seed}_{n}", "metric": "nullValues", "mustBe": 0}]}
            for n in range(count)]


def _synthetic_bundle(contracts: int, *, columns: int = 4) -> dict:
    """A bundle of `contracts` dbt contracts whose ids sort differently by code point and by UTF-16
    code unit, with documents that hold keys named `summary` and keys beyond the BMP."""
    names = ["a", "é", "Ω", "z", _HIGH_BMP, _ASTRAL, f"x{_ASTRAL}", f"x{_HIGH_BMP}", "\U00010000", "ﬁ"]
    stored = []
    for n in range(contracts):
        unique_id = f"s{n % 7}.{names[n % len(names)]}_{n}"
        document = {"id": unique_id, "apiVersion": "v3.2.0", "schema": [{"name": unique_id, "properties": _columns(
            columns, n)}], "customProperties": [{"property": "meta", "value": {
                "summary": {"by_family": {"dbt": n}}, f"k{_ASTRAL}": 1, f"k{_HIGH_BMP}": 2}}]}
        stored.append(_contract(unique_id, ContractFamily.DBT if n % 3 else ContractFamily.PYTHON, document))
    return _bundle(*stored, timings={"stages_ms": {"dbt_parse": 5, f"x{_ASTRAL}": 1, f"x{_HIGH_BMP}": 2}})


def test_the_ids_of_the_synthetic_bundle_sort_differently_by_code_point_and_by_utf16():
    ids = list(_synthetic_bundle(200)["contracts"])
    assert sorted(ids) != sorted(ids, key=lambda key: key.encode("utf-16-be"))


@pytest.mark.parametrize("make", [
    lambda: _fixture_bundle()[0],
    lambda: _synthetic_bundle(200),
    lambda: {**_synthetic_bundle(3), f"top{_ASTRAL}": [1, {"b": 2, "a": 1}], f"top{_HIGH_BMP}": "x"},
    lambda: {"contracts": [1, 2], "summary": {}},
    lambda: {},
], ids=["shared-fixture", "200-contracts", "top-level-keys-beyond-the-bmp", "contracts-not-a-mapping", "empty"])
def test_the_streamed_bytes_are_gzip_compress_of_the_canonical_document(make):
    document = make()
    assert writer_module.gzipped_canonical_json(document) == gzip.compress(canonical_json(document), mtime=0)


def test_the_streamed_bytes_of_the_shared_fixture_inflate_to_its_canonical_text():
    document, canonical = _fixture_bundle()
    assert gzip.decompress(writer_module.gzipped_canonical_json(document)) == canonical


@pytest.mark.parametrize("document", [
    {1: "a"}, {"contracts": {2: {}}}, {"a\ud800": 1}, {"contracts": {"x\ud800": {}}}, {"contracts": {"x": {1: 2}}},
], ids=["top-level-key", "contract-id", "top-level-surrogate", "contract-id-surrogate", "nested-key"])
def test_a_key_canonical_json_refuses_is_refused(document):
    with pytest.raises(CanonicalJsonError):
        canonical_json(document)
    with pytest.raises(CanonicalJsonError):
        writer_module.gzipped_canonical_json(document)


def test_writing_a_large_bundle_holds_no_more_than_a_few_mebibytes_above_its_input():
    """The canonical JSON of 2,000 contracts of 40 columns is larger than the bound on its own, so a
    writer that built it whole (the pieces it joins, the text and its bytes) could not stay under it."""
    document = _synthetic_bundle(2000, columns=40)
    bound = 12 * 2**20
    canonical = canonical_json(document)
    assert len(canonical) > bound
    expected = gzip.compress(canonical, mtime=0)
    del canonical

    tracemalloc.start()
    try:
        tracemalloc.reset_peak()
        before, _ = tracemalloc.get_traced_memory()
        body = writer_module.gzipped_canonical_json(document)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert peak - before < bound, f"the write held {(peak - before) / 2**20:.1f} MiB above its input"
    assert body == expected
