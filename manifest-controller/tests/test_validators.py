import pytest

from service.validators import (
    MAX_OFFENDERS_IN_ERROR,
    ManifestValidationError,
    validate_manifest_batch,
)
from domain.model import ManifestNode


def _node(service: str, schema: str, table: str, image_tag: str = "abc123") -> ManifestNode:
    """Build a ManifestNode with stable defaults for fields irrelevant to this test."""
    return ManifestNode(
        service_name=service,
        schema_name=schema,
        table_name=table,
        owner="team-test",
        schedule_name="daily",
        criticality="CORE",
        compiled_sql="SELECT 1",
        image_tag=image_tag,
    )


def test_all_populated_returns_none():
    nodes = [_node("svc-a", "raw", "users"), _node("svc-b", "raw", "orders")]
    assert validate_manifest_batch(nodes) is None


def test_empty_input_returns_none():
    assert validate_manifest_batch([]) is None


def test_single_empty_raises_with_offender():
    nodes = [_node("svc-a", "raw", "users", image_tag="")]
    with pytest.raises(ManifestValidationError) as ei:
        validate_manifest_batch(nodes)
    assert ei.value.total_missing == 1
    assert "svc-a/raw/users" in ei.value.offenders


def test_multiple_empty_aggregated_and_sorted():
    nodes = [
        _node("z-svc", "raw", "z-tbl", image_tag=""),
        _node("a-svc", "raw", "a-tbl", image_tag=""),
        _node("m-svc", "raw", "m-tbl", image_tag=""),
    ]
    with pytest.raises(ManifestValidationError) as ei:
        validate_manifest_batch(nodes)
    assert ei.value.offenders == [
        "a-svc/raw/a-tbl",
        "m-svc/raw/m-tbl",
        "z-svc/raw/z-tbl",
    ]
    assert ei.value.total_missing == 3


def test_over_ten_truncated():
    nodes = [
        _node(f"svc-{i:02d}", "raw", f"tbl-{i:02d}", image_tag="")
        for i in range(12)
    ]
    with pytest.raises(ManifestValidationError) as ei:
        validate_manifest_batch(nodes)
    assert len(ei.value.offenders) == MAX_OFFENDERS_IN_ERROR
    assert ei.value.total_missing == 12
    # Lex-sort: svc-00..svc-09 fit; svc-10 and svc-11 are truncated.
    assert "svc-10/raw/tbl-10" not in ei.value.offenders
    assert "svc-11/raw/tbl-11" not in ei.value.offenders
    assert "...and 2 more" in str(ei.value)


def test_exactly_ten_no_truncation():
    nodes = [
        _node(f"svc-{i:02d}", "raw", f"tbl-{i:02d}", image_tag="")
        for i in range(MAX_OFFENDERS_IN_ERROR)
    ]
    with pytest.raises(ManifestValidationError) as ei:
        validate_manifest_batch(nodes)
    assert ei.value.total_missing == MAX_OFFENDERS_IN_ERROR
    assert "more" not in str(ei.value)
