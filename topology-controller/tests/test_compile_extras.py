import hashlib
import json

import pytest
from continuo_contracts.canonical import canonical_json

from domain.exceptions import InvalidCompileExtrasError
from domain.model import SeedExtras
from service.compile_extras import load_compile_extras, seed_columns_hash

EXTRAS = {"version": 1, "service": "core", "release_id": "rel-1",
          "seeds": {"seed.core.seed_users": {"sha256": "4a" * 32, "header": ["id", "name"], "column_types": {"id": "integer"}}},
          "project_flags": {}, "packages": {"dbt-core": "1.12.0b1"}}


def _load(tmp_path, document, expected_service="core"):
    path = tmp_path / "x.json"
    path.write_text(document if isinstance(document, str) else json.dumps(document))
    return load_compile_extras(str(path), expected_service)


def test_load_compile_extras_reads_version_1(tmp_path):
    extras = _load(tmp_path, EXTRAS)
    assert extras.seeds["seed.core.seed_users"] == SeedExtras(sha256="4a" * 32, header=("id", "name"), column_types={"id": "integer"})
    assert (extras.service, extras.release_id, extras.project_flags, extras.packages) == (
        "core", "rel-1", {}, {"dbt-core": "1.12.0b1"})


def test_load_compile_extras_skips_the_service_check_when_none_is_expected(tmp_path):
    assert _load(tmp_path, {**EXTRAS, "service": "other"}, expected_service="").service == "other"


@pytest.mark.parametrize("change", [
    {"version": 2},
    {"version": True},
    {"service": "other"},
    {"service": 3},
    {"release_id": None},
    {"seeds": []},
    {"seeds": {"seed.core.seed_users": {"header": [], "column_types": {}}}},
    {"seeds": {"seed.core.seed_users": {"sha256": "4A" * 32, "header": [], "column_types": {}}}},
    {"seeds": {"seed.core.seed_users": {"sha256": "4a" * 31, "header": [], "column_types": {}}}},
    {"seeds": {"seed.core.seed_users": {"sha256": "4a" * 32, "header": "id", "column_types": {}}}},
    {"seeds": {"seed.core.seed_users": {"sha256": "4a" * 32, "header": [1], "column_types": {}}}},
    {"seeds": {"seed.core.seed_users": {"sha256": "4a" * 32, "header": [], "column_types": {"id": 3}}}},
    {"seeds": {"seed.core.seed_users": "x"}},
    {"project_flags": []},
    {"packages": {"dbt-core": 1}},
])
def test_load_compile_extras_refuses_a_bad_object(tmp_path, change):
    with pytest.raises(InvalidCompileExtrasError, match=r"^compile-extras\.json: "):
        _load(tmp_path, {**EXTRAS, **change})


@pytest.mark.parametrize("document", ["{", "[]", "null", '"x"'])
def test_load_compile_extras_refuses_a_document_that_is_not_an_object(tmp_path, document):
    with pytest.raises(InvalidCompileExtrasError, match=r"^compile-extras\.json: "):
        _load(tmp_path, document)


def test_load_compile_extras_refuses_a_missing_key(tmp_path):
    document = {key: value for key, value in EXTRAS.items() if key != "packages"}
    with pytest.raises(InvalidCompileExtrasError, match="packages"):
        _load(tmp_path, document)


def test_load_compile_extras_refuses_an_unreadable_object(tmp_path):
    with pytest.raises(InvalidCompileExtrasError, match=r"^compile-extras\.json: "):
        load_compile_extras(str(tmp_path / "absent.json"), "core")


def test_seed_columns_hash_is_the_jcs_of_header_and_column_types():
    seed = SeedExtras(sha256="4a" * 32, header=("id", "name"), column_types={"id": "integer"})
    want = "sha256:" + hashlib.sha256(canonical_json({"column_types": {"id": "integer"}, "header": ["id", "name"]})).hexdigest()
    assert seed_columns_hash(seed) == want


def test_seed_columns_hash_ignores_the_file_digest():
    first = SeedExtras(sha256="4a" * 32, header=("id",), column_types={})
    second = SeedExtras(sha256="5b" * 32, header=("id",), column_types={})
    assert seed_columns_hash(first) == seed_columns_hash(second)
