import ast
import hashlib
import json
import pathlib

import compile_extras


def _manifest(tmp_path, csv_bytes=b"id,name\n1,a\n", delimiter=None, root_path=True):
    seeds = tmp_path / "seeds"
    seeds.mkdir()
    (seeds / "seed_users.csv").write_bytes(csv_bytes)
    (tmp_path / "dbt_project.yml").write_text("name: core\nflags:\n  send_anonymous_usage_stats: false\n")
    cfg = {"column_types": {"id": "integer"}}
    if delimiter:
        cfg["delimiter"] = delimiter
    node = {"resource_type": "seed", "original_file_path": "seeds/seed_users.csv", "config": cfg}
    if root_path:
        node["root_path"] = str(tmp_path)
    return {"metadata": {"adapter_type": "postgres"}, "nodes": {"seed.core.seed_users": node}}


def test_seed_bytes_are_hashed_raw(tmp_path):
    raw = b"\xef\xbb\xbfid,name\r\n1,a  \r\n"
    doc = compile_extras.build_extras(_manifest(tmp_path, raw), str(tmp_path), "core", "rel-1")
    assert doc["seeds"]["seed.core.seed_users"]["sha256"] == hashlib.sha256(raw).hexdigest()


def test_header_strips_the_bom_and_honours_the_delimiter(tmp_path):
    raw = b'\xef\xbb\xbfid;"full, name"\n1;a\n'
    doc = compile_extras.build_extras(_manifest(tmp_path, raw, delimiter=";"), str(tmp_path), "core", "rel-1")
    assert doc["seeds"]["seed.core.seed_users"]["header"] == ["id", "full, name"]


def test_extras_carry_column_types_flags_and_identity(tmp_path):
    doc = compile_extras.build_extras(_manifest(tmp_path), str(tmp_path), "core", "rel-1")
    assert doc["version"] == 1 and doc["service"] == "core" and doc["release_id"] == "rel-1"
    assert doc["seeds"]["seed.core.seed_users"]["column_types"] == {"id": "integer"}
    assert doc["project_flags"] == {"send_anonymous_usage_stats": False}


def test_a_seed_without_root_path_is_read_from_the_project_dir(tmp_path):
    doc = compile_extras.build_extras(_manifest(tmp_path, root_path=False), str(tmp_path), "core", "rel-1")
    assert "seed.core.seed_users" in doc["seeds"]


def test_a_project_without_flags_or_seeds(tmp_path):
    (tmp_path / "dbt_project.yml").write_text("name: core\n")
    doc = compile_extras.build_extras({"metadata": {}, "nodes": {}}, str(tmp_path), "core", "rel-1")
    assert doc["seeds"] == {} and doc["project_flags"] == {}


def test_packages_name_the_installed_dbt_distributions(monkeypatch):
    versions = {"dbt-core": "1.12.0b1", "dbt-adapters": "1.24.5", "dbt-common": "1.39.0", "dbt-postgres": "1.10.0"}
    monkeypatch.setattr(compile_extras, "_version", versions.get)
    assert compile_extras.installed_packages("postgres") == versions


def test_main_fails_without_dbt_core(tmp_path, monkeypatch):
    monkeypatch.setattr(compile_extras, "_version", lambda dist: None)
    (tmp_path / "m.json").write_text(json.dumps(_manifest(tmp_path)))
    rc = compile_extras.main(["--manifest", str(tmp_path / "m.json"), "--project-dir", str(tmp_path),
                              "--service", "core", "--release-id", "rel-1", "--out", str(tmp_path / "x.json")])
    assert rc == 1 and not (tmp_path / "x.json").exists()


def test_main_fails_without_dbt_project_yml(tmp_path, monkeypatch):
    monkeypatch.setattr(compile_extras, "_version", lambda dist: "1.0")
    (tmp_path / "m.json").write_text(json.dumps({"metadata": {}, "nodes": {}}))
    rc = compile_extras.main(["--manifest", str(tmp_path / "m.json"), "--project-dir", str(tmp_path / "nope"),
                              "--service", "core", "--release-id", "rel-1", "--out", str(tmp_path / "x.json")])
    assert rc == 1


# The script runs on the team image's Python, which can be 3.9, while these tests run on 3.12.
def test_the_script_parses_as_python_3_9():
    ast.parse(pathlib.Path(compile_extras.__file__).read_text(), feature_version=(3, 9))
