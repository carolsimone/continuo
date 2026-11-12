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


def _project_with_package_seed(tmp_path, project_yml="name: core\n", install_dir="dbt_packages"):
    """A root project and an installed package, each with seeds/shared.csv holding different bytes."""
    (tmp_path / "dbt_project.yml").write_text(project_yml)
    (tmp_path / "seeds").mkdir()
    (tmp_path / "seeds" / "shared.csv").write_bytes(b"root_col\n1\n")
    pkg_seeds = tmp_path / install_dir / "pkg" / "seeds"
    pkg_seeds.mkdir(parents=True)
    (pkg_seeds / "shared.csv").write_bytes(b"pkg_col\n2\n")
    node = {
        "resource_type": "seed",
        "package_name": "pkg",
        "original_file_path": "seeds/shared.csv",
        "config": {},
    }
    return {"metadata": {}, "nodes": {"seed.pkg.shared": node}}


def test_a_package_seed_is_read_from_the_installed_package(tmp_path):
    manifest = _project_with_package_seed(tmp_path)
    doc = compile_extras.build_extras(manifest, str(tmp_path), "core", "rel-1")
    seed = doc["seeds"]["seed.pkg.shared"]
    assert seed["sha256"] == hashlib.sha256(b"pkg_col\n2\n").hexdigest()
    assert seed["header"] == ["pkg_col"]


def test_a_package_seed_honours_packages_install_path(tmp_path):
    manifest = _project_with_package_seed(
        tmp_path, project_yml="name: core\npackages-install-path: vendor\n", install_dir="vendor"
    )
    doc = compile_extras.build_extras(manifest, str(tmp_path), "core", "rel-1")
    assert doc["seeds"]["seed.pkg.shared"]["header"] == ["pkg_col"]


def test_a_root_project_seed_with_a_package_name_is_read_from_the_project_dir(tmp_path):
    manifest = _project_with_package_seed(tmp_path)
    node = manifest["nodes"]["seed.pkg.shared"]
    node["package_name"] = "core"
    doc = compile_extras.build_extras(manifest, str(tmp_path), "core", "rel-1")
    assert doc["seeds"]["seed.pkg.shared"]["header"] == ["root_col"]


def test_a_missing_package_seed_fails_naming_the_path(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(compile_extras, "_version", lambda dist: "1.0")
    manifest = _project_with_package_seed(tmp_path)
    (tmp_path / "dbt_packages" / "pkg" / "seeds" / "shared.csv").unlink()
    (tmp_path / "m.json").write_text(json.dumps(manifest))
    rc = compile_extras.main(["--manifest", str(tmp_path / "m.json"), "--project-dir", str(tmp_path),
                              "--service", "core", "--release-id", "rel-1", "--out", str(tmp_path / "x.json")])
    assert rc == 1 and not (tmp_path / "x.json").exists()
    assert str(tmp_path / "dbt_packages" / "pkg" / "seeds" / "shared.csv") in caplog.text


def test_main_writes_the_extras_with_sorted_keys(tmp_path, monkeypatch):
    monkeypatch.setattr(compile_extras, "_version", lambda dist: "1.0")
    (tmp_path / "m.json").write_text(json.dumps(_manifest(tmp_path)))
    out = tmp_path / "x.json"
    rc = compile_extras.main(["--manifest", str(tmp_path / "m.json"), "--project-dir", str(tmp_path),
                              "--service", "core", "--release-id", "rel-1", "--out", str(out)])
    assert rc == 0
    written = json.loads(out.read_text())
    assert list(written) == sorted(written)
    assert written["service"] == "core" and "seed.core.seed_users" in written["seeds"]
    seed = written["seeds"]["seed.core.seed_users"]
    assert list(seed) == sorted(seed)


def test_main_fails_when_a_seed_file_is_unreadable(tmp_path, monkeypatch):
    monkeypatch.setattr(compile_extras, "_version", lambda dist: "1.0")
    manifest = _manifest(tmp_path)
    (tmp_path / "seeds" / "seed_users.csv").unlink()
    (tmp_path / "m.json").write_text(json.dumps(manifest))
    rc = compile_extras.main(["--manifest", str(tmp_path / "m.json"), "--project-dir", str(tmp_path),
                              "--service", "core", "--release-id", "rel-1", "--out", str(tmp_path / "x.json")])
    assert rc == 1 and not (tmp_path / "x.json").exists()


def test_an_empty_seed_file_has_an_empty_header(tmp_path):
    doc = compile_extras.build_extras(_manifest(tmp_path, b""), str(tmp_path), "core", "rel-1")
    seed = doc["seeds"]["seed.core.seed_users"]
    assert seed["header"] == [] and seed["sha256"] == hashlib.sha256(b"").hexdigest()
