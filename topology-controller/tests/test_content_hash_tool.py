"""The e2e tests ask topology-controller's own parser for the content hash of each node, by running
`tools/content_hash.py` in its container. It calls the parser the way a release does, so a change to
that call breaks it where only the e2e run would show: this runs the script, as the e2e does, on the
manifests the component's own tests use."""
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from tests.fakes import parse_manifest as parse_for_test

COMPONENT_ROOT = Path(__file__).parent.parent
FIXTURES = Path(__file__).parent / "fixtures"


def _run(directory: Path) -> dict[str, str]:
    environment = {**os.environ, "PYTHONPATH": os.pathsep.join([str(COMPONENT_ROOT), str(COMPONENT_ROOT / "proto")])}
    done = subprocess.run(
        [sys.executable, str(COMPONENT_ROOT / "tools" / "content_hash.py"), str(directory)],
        cwd=COMPONENT_ROOT, env=environment, capture_output=True, text=True, check=False,
    )
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout)


@pytest.mark.parametrize("fixture", ["manifest_valid.json", "manifest_with_test.json", "manifest_service1.json"])
def test_the_script_prints_the_content_hash_of_each_node_the_parser_yields(tmp_path, fixture):
    shutil.copy(FIXTURES / fixture, tmp_path / "manifest.json")

    printed = _run(tmp_path)

    parsed = parse_for_test(str(tmp_path / "manifest.json"))
    assert printed == {node.unique_id: node.content_hash for node in parsed.nodes}
    assert printed, "the manifest yields nodes"


def test_the_script_reads_the_compile_extras_beside_the_manifest(tmp_path):
    shutil.copy(FIXTURES / "manifest_valid.json", tmp_path / "manifest.json")
    seed_manifest = json.loads((tmp_path / "manifest.json").read_text())
    seed_id = next((key for key, node in seed_manifest["nodes"].items() if node["resource_type"] == "seed"), None)
    if seed_id is None:
        pytest.skip("the fixture holds no seed whose file hash the extras would change")
    (tmp_path / "compile-extras.json").write_text(json.dumps({
        "version": 1, "project": "", "release_id": "rel-1", "project_flags": {}, "packages": {},
        "seeds": {seed_id: {"sha256": "ab" * 32, "header": [], "column_types": {}}},
    }))
    with_extras = _run(tmp_path)
    (tmp_path / "compile-extras.json").unlink()

    assert with_extras != _run(tmp_path)

