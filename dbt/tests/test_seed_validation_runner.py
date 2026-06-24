"""Unit tests for the seed validation wrapper (no real dbt or DB)."""
import json

import base.seed_validation_runner as swr
from base import validation_result


def test_emit_from_run_results_projects_and_frames(capsys, tmp_path):
    rr = tmp_path / "run_results.json"
    rr.write_text(json.dumps({"results": [
        {"unique_id": "seed.svc.things", "status": "error",
         "message": "bad csv", "failures": None}
    ]}))
    swr.emit_from_run_results(str(rr))
    out = capsys.readouterr().out
    doc = json.loads(out.splitlines()[1])
    assert doc["status"] == "error"
    assert doc["unique_id"] == "seed.svc.things"
    assert validation_result.SENTINEL_END in out


def test_emit_when_run_results_missing_emits_skipped(capsys, tmp_path):
    swr.emit_from_run_results(str(tmp_path / "nope.json"))
    doc = json.loads(capsys.readouterr().out.splitlines()[1])
    assert doc["status"] == "skipped"
