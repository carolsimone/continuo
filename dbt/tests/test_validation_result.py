"""Unit tests for the cross-language validation-result contract helpers."""
import json

from base.validation_result import (
    SENTINEL_BEGIN,
    SENTINEL_END,
    result_block,
    project_run_results,
)


def _extract(block: str) -> dict:
    lines = block.splitlines()
    assert lines[0] == SENTINEL_BEGIN
    assert lines[-1] == SENTINEL_END
    return json.loads(lines[1])


def test_result_block_is_sentinel_framed_single_line_json():
    block = result_block(status="error", message="boom", unique_id="model.svc.x")
    doc = _extract(block)
    assert doc == {
        "schema_version": 1,
        "status": "error",
        "message": "boom",
        "failures": 0,
        "unique_id": "model.svc.x",
    }
    # The JSON payload must be exactly one line so the Go side can split robustly.
    assert len(block.splitlines()) == 3


def test_result_block_success_defaults():
    doc = _extract(result_block(status="success"))
    assert doc["status"] == "success"
    assert doc["message"] == ""
    assert doc["failures"] == 0


def test_project_run_results_picks_first_result():
    doc = project_run_results({
        "results": [
            {"unique_id": "seed.svc.things", "status": "error",
             "message": "could not load", "failures": None},
        ]
    })
    assert doc == {
        "schema_version": 1,
        "status": "error",
        "message": "could not load",
        "failures": 0,
        "unique_id": "seed.svc.things",
    }


def test_project_run_results_empty_is_unknown_skipped():
    doc = project_run_results({"results": []})
    assert doc["status"] == "skipped"
    assert doc["unique_id"] == ""
