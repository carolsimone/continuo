"""Unit tests for the validation-result contract helper."""
import json

from validation_result import SENTINEL_BEGIN, SENTINEL_END, result_block


def _extract(block: str) -> dict:
    lines = block.splitlines()
    assert lines[0] == SENTINEL_BEGIN
    assert lines[-1] == SENTINEL_END
    return json.loads(lines[1])


def test_result_block_is_sentinel_framed_single_line_json():
    block = result_block(status="error", message="boom", unique_id="model.svc.x")
    assert _extract(block) == {
        "schema_version": 1, "status": "error", "message": "boom",
        "failures": 0, "unique_id": "model.svc.x",
    }
    assert len(block.splitlines()) == 3


def test_result_block_success_defaults():
    doc = _extract(result_block(status="success"))
    assert doc["status"] == "success"
    assert doc["message"] == ""
    assert doc["failures"] == 0
