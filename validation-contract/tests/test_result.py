"""Unit tests for the validation-result contract helper."""
import json

from continuo_validation_contract import result


def _extract(block: str) -> dict:
    lines = block.splitlines()
    assert lines[0] == result.SENTINEL_BEGIN
    assert lines[-1] == result.SENTINEL_END
    return json.loads(lines[1])


def test_result_block_is_sentinel_framed_single_line_json():
    """A populated block round-trips through the sentinel framing intact."""
    block = result.result_block(status="error", message="boom", unique_id="model.svc.x")
    assert _extract(block) == {
        "schema_version": 1, "status": "error", "message": "boom",
        "failures": 0, "unique_id": "model.svc.x",
    }
    assert len(block.splitlines()) == 3


def test_result_block_success_defaults():
    """Omitted message/failures default to empty string and zero."""
    doc = _extract(result.result_block(status="success"))
    assert doc["status"] == "success"
    assert doc["message"] == ""
    assert doc["failures"] == 0
