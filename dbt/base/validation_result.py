"""Cross-language contract for one validation node's structured result.

Both validation entrypoints (validation_runner.py for models/snapshots and
seed_validation_runner.py for seeds) print exactly one sentinel-framed block on
stdout as their LAST output. The k8s-controller extracts the block from the pod
log, uploads the JSON to S3, and surfaces it as run_results_uri. The remediation
classifier reads status + message deterministically, falling back to the text
log when the block is absent.

status uses dbt's RunStatus vocabulary (success | error | fail | skipped) so the
synthetic model/snapshot path and the real-dbt seed path speak one language.
"""
import json

SENTINEL_BEGIN = "===CONTINUO_VALIDATION_RESULT_BEGIN==="
SENTINEL_END = "===CONTINUO_VALIDATION_RESULT_END==="

SCHEMA_VERSION = 1


def result_block(status: str, message: str = "", failures: int = 0, unique_id: str = "") -> str:
    """Return the sentinel-framed, single-line-JSON block for one node.

    The JSON is compact (no embedded newlines) so the consumer can split the
    block out of a multi-line pod log by marker lines alone.
    """
    body = json.dumps(
        {
            "schema_version": SCHEMA_VERSION,
            "status": status,
            "message": message or "",
            "failures": int(failures or 0),
            "unique_id": unique_id or "",
        },
        separators=(",", ":"),
    )
    return f"{SENTINEL_BEGIN}\n{body}\n{SENTINEL_END}"


def project_run_results(doc: dict) -> dict:
    """Project a dbt run_results.json dict to the contract dict (first result).

    An empty results array (nothing ran) maps to a skipped record rather than an
    error, so the classifier treats it as "no structured signal" and falls back.
    """
    results = doc.get("results") or []
    if not results:
        return {
            "schema_version": SCHEMA_VERSION,
            "status": "skipped",
            "message": "",
            "failures": 0,
            "unique_id": "",
        }
    r = results[0]
    return {
        "schema_version": SCHEMA_VERSION,
        "status": r.get("status") or "error",
        "message": r.get("message") or "",
        "failures": int(r.get("failures") or 0),
        "unique_id": r.get("unique_id") or "",
    }
