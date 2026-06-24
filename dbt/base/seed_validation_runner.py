#!/usr/bin/env python3
"""Validation entrypoint for seed nodes.

Seeds are the one validation path that runs real dbt: `dbt seed --select <table>
--empty` builds an empty seed table in the candidate schema from the CSV's column
definitions. dbt writes target/run_results.json; we project its first result down
to the shared structured contract and print it as the sentinel block, so seed
nodes participate uniformly with model/snapshot nodes.

A non-zero dbt exit is preserved (the node fails); a missing/garbled
run_results.json degrades to a skipped block so the classifier falls back to the
text log rather than crashing.
"""
import argparse
import json
import os
import subprocess
import sys

try:
    from base import validation_result  # repo/test context (pythonpath=".")
except ModuleNotFoundError:  # pragma: no cover - flat layout inside the image
    import validation_result

RUN_RESULTS_PATH = "target/run_results.json"


def emit_from_run_results(path: str = RUN_RESULTS_PATH) -> None:
    """Read dbt run_results.json at path and print the contract block."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            doc = json.load(fh)
    except (OSError, json.JSONDecodeError):
        print(validation_result.result_block("skipped"), flush=True)
        return
    projected = validation_result.project_run_results(doc)
    print(
        validation_result.result_block(
            status=projected["status"],
            message=projected["message"],
            failures=projected["failures"],
            unique_id=projected["unique_id"],
        ),
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--select", required=True)
    args = parser.parse_args()

    proc = subprocess.run(
        ["dbt", "seed", "--select", args.select, "--empty"],
        cwd=os.getcwd(),
    )
    # Emit the structured block regardless of dbt's exit; the block reflects what
    # dbt recorded, and dbt's exit code still determines pod success/failure.
    emit_from_run_results()
    sys.exit(proc.returncode)


if __name__ == "__main__":
    main()
