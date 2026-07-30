#!/usr/bin/env python3
"""Merge several single-run SARIF files from the same tool into one run.

govulncheck is invoked once per Go module, so a repository-wide scan produces one
SARIF file per module. Code scanning refuses more than one run per category
(https://github.blog/changelog/2025-07-21-code-scanning-will-stop-combining-multiple-sarif-runs-uploaded-in-the-same-sarif-file/),
and uploading each module under its own category would need a job matrix and
scatter one tool across a dozen entries in the Security tab.

Merging is not a concatenation: each run carries its own tool.driver.rules array,
and every result points at a rule by ruleIndex into *that* array. Appending
results without remapping those indices silently mislabels findings. This
deduplicates rules by id and rewrites each result's ruleIndex to match.

Usage: merge-sarif.py OUT.sarif IN1.sarif [IN2.sarif ...]
"""
import json
import sys


def main() -> int:
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2
    out_path, in_paths = sys.argv[1], sys.argv[2:]

    rules: list[dict] = []
    rule_index_by_id: dict[str, int] = {}
    results: list[dict] = []
    driver: dict | None = None
    version = "2.1.0"
    schema = None

    for path in in_paths:
        with open(path) as fh:
            doc = json.load(fh)
        version = doc.get("version", version)
        schema = doc.get("$schema", schema)

        for run in doc.get("runs", []):
            run_driver = run.get("tool", {}).get("driver", {})
            if driver is None:
                # Keep the first driver's identity (name, version, informationUri)
                # but not its rules; those are rebuilt below across all runs.
                driver = {k: v for k, v in run_driver.items() if k != "rules"}

            # Map this run's rule positions onto the merged array.
            local_to_merged: dict[int, int] = {}
            for local_i, rule in enumerate(run_driver.get("rules", [])):
                rid = rule.get("id")
                if rid not in rule_index_by_id:
                    rule_index_by_id[rid] = len(rules)
                    rules.append(rule)
                local_to_merged[local_i] = rule_index_by_id[rid]

            for res in run.get("results", []):
                if "ruleIndex" in res:
                    old = res["ruleIndex"]
                    if old in local_to_merged:
                        res["ruleIndex"] = local_to_merged[old]
                    elif res.get("ruleId") in rule_index_by_id:
                        # Index out of range for its own run: fall back to the id,
                        # which is authoritative, rather than emit a bad pointer.
                        res["ruleIndex"] = rule_index_by_id[res["ruleId"]]
                    else:
                        del res["ruleIndex"]
                results.append(res)

    if driver is None:
        print("error: no runs found in input", file=sys.stderr)
        return 1
    driver["rules"] = rules

    merged = {"version": version, "runs": [{"tool": {"driver": driver}, "results": results}]}
    if schema:
        merged["$schema"] = schema

    with open(out_path, "w") as fh:
        json.dump(merged, fh)

    print(f"merged {len(in_paths)} file(s): {len(results)} result(s), {len(rules)} rule(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
