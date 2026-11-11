#!/usr/bin/env python3
"""Write compile-extras.json for a dbt compile.

Runs with the team image's own Python, the one that runs dbt, so it depends only on
the standard library and PyYAML (a dbt-core dependency) and keeps Python 3.9 syntax.

For one compiled service it records what the manifest does not carry about its seeds
and its dbt install: for each seed the SHA-256 of the raw file bytes, the CSV header
and the configured column types, plus the dbt_project.yml flags and the versions of
the installed dbt distributions. The output is deterministic JSON (sorted keys):

    {"version": 1, "service": str, "release_id": str,
     "seeds": {<seed unique_id>: {"sha256": hex, "header": [str], "column_types": {str: str}}},
     "project_flags": {...}, "packages": {<distribution>: <version>}}

Usage: compile_extras.py --manifest PATH --project-dir DIR --service NAME
                         --release-id ID --out PATH
Exit status: 0 on success, 1 (with one logged line) otherwise.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import logging
import os
import sys
from importlib import metadata

import yaml

logger = logging.getLogger("compile_extras")

SCHEMA_VERSION = 1


def _version(dist: str) -> str | None:
    """The installed version of a distribution, or None when it is not installed."""
    try:
        return metadata.version(dist)
    except metadata.PackageNotFoundError:
        return None


def installed_packages(adapter_type: str) -> dict[str, str]:
    """Versions of the installed dbt distributions: dbt-core, dbt-adapters, dbt-common
    and the adapter named by adapter_type. A distribution that is not installed is left out."""
    names = ["dbt-core", "dbt-adapters", "dbt-common"]
    if adapter_type:
        names.append("dbt-" + adapter_type)
    versions = {}
    for name in names:
        version = _version(name)
        if version is not None:
            versions[name] = version
    return versions


def seed_header(data: bytes, delimiter: str) -> list[str]:
    """The first CSV row of a seed's raw bytes, decoded as UTF-8 with a leading BOM
    dropped. An empty file has no header."""
    text = data.decode("utf-8-sig")
    row = next(csv.reader(io.StringIO(text, newline=""), delimiter=delimiter), [])
    return list(row)


def _seed_root(node: dict, project: dict, project_dir: str) -> str:
    """The directory a seed's original_file_path is relative to: the manifest's
    root_path when the node carries one, the installed package's directory for a seed
    of a dbt package, otherwise the project directory."""
    if node.get("root_path"):
        return node["root_path"]
    package = node.get("package_name")
    if package and package != project.get("name"):
        install_path = project.get("packages-install-path") or "dbt_packages"
        return os.path.join(project_dir, install_path, package)
    return project_dir


def build_extras(manifest: dict, project_dir: str, service: str, release_id: str) -> dict:
    """The compile-extras document for a parsed manifest and its dbt project directory."""
    with open(os.path.join(project_dir, "dbt_project.yml"), "rb") as f:
        project = yaml.safe_load(f) or {}

    seeds = {}
    for unique_id, node in (manifest.get("nodes") or {}).items():
        if node.get("resource_type") != "seed":
            continue
        path = os.path.join(_seed_root(node, project, project_dir), node["original_file_path"])
        with open(path, "rb") as f:
            raw = f.read()
        config = node.get("config") or {}
        seeds[unique_id] = {
            "sha256": hashlib.sha256(raw).hexdigest(),
            "header": seed_header(raw, config.get("delimiter") or ","),
            "column_types": dict(config.get("column_types") or {}),
        }

    adapter_type = (manifest.get("metadata") or {}).get("adapter_type") or ""
    return {
        "version": SCHEMA_VERSION,
        "service": service,
        "release_id": release_id,
        "seeds": seeds,
        "project_flags": dict(project.get("flags") or {}),
        "packages": installed_packages(adapter_type),
    }


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Write compile-extras.json for a dbt compile.")
    parser.add_argument("--manifest", required=True, help="path of the compiled manifest.json")
    parser.add_argument("--project-dir", required=True, help="the dbt project directory")
    parser.add_argument("--service", required=True, help="the service the project belongs to")
    parser.add_argument("--release-id", required=True, help="the release being compiled")
    parser.add_argument("--out", required=True, help="path of the compile-extras.json to write")
    args = parser.parse_args(argv)

    if _version("dbt-core") is None:
        logger.error(
            "dbt-core is not installed in %s: this script needs the Python that runs dbt",
            sys.executable,
        )
        return 1
    project_file = os.path.join(args.project_dir, "dbt_project.yml")
    if not os.path.isfile(project_file):
        logger.error("dbt project file %s does not exist", project_file)
        return 1
    try:
        with open(args.manifest, "rb") as f:
            manifest = json.load(f)
        doc = build_extras(manifest, args.project_dir, args.service, args.release_id)
    except (OSError, ValueError, KeyError, TypeError, csv.Error, yaml.YAMLError) as exc:
        logger.error("cannot build the compile extras from %s: %s: %s", args.manifest, type(exc).__name__, exc)
        return 1
    try:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(doc, f, sort_keys=True)
    except OSError as exc:
        logger.error("cannot write %s: %s", args.out, exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
