#!/usr/bin/env python3
"""Rebase one Go module's govulncheck SARIF paths onto the repository root.

govulncheck runs once per Go module and writes every path in its report relative
to the module it ran in, under the base %SRCROOT%: a finding in the pkg module
points at "go.mod", not "pkg/go.mod". Code scanning resolves %SRCROOT% to the
repository root, so without this rewrite every module's findings land on one
root go.mod that does not exist, and the modules cannot be told apart.

This prefixes MODULE_DIR to every artifact location under %SRCROOT%, wherever it
appears in the document (result locations and call-stack frames alike).
Locations under any other base, such as the standard library's %GOROOT%, keep
their path. The file is rewritten in place once the whole document has been
read; a file that cannot be read as JSON is left as it was.

Usage: sarif-rebase.py MODULE_DIR SARIF_FILE
"""
import json
import logging
import posixpath
import sys

log = logging.getLogger("sarif-rebase")

SRCROOT = "%SRCROOT%"


def rebase(node, prefix: str) -> None:
    """Prefix the uri of every %SRCROOT% artifactLocation under node."""
    if isinstance(node, dict):
        location = node.get("artifactLocation")
        if isinstance(location, dict) and location.get("uriBaseId") == SRCROOT and "uri" in location:
            location["uri"] = f"{prefix}/{location['uri']}"
        for value in node.values():
            rebase(value, prefix)
    elif isinstance(node, list):
        for item in node:
            rebase(item, prefix)


def main() -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="sarif-rebase: %(message)s")
    if len(sys.argv) != 3:
        log.error("Usage: sarif-rebase.py MODULE_DIR SARIF_FILE")
        return 2
    module_dir, path = sys.argv[1], sys.argv[2]

    try:
        with open(path) as fh:
            doc = json.load(fh)
    except (OSError, ValueError) as exc:
        log.error("cannot read %s as SARIF: %s", path, exc)
        return 1

    # normpath turns "./agent-chat" into "agent-chat"; a module at the
    # repository root (".") already has the right paths.
    prefix = posixpath.normpath(module_dir)
    if prefix != ".":
        rebase(doc, prefix)

    with open(path, "w") as fh:
        json.dump(doc, fh)
    return 0


if __name__ == "__main__":
    sys.exit(main())
