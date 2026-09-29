#!/usr/bin/env bash
# Fails unless the python node kinds continuo-python-runtime accepts (its
# KINDS, read from the exact runtime image continuo pins for validation) equal
# the node_type values pkg/streams/contract.yaml marks runtime: python.
#
# The runtime is released from its own repository, so its kind list cannot be
# generated from contract.yaml; this check is what keeps the two in step. The
# image is the one docker-compose.yml pins, which check-validation-image-pin.sh
# already holds equal to every other pin location.
set -euo pipefail
cd "$(dirname "$0")/.."

image=$(grep -oE 'ghcr\.io/carolsimone/continuo-python-runtime-postgres:v[0-9]+\.[0-9]+\.[0-9]+' docker-compose.yml | head -1)
if [ -z "$image" ]; then
  echo "ERROR: no continuo-python-runtime-postgres pin found in docker-compose.yml" >&2
  exit 1
fi

docker run --rm \
  -v "$PWD/pkg/streams/contract.yaml:/contract.yaml:ro" \
  --entrypoint python "$image" -c '
import sys, yaml
from continuo_python_runtime.contract.model import KINDS
doc = yaml.safe_load(open("/contract.yaml"))
catalog = {
    v["value"]
    for voc in doc["vocabularies"] if voc["name"] == "node_type"
    for v in voc["values"] if v.get("runtime") == "python"
}
if set(KINDS) != catalog:
    sys.stderr.write(f"runtime KINDS {sorted(KINDS)} != catalog python node types {sorted(catalog)}\n")
    sys.exit(1)
'
echo "python node kinds match $image"
