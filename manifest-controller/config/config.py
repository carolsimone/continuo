import os

from streams_contract import (
    UPDATE_GRAPH_V1,
    MANIFEST_LOADED_V1,
    MANIFEST_UPDATE_GRAPH,
)

REDIS_URL       = os.environ.get("REDIS_URL", "")
REDIS_STREAM    = UPDATE_GRAPH_V1
REDIS_GROUP     = MANIFEST_UPDATE_GRAPH
REGISTRY_PATH   = os.environ.get("REGISTRY_PATH", "")
MANIFESTS_BASE  = os.environ.get("MANIFESTS_BASE", "")

S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "")
S3_BUCKET       = os.environ.get("S3_BUCKET", "")
S3_ENV          = os.environ.get("S3_ENV", "")

AWS_DEFAULT_REGION = os.environ.get("AWS_DEFAULT_REGION", "")

MANIFEST_LOADED_STREAM = MANIFEST_LOADED_V1

_REQUIRED = [
    "REDIS_URL",
    "REGISTRY_PATH", "MANIFESTS_BASE",
    "S3_ENDPOINT_URL", "S3_BUCKET", "S3_ENV", "AWS_DEFAULT_REGION",
]


def validate() -> None:
    """Raise RuntimeError listing all missing required env vars."""
    missing = [key for key in _REQUIRED if not os.environ.get(key)]
    if missing:
        raise RuntimeError(f"missing required env vars: {', '.join(missing)}")
