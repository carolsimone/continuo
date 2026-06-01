import os

from streams_contract import (
    RELEASE_REQUESTED_V1,
    MANIFEST_LOADED_CANDIDATE_V1,
    MANIFEST_CONTROLLER_RELEASE_REQUESTED,
)

REDIS_URL       = os.environ.get("REDIS_URL", "")

S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "")
S3_BUCKET       = os.environ.get("S3_BUCKET", "")
S3_ENV          = os.environ.get("S3_ENV", "")

AWS_DEFAULT_REGION = os.environ.get("AWS_DEFAULT_REGION", "")

# Candidate-parse flow: release.requested:v1 → manifest.loaded.candidate:v1.
RELEASE_REQUESTED_STREAM         = RELEASE_REQUESTED_V1
RELEASE_REQUESTED_GROUP          = MANIFEST_CONTROLLER_RELEASE_REQUESTED
MANIFEST_LOADED_CANDIDATE_STREAM = MANIFEST_LOADED_CANDIDATE_V1

_REQUIRED = [
    "REDIS_URL",
    "S3_ENDPOINT_URL", "S3_BUCKET", "S3_ENV", "AWS_DEFAULT_REGION",
]


def validate() -> None:
    """Raise RuntimeError listing all missing required env vars."""
    missing = [key for key in _REQUIRED if not os.environ.get(key)]
    if missing:
        raise RuntimeError(f"missing required env vars: {', '.join(missing)}")
