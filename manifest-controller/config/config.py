import os

REDIS_URL       = os.environ.get("REDIS_URL", "")
REDIS_STREAM    = os.environ.get("REDIS_STREAM", "")
REDIS_GROUP     = os.environ.get("REDIS_GROUP", "")
REGISTRY_PATH   = os.environ.get("REGISTRY_PATH", "")
MANIFESTS_BASE  = os.environ.get("MANIFESTS_BASE", "")

S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "")
S3_BUCKET       = os.environ.get("S3_BUCKET", "")
S3_ENV          = os.environ.get("S3_ENV", "")

AWS_DEFAULT_REGION = os.environ.get("AWS_DEFAULT_REGION", "")

SCHEDULES_LOADED_STREAM = os.getenv("SCHEDULES_LOADED_STREAM", "schedules.loaded:v1")
MANIFEST_LOADED_STREAM  = os.getenv("REDIS_OUTPUT_STREAM", "manifest.loaded:v1")

_REQUIRED = [
    "REDIS_URL", "REDIS_STREAM", "REDIS_GROUP",
    "REGISTRY_PATH", "MANIFESTS_BASE",
    "S3_ENDPOINT_URL", "S3_BUCKET", "S3_ENV", "AWS_DEFAULT_REGION",
]


def validate() -> None:
    """Raise RuntimeError listing all missing required env vars."""
    missing = [key for key in _REQUIRED if not os.environ.get(key)]
    if missing:
        raise RuntimeError(f"missing required env vars: {', '.join(missing)}")
