import os

from streams_contract import (
    RELEASE_REQUESTED_V1,
    MANIFEST_LOADED_CANDIDATE_V1,
    TOPOLOGY_CONTROLLER_RELEASE_REQUESTED,
)

REDIS_URL       = os.environ.get("REDIS_URL", "")

# Serves /health and /ready for the k8s liveness/readiness probes (see
# deploy/continuo/templates/deployment.yaml and values.yaml's
# topology-controller.httpPort). Not in _REQUIRED: an operational default is
# fine, unlike REDIS_URL/S3_* which must be explicitly wired per environment.
HTTP_PORT       = int(os.environ.get("HTTP_PORT", "8086"))

S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "")
S3_BUCKET       = os.environ.get("S3_BUCKET", "")
S3_ENV          = os.environ.get("S3_ENV", "")

AWS_DEFAULT_REGION    = os.environ.get("AWS_DEFAULT_REGION", "")
AWS_ACCESS_KEY_ID     = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

# Engine name (as the chart declares it in validation.engine, injected through
# the shared ConfigMap as WAREHOUSE_ENGINE) -> the sqlglot dialect this service
# reads and writes SQL with. Defaults to postgres when unset, which is what an
# install that never chose an engine runs.
#
# An engine missing from this map has no verified sqlglot behaviour here, so
# startup fails rather than silently reading one engine's SQL under another
# engine's rules and emitting the result to the warehouse: the postgres dialect
# renders a cast as CAST(x AS TEXT), which Trino rejects.
_ENGINE_DIALECTS = {
    "postgres": "postgres",
    "trino": "trino",
}


def warehouse_dialect() -> str:
    """Return the sqlglot dialect for the configured warehouse engine.

    Raises RuntimeError for an engine with no dialect mapping. validate() calls
    this at startup so an unsupported engine fails at boot rather than partway
    through a release.
    """
    engine = os.environ.get("WAREHOUSE_ENGINE", "") or "postgres"
    try:
        return _ENGINE_DIALECTS[engine]
    except KeyError:
        raise RuntimeError(
            f"unsupported WAREHOUSE_ENGINE {engine!r}: expected one of "
            f"{', '.join(sorted(_ENGINE_DIALECTS))}"
        ) from None

# Candidate-parse flow: release.requested:v1 → manifest.loaded.candidate:v1.
RELEASE_REQUESTED_STREAM         = RELEASE_REQUESTED_V1
RELEASE_REQUESTED_GROUP          = TOPOLOGY_CONTROLLER_RELEASE_REQUESTED
MANIFEST_LOADED_CANDIDATE_STREAM = MANIFEST_LOADED_CANDIDATE_V1

_REQUIRED = [
    "REDIS_URL",
    "S3_ENDPOINT_URL", "S3_BUCKET", "S3_ENV", "AWS_DEFAULT_REGION",
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
]


def validate() -> None:
    """Raise RuntimeError listing all missing required env vars.

    Also rejects an unsupported WAREHOUSE_ENGINE, which is optional but must
    name an engine this service has a SQL dialect for.
    """
    missing = [key for key in _REQUIRED if not os.environ.get(key)]
    if missing:
        raise RuntimeError(f"missing required env vars: {', '.join(missing)}")
    warehouse_dialect()
