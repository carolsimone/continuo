import logging
import os
import re

from config.secrets import read_secret
from streams_contract import (
    RELEASE_REQUESTED_V1,
    MANIFEST_LOADED_CANDIDATE_V2,
    TOPOLOGY_CONTROLLER_RELEASE_REQUESTED,
)

REDIS_URL       = os.environ.get("REDIS_URL", "")

# Names this service as the producer of the entries it writes to Redis, such as
# the dead letters of release.requested messages it cannot process.
SERVICE_NAME    = "topology-controller"

# Serves /health and /ready for the k8s liveness/readiness probes (see
# deploy/continuo/templates/deployment.yaml and values.yaml's
# topology-controller.httpPort). Not in _REQUIRED: an operational default is
# fine, unlike REDIS_URL/S3_* which must be explicitly wired per environment.
HTTP_PORT       = int(os.environ.get("HTTP_PORT", "8086"))

# Serves GET /metrics (Prometheus text format) on a listener of its own, apart
# from the probe port above, so a scrape never shares a socket with the probes.
# METRICS_PORT overrides it; metrics_port() validates the value.
DEFAULT_METRICS_PORT = 9464

S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "")
S3_BUCKET       = os.environ.get("S3_BUCKET", "")
S3_ENV          = os.environ.get("S3_ENV", "")

AWS_DEFAULT_REGION    = os.environ.get("AWS_DEFAULT_REGION", "")

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


# LOG_LEVEL (the chart's global.logLevel, injected through the shared
# ConfigMap) -> the logging level this service logs at, matched without regard
# to case. These are the names every Go service accepts
# (pkg/config.AcceptedLogLevels, in the same order; pkg/streams'
# TestTopologyControllerLogLevelsMatchTheGoServices pins the two). Unset or
# empty is INFO.
_LOG_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warn": logging.WARNING,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


def log_level() -> int:
    """Return the logging level LOG_LEVEL names.

    Raises RuntimeError for a name outside _LOG_LEVELS, so a level this service
    cannot honour stops it at startup rather than logging at another level.
    """
    raw = os.environ.get("LOG_LEVEL", "")
    if not raw:
        return logging.INFO
    try:
        return _LOG_LEVELS[raw.lower()]
    except KeyError:
        raise RuntimeError(
            f"unsupported LOG_LEVEL {raw!r}: expected one of {', '.join(_LOG_LEVELS)}"
        ) from None


def metrics_port() -> int:
    """Return the port METRICS_PORT names, DEFAULT_METRICS_PORT when it is unset or empty.

    A value is accepted only when it is written as ASCII digits and lies in
    1..65535. int() alone would also take "9_464", " 9464" and non-ASCII
    digits, which a port setting never means. Raises RuntimeError for any
    other value, so a port this service cannot bind stops it at startup.
    """
    raw = os.environ.get("METRICS_PORT", "")
    if not raw:
        return DEFAULT_METRICS_PORT
    if re.fullmatch(r"[0-9]+", raw) and 1 <= int(raw) <= 65535:
        return int(raw)
    raise RuntimeError(
        f"unsupported METRICS_PORT {raw!r}: expected a whole number from 1 to 65535"
    )


# REDIS_ACL_MODE (the chart's Redis login mode, injected through the shared
# ConfigMap): in perService mode each service signs in as its own Redis login,
# whose rule fences it to its own keys, and the service refuses to start when
# its login reaches keys outside them (adapters.redis.fence). In shared mode
# every service signs in with the same password and nothing is checked. These
# are the values pkg/config accepts for the Go services. Unset is shared.
REDIS_ACL_MODE_SHARED = "shared"
REDIS_ACL_MODE_PER_SERVICE = "perService"
_REDIS_ACL_MODES = (REDIS_ACL_MODE_SHARED, REDIS_ACL_MODE_PER_SERVICE)


def redis_acl_mode() -> str:
    """Return the REDIS_ACL_MODE this install declares.

    Raises RuntimeError for any other value than the two modes, so a mode this
    service cannot honour stops it at startup instead of skipping the check.
    """
    raw = os.environ.get("REDIS_ACL_MODE", "")
    if not raw:
        return REDIS_ACL_MODE_SHARED
    if raw not in _REDIS_ACL_MODES:
        raise RuntimeError(
            f"unsupported REDIS_ACL_MODE {raw!r}: expected one of {', '.join(_REDIS_ACL_MODES)}"
        )
    return raw


# Candidate-parse flow: release.requested:v1 → manifest.loaded.candidate:v2.
RELEASE_REQUESTED_STREAM         = RELEASE_REQUESTED_V1
RELEASE_REQUESTED_GROUP          = TOPOLOGY_CONTROLLER_RELEASE_REQUESTED
MANIFEST_LOADED_CANDIDATE_STREAM = MANIFEST_LOADED_CANDIDATE_V2

_REQUIRED = [
    "REDIS_URL",
    "S3_ENDPOINT_URL", "S3_BUCKET", "S3_ENV", "AWS_DEFAULT_REGION",
]

# Credentials that must be given, each as the variable or as <NAME>_FILE.
_REQUIRED_SECRETS = [
    "REDIS_PASSWORD",
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
]


def validate() -> None:
    """Raise RuntimeError listing all missing required env vars.

    A credential counts as present when read_secret finds it in the variable
    or in its <NAME>_FILE; a credential given both ways, or as an unreadable or
    empty file, raises read_secret's error. Also rejects a REDIS_URL that
    carries credentials (they come from REDIS_USERNAME and REDIS_PASSWORD), an
    unsupported WAREHOUSE_ENGINE, which is optional but must name an engine
    this service has a SQL dialect for, a LOG_LEVEL that names no level this
    service logs at, a METRICS_PORT that is not a whole number from 1 to 65535,
    and a REDIS_ACL_MODE other than shared or perService.
    """
    missing = [key for key in _REQUIRED if not os.environ.get(key)]
    missing += [key for key in _REQUIRED_SECRETS if not read_secret(key)]
    if missing:
        raise RuntimeError(f"missing required env vars: {', '.join(missing)}")
    if "@" in os.environ["REDIS_URL"]:
        raise RuntimeError(
            "REDIS_URL must not carry credentials; set REDIS_USERNAME and REDIS_PASSWORD"
        )
    warehouse_dialect()
    log_level()
    metrics_port()
    redis_acl_mode()
