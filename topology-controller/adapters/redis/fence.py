"""The Redis permission fence: in perService mode a service refuses to start
when its Redis login can read keys outside its own.

The fence runs XINFO GROUPS on PERMISSION_PROBE_KEY. Every generated Redis rule
allows that command, so a NOPERM answer can only mean the key lies outside the
login's key patterns, and any other answer (the key's groups, "no such key",
another error reply) means the login reaches it. The probe key lies outside
every pattern of the generated rules, the ones under the continuo: prefix and
the unprefixed stream names alike.
"""

import logging
import time

from redis import Redis
from redis.exceptions import (
    AuthenticationError,
    AuthorizationError,
    ConnectionError as RedisConnectionError,
    NoPermissionError,
    ResponseError,
    TimeoutError as RedisTimeoutError,
)

logger = logging.getLogger(__name__)

PERMISSION_PROBE_KEY = "continuo-permission-probe"

# The REDIS_ACL_MODE values the fence acts on: config.config.redis_acl_mode
# accepts exactly these.
SHARED = "shared"
PER_SERVICE = "perService"

# How long the fence waits for a Redis it cannot reach yet, retrying after a
# delay that starts at _STARTUP_INITIAL_DELAY_S and doubles up to
# _STARTUP_MAX_DELAY_S: the same window the consumer waits for its group, since
# the fence is the first command the process sends.
_STARTUP_TIMEOUT_S = 300.0
_STARTUP_INITIAL_DELAY_S = 1.0
_STARTUP_MAX_DELAY_S = 10.0


class PermissionFenceError(RuntimeError):
    """The login reaches keys outside its own, or the mode is unknown."""


def check_permission_fence(client: Redis, mode: str) -> None:
    """Make sure, in perService mode, that client's login cannot reach keys
    outside its own.

    Returns when Redis answers the probe with NOPERM, and raises
    PermissionFenceError naming the login on any other answer. A Redis it
    cannot reach yet (a refused connection, a timeout, a server still loading)
    is waited out for up to _STARTUP_TIMEOUT_S, after which the connection
    error itself is raised. A refused sign-in is raised at once. In shared
    mode it checks nothing; an unknown mode raises PermissionFenceError.
    """
    if mode == SHARED:
        return
    if mode != PER_SERVICE:
        raise PermissionFenceError(
            f"unknown REDIS_ACL_MODE {mode!r}: expected {SHARED} or {PER_SERVICE}"
        )
    deadline = time.monotonic() + _STARTUP_TIMEOUT_S
    delay = _STARTUP_INITIAL_DELAY_S
    attempt = 0
    while True:
        attempt += 1
        try:
            client.execute_command("XINFO", "GROUPS", PERMISSION_PROBE_KEY)
        except NoPermissionError:
            logger.info("Redis login %s is fenced to its own keys", _login_name(client))
            return
        except ResponseError:
            break
        except (AuthenticationError, AuthorizationError):
            raise
        except (RedisConnectionError, RedisTimeoutError) as exc:
            if time.monotonic() + delay > deadline:
                logger.error(
                    "Redis still unreachable after %.0fs and %d attempts; giving up: %s",
                    _STARTUP_TIMEOUT_S, attempt, exc,
                )
                raise
            logger.warning(
                "Redis not reachable yet (attempt %d), retrying in %.0fs: %s",
                attempt, delay, exc,
            )
            time.sleep(delay)
            delay = min(delay * 2, _STARTUP_MAX_DELAY_S)
            continue
        break
    raise PermissionFenceError(
        f"redis login {_login_name(client)} can read keys outside its own: "
        "apply the rules in deploy/redis-acl-rules.acl"
    )


def _login_name(client: Redis) -> str:
    """The Redis user client signs in as: its username, from its connection
    settings or its credential provider, and "default" when it names none."""
    settings = client.connection_pool.connection_kwargs
    username = settings.get("username")
    provider = settings.get("credential_provider")
    if not username and provider is not None:
        credentials = provider.get_credentials()
        if len(credentials) == 2:
            username = credentials[0]
    return username or "default"
