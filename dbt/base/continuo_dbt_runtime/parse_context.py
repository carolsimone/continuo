"""The canonical parse context digest for a dbt runtime manifest artifact.

The digest answers one question: was this artifact parsed under the same
conditions the process asking for it runs under? It is computed once in the pod
that compiles a release and recomputed in every worker pod that hydrates the
resulting artifact; a worker whose digest differs refuses to serve the artifact.

Both sides import this module, so the two computations cannot drift.
"""
import hashlib
import json
import os

# Environment variables hashed into the digest.
#
# Membership rule: a key belongs here only if it changes what dbt parses AND is
# provably identical in the compile pod and the worker pod of the same release.
# The digest is recomputed at worker startup and hard-fails the pod with no
# fallback, so admitting a key that legitimately differs between the two pods
# would strand the pool permanently unready.
#
# Both pods draw these from the same sources: the compile initContainer and the
# worker Deployment are built by executor-controller from its own process
# environment, and both run the same team image (<service>:<image_tag>), so any
# value baked into the image is identical on both sides.
#
#   DBT_POSTGRES_DB   Names the warehouse database, which dbt resolves into the
#                     relation names it writes into the manifest. Both pod specs
#                     set it from the controller's DBT_POSTGRES_DB.
#   DBT_PROFILES_DIR  Selects the profiles.yml a parse reads. Neither pod spec
#                     sets it, so it resolves from the shared team image.
#   DBT_PROJECT_DIR   Selects the dbt_project.yml a parse reads. Neither pod spec
#                     sets it, so it resolves from the shared team image.
#   DBT_TARGET        Selects the profile target a parse resolves against.
#                     Neither pod spec sets it, so it resolves from the shared
#                     team image.
#
# Deliberately excluded:
#   DBT_TARGET_SCHEMA        Diverges by design — a compile runs against the
#                            release's candidate schema while the worker it
#                            feeds runs against the production schema.
#   DBT_POSTGRES_HOST/PORT/  Connection credentials. They do not change what dbt
#   USER/PASSWORD            parses, and hashing them would strand every pool on
#                            a routine credential rotation.
PARSE_CONTEXT_ENV_KEYS: tuple[str, ...] = (
    "DBT_POSTGRES_DB",
    "DBT_PROFILES_DIR",
    "DBT_PROJECT_DIR",
    "DBT_TARGET",
)


def _file_hash(value) -> dict[str, str]:
    return {"name": value.name, "checksum": value.checksum}


def parse_context_sha256(manifest, controller_context: str) -> str:
    """Digest the conditions manifest was parsed under.

    controller_context is the controller's canonical JSON description of the
    service's resolved command surface and target. Environment values are
    hashed individually, never carried, so the digest is safe to publish.
    """
    controller = json.loads(controller_context)
    state = manifest.state_check
    payload = {
        "controller": controller,
        "state_check": {
            "vars_hash": _file_hash(state.vars_hash),
            "project_env_vars_hash": _file_hash(state.project_env_vars_hash),
            "profile_env_vars_hash": _file_hash(state.profile_env_vars_hash),
            "profile_hash": _file_hash(state.profile_hash),
            "project_hashes": {
                key: _file_hash(value)
                for key, value in sorted(state.project_hashes.items())
            },
        },
        "parse_env_sha256": {
            key: hashlib.sha256(os.environ.get(key, "").encode()).hexdigest()
            for key in PARSE_CONTEXT_ENV_KEYS
        },
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()
