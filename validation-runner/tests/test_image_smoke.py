r"""Tier-4 image smoke tests: a built slim engine image fails structurally.

Needs the image built first, e.g.:
    docker build -t continuo-validation-runner-postgres:dev \
        -f validation-runner/Dockerfile.postgres validation-runner
Select the tag under test with VALIDATION_IMAGE_UNDER_TEST, and the engine it bakes
with VALIDATION_IMAGE_ENGINE / VALIDATION_IMAGE_REQUIRED_ENV (the adapter's first
required connection var), so the same tests cover every engine image.
"""
import os
import subprocess

import pytest
from continuo_validation_contract import result

IMAGE = os.environ.get("VALIDATION_IMAGE_UNDER_TEST", "continuo-validation-runner-postgres:dev")
ENGINE = os.environ.get("VALIDATION_IMAGE_ENGINE", "postgres")
REQUIRED_ENV = os.environ.get("VALIDATION_IMAGE_REQUIRED_ENV", "POSTGRES_HOST")


def _run(env: dict) -> subprocess.CompletedProcess:
    args = ["docker", "run", "--rm"]
    for k, v in env.items():
        args += ["-e", f"{k}={v}"]
    return subprocess.run(args + [IMAGE], capture_output=True, text=True, timeout=120)


@pytest.mark.image
def test_no_env_exits_2_with_structured_block():
    """No env at all: the runner exits 2, naming the first missing var on stderr."""
    proc = _run({})
    assert proc.returncode == 2
    assert "missing required env var DBT_TARGET_SCHEMA" in proc.stderr


@pytest.mark.image
def test_discovery_and_required_env_produce_structured_block():
    """The baked adapter is discovered; its missing connection env fails cleanly."""
    proc = _run({
        "DBT_TARGET_SCHEMA": "_candidate_smoke",
        "TABLE_NAME": "t",
        "VALIDATION_OP": "clone_from_prod",
        "PROD_SCHEMA": "analytics",
    })
    # Discovery loaded exactly one adapter; its required env is absent, so the
    # runner emits a structured block naming the missing vars and exits 2.
    assert proc.returncode == 2
    assert result.SENTINEL_BEGIN in proc.stdout
    assert REQUIRED_ENV in proc.stdout
    assert ENGINE in proc.stdout
