"""Guard: the production image installs the dependencies uv.lock resolves.

pyproject.toml and uv.lock are the declared dependency set, and CI tests against
them — the dev image runs `uv sync --frozen`. If Dockerfile.prod names its own
versions instead, the two drift apart silently: dependabot updates the lockfile,
CI goes green against the new versions, and the deployed image keeps installing
the old ones. Nothing fails, so nothing tells you.

That shipped here. The prod image ran sqlglot 29.0.1 while the lockfile pinned
30.17.0, with protobuf 6 against a tested 7 and redis 7 against a tested 8, so
every Python dependency bump had been inert in production and CI had never
exercised what production ran.

This guard pins the two together the way test_supported_engines_match_the_chart
pins the chart's engines to the code's.
"""
import re
from pathlib import Path

SERVICE_ROOT = Path(__file__).parent.parent
DOCKERFILE_PROD = SERVICE_ROOT / "Dockerfile.prod"

# A pinned dependency in a pip/uv install line, e.g. `sqlglot==29.0.1`.
# Ignores base-image tags (`FROM python:3.14-slim`), which are Docker's own
# versioning and are updated by dependabot's docker ecosystem, not uv's.
_PINNED_DEPENDENCY = re.compile(r"^\s*([A-Za-z][A-Za-z0-9._-]*)==([0-9][^\s\\]*)", re.M)


def _dockerfile_prod_text() -> str:
    assert DOCKERFILE_PROD.is_file(), f"missing {DOCKERFILE_PROD}"
    return DOCKERFILE_PROD.read_text()


def test_prod_image_resolves_dependencies_from_the_lockfile():
    """Dockerfile.prod must build its venv from pyproject.toml + uv.lock."""
    text = _dockerfile_prod_text()

    assert "uv sync --frozen" in text, (
        "Dockerfile.prod must install dependencies with `uv sync --frozen` so the "
        "image gets exactly what uv.lock resolves and CI tested."
    )
    for required in ("pyproject.toml", "uv.lock"):
        assert required in text, (
            f"Dockerfile.prod must COPY {required} — `uv sync --frozen` needs it "
            "to resolve the locked dependency set."
        )


def test_prod_image_pins_no_dependency_versions_of_its_own():
    """No `package==version` literals: the lockfile is the only version source."""
    offenders = [
        f"{m.group(1)}=={m.group(2)}"
        for m in _PINNED_DEPENDENCY.finditer(_dockerfile_prod_text())
    ]

    assert not offenders, (
        "Dockerfile.prod pins dependency versions itself, so uv.lock no longer "
        "reaches the deployed image. Install with `uv sync --frozen` instead:\n  "
        + "\n  ".join(offenders)
    )


def test_guard_would_catch_a_regression():
    """The pattern matches the shape it rejects and spares the shapes it must not."""
    assert _PINNED_DEPENDENCY.search("    sqlglot==29.0.1 \\\n")
    assert _PINNED_DEPENDENCY.search("    boto3==1.42.68\n")
    # A base image tag is not a dependency pin.
    assert not _PINNED_DEPENDENCY.search("FROM python:3.14-slim AS builder\n")
    # Neither is a lockfile-driven install.
    assert not _PINNED_DEPENDENCY.search("RUN uv sync --frozen --no-dev\n")
