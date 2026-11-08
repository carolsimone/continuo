"""Guard: the production image installs its dependencies without CPU emulation.

The image is published for linux/amd64 and linux/arm64 from a runner of one
architecture. A build step that executes inside the other architecture's stage
runs under QEMU user-mode emulation, and uv's binaries have segfaulted at start
under QEMU 9 and later. So every step that executes a program runs on the build
platform, and the stage of the target platform only copies files:

- the `uv` and builder stages are `FROM --platform=$BUILDPLATFORM`;
- the builder installs the target platform's wheels into the venv with
  `uv sync --python-platform`, and `--no-build` refuses a package that has no
  wheel for that platform (a wheel built from source would be built for the
  build platform, not the target);
- the runtime stage has no RUN instruction;
- the builder and the runtime stage use one Python image, pinned by digest, so
  the venv's interpreter (`/usr/local/bin/python3`) is the one it was made for;
- Dockerfile.dev, where CI runs the tests, uses the same Python minor version.
"""
import re
from pathlib import Path

SERVICE_ROOT = Path(__file__).parent.parent
DOCKERFILE_PROD = SERVICE_ROOT / "Dockerfile.prod"
DOCKERFILE_DEV = SERVICE_ROOT / "Dockerfile.dev"

_FROM = re.compile(r"^FROM\s+(?P<args>.+?)\s*$", re.M | re.I)
_RUN = re.compile(r"^RUN\s", re.M | re.I)
_PYTHON_IMAGE = re.compile(r"\bpython:(?P<tag>[^\s@]+)(?P<digest>@sha256:[0-9a-f]{64})?")
_PYTHON_MINOR = re.compile(r"^(?P<minor>\d+\.\d+)")


def _instructions(text: str) -> str:
    """The Dockerfile without comment lines, so a comment never satisfies a check."""
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


def _stages(text: str) -> list[tuple[str, str]]:
    """Each stage as (its FROM arguments, its body up to the next FROM)."""
    text = _instructions(text)
    matches = list(_FROM.finditer(text))
    stages = []
    for i, m in enumerate(matches):
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        stages.append((m.group("args"), text[m.end():end]))
    return stages


def _prod_stages() -> list[tuple[str, str]]:
    assert DOCKERFILE_PROD.is_file(), f"missing {DOCKERFILE_PROD}"
    stages = _stages(DOCKERFILE_PROD.read_text())
    assert len(stages) >= 2, "Dockerfile.prod must have a builder and a runtime stage"
    return stages


def _python_image(from_args: str) -> re.Match:
    match = _PYTHON_IMAGE.search(from_args)
    assert match, f"stage `FROM {from_args}` does not use the python image"
    return match


def _builder_stage(stages: list[tuple[str, str]]) -> tuple[str, str]:
    builders = [s for s in stages if re.search(r"\bAS\s+builder\b", s[0], re.I)]
    assert len(builders) == 1, "Dockerfile.prod must have exactly one stage named builder"
    return builders[0]


def test_every_stage_but_the_runtime_runs_on_the_build_platform():
    """The uv and builder stages execute natively, whatever the target is."""
    *build_stages, (runtime_from, _) = _prod_stages()

    offenders = [args for args, _ in build_stages if "--platform=$BUILDPLATFORM" not in args]
    assert not offenders, (
        "These stages run on the target platform, so the non-native half of the "
        "image runs them under QEMU. Add `--platform=$BUILDPLATFORM`:\n  "
        + "\n  ".join(f"FROM {a}" for a in offenders)
    )
    assert "--platform" not in runtime_from, (
        "The runtime stage must be the target platform's image: "
        f"`FROM {runtime_from}` overrides it."
    )


def test_dependency_install_targets_the_target_platform_and_builds_nothing():
    """uv installs the target's wheels and refuses a package with none."""
    _, body = _builder_stage(_prod_stages())
    joined = body.replace("\\\n", " ")
    sync = " ".join(re.findall(r"uv sync[^\n]*", joined))

    assert sync, "the builder stage must install dependencies with `uv sync`"
    assert "--python-platform" in sync, (
        "The builder runs on the build platform, so `uv sync` must name the "
        "target platform with `--python-platform` or it installs the wrong wheels."
    )
    assert "--no-build" in sync, (
        "`uv sync` must pass `--no-build`: a package built from source is built "
        "for the build platform, not for the target platform."
    )
    for target in ("x86_64-manylinux", "aarch64-manylinux"):
        assert target in body, f"the builder stage must map a TARGETARCH to {target}"


def test_the_runtime_stage_only_copies():
    """No RUN in the target platform's stage, so nothing executes emulated."""
    runtime_from, runtime_body = _prod_stages()[-1]

    assert not _RUN.search(runtime_body), (
        f"The runtime stage (`FROM {runtime_from}`) runs a command, which the "
        "non-native half of the image runs under QEMU. Do the work in the builder "
        "stage and COPY its result."
    )


def test_builder_and_runtime_use_one_python_image_pinned_by_digest():
    """The venv's interpreter path and version are those of the runtime image."""
    stages = _prod_stages()
    builder = _python_image(_builder_stage(stages)[0])
    runtime = _python_image(stages[-1][0])

    assert builder.group("digest") and runtime.group("digest"), (
        "Pin the python image by digest in both the builder and the runtime stage."
    )
    assert builder.group(0) == runtime.group(0), (
        "The builder and the runtime stage must name the same python image, "
        f"or the venv targets another interpreter: {builder.group(0)} vs {runtime.group(0)}"
    )


def test_dev_image_tests_the_python_minor_the_prod_image_ships():
    """CI runs the tests in Dockerfile.dev, so it must run the prod image's Python."""
    prod_tag = _python_image(_prod_stages()[-1][0]).group("tag")
    dev_stages = _stages(DOCKERFILE_DEV.read_text())
    dev_tag = _python_image(dev_stages[-1][0]).group("tag")

    prod_minor = _PYTHON_MINOR.match(prod_tag)
    dev_minor = _PYTHON_MINOR.match(dev_tag)
    assert prod_minor and dev_minor, f"python tags must start with a version: {prod_tag}, {dev_tag}"
    assert prod_minor.group("minor") == dev_minor.group("minor"), (
        f"Dockerfile.dev runs Python {dev_minor.group('minor')} but Dockerfile.prod "
        f"ships Python {prod_minor.group('minor')}, so CI never tests what ships."
    )


def test_guard_would_catch_a_regression():
    """The stage parser sees the shapes these checks reject."""
    emulated = (
        "# FROM --platform=$BUILDPLATFORM python:3.14-slim AS builder\n"
        "FROM python:3.14-slim AS builder\n"
        "RUN uv sync --frozen\n"
        "FROM python:3.14-slim\n"
        "RUN useradd nonroot\n"
    )
    stages = _stages(emulated)

    assert len(stages) == 2, "a commented-out FROM must not count as a stage"
    assert "--platform=$BUILDPLATFORM" not in stages[0][0]
    assert _RUN.search(stages[-1][1])
    assert _python_image(stages[-1][0]).group("digest") is None
    assert _python_image("python:3.14-slim@sha256:" + "a" * 64).group("digest")
