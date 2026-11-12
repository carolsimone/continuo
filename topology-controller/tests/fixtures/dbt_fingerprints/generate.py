#!/usr/bin/env python3
"""Compile every variant of the fpshop fixture project with real dbt.

For each variant in variants.py it copies project/ to .work/<variant>/, writes the
generated seed seeds/seed_big.csv, applies the variant's edits and runs `dbt compile`
and s3-sidecar/compile_extras.py in the variant's dbt image, attached to the
docker-compose network so dbt reaches the compose `postgres`. The warehouse schema
fp_analytics holds no tables, so `is_incremental()` is false. The results are written
to manifests/<variant>/manifest.json.gz (gzip with no timestamp, so a recompile with
the same output writes the same bytes) and manifests/<variant>/compile-extras.json.

The copies live under the fixture directory, not the system temp directory: colima
shares only the home directory with its VM, so a bind mount of /var/folders/... is
empty inside the container.

Usage: generate.py [--network NAME] [--keep] [VARIANT ...]
With no VARIANT it compiles every variant. --keep leaves .work/<variant>/ in place.
Standard library only.
"""
from __future__ import annotations

import argparse
import gzip
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

FIXTURE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(FIXTURE_DIR))

from variants import VARIANTS, Variant  # noqa: E402

PROJECT_DIR = FIXTURE_DIR / "project"
WORK_DIR = FIXTURE_DIR / ".work"
MANIFESTS_DIR = FIXTURE_DIR / "manifests"
REPO_ROOT = FIXTURE_DIR.parents[3]
COMPILE_EXTRAS = REPO_ROOT / "s3-sidecar" / "compile_extras.py"

SEED_BIG = "seeds/seed_big.csv"
SEED_BIG_ROWS = 50_000
SEED_BIG_MIN_BYTES = 1_048_576

logger = logging.getLogger("dbt_fingerprints.generate")


def seed_big_text() -> str:
    """The generated seed: a header and SEED_BIG_ROWS rows from a fixed formula, over
    1 MiB, so dbt records a path checksum for it instead of a content hash."""
    rows = ["id,label,value"]
    rows.extend(f"{i},row_{i:07d},{(i * 7919) % 100_003}" for i in range(1, SEED_BIG_ROWS + 1))
    text = "\n".join(rows) + "\n"
    if len(text.encode("utf-8")) <= SEED_BIG_MIN_BYTES:
        raise RuntimeError(f"{SEED_BIG} must be over {SEED_BIG_MIN_BYTES} bytes")
    return text


def apply_edit(project: Path, path: str, old: str, new: str) -> None:
    target = project / path
    if old == "":
        if target.exists():
            raise RuntimeError(f"edit creates {path}, which already exists")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(new, encoding="utf-8")
        return
    text = target.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"edit of {path}: {old!r} occurs {count} times, expected once")
    target.write_text(text.replace(old, new), encoding="utf-8")


def make_writable(root: Path) -> None:
    """Open the copy to every user, so the image's dbt user (uid 1000) writes target/ and logs/."""
    for dirpath, dirnames, filenames in os.walk(root):
        os.chmod(dirpath, 0o777)
        for name in filenames:
            os.chmod(os.path.join(dirpath, name), 0o666)


def prepare(name: str, variant: Variant) -> Path:
    copy = WORK_DIR / name
    if copy.exists():
        shutil.rmtree(copy)
    shutil.copytree(PROJECT_DIR, copy)
    (copy / SEED_BIG).write_text(seed_big_text(), encoding="utf-8")
    for path, old, new in variant.edits:
        apply_edit(copy, path, old, new)
    make_writable(copy)
    return copy


def docker_command(name: str, variant: Variant, copy: Path, network: str) -> list[str]:
    script = (
        "dbt compile --profiles-dir /project --target fp"
        " && python3 /extras/compile_extras.py --manifest target/manifest.json --project-dir /project"
        f" --service fpshop --release-id {name} --out target/compile-extras.json"
    )
    if variant.pre_compile:
        script = f"{variant.pre_compile} && {script}"
    cmd = ["docker", "run", "--rm", "--network", network, "--entrypoint", "sh",
           "-v", f"{copy}:/project", "-v", f"{COMPILE_EXTRAS}:/extras/compile_extras.py:ro",
           "-w", "/project", "-e", "DBT_SEND_ANONYMOUS_USAGE_STATS=false"]
    if variant.pre_compile:
        cmd += ["--user", "root"]
    for key, value in sorted(variant.env.items()):
        cmd += ["-e", f"{key}={value}"]
    cmd += [variant.image, "-c", script]
    return cmd


def write_outputs(name: str, copy: Path) -> None:
    out = MANIFESTS_DIR / name
    out.mkdir(parents=True, exist_ok=True)
    manifest = (copy / "target" / "manifest.json").read_bytes()
    with open(out / "manifest.json.gz", "wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0, compresslevel=9) as gz:
            gz.write(manifest)
    shutil.copyfile(copy / "target" / "compile-extras.json", out / "compile-extras.json")


def generate(name: str, network: str, keep: bool) -> None:
    variant = VARIANTS[name]
    logger.info("compiling variant %s", name)
    copy = prepare(name, variant)
    result = subprocess.run(docker_command(name, variant, copy, network),
                            capture_output=True, text=True, check=False)
    if result.returncode != 0:
        logger.error("variant %s failed (exit %d):\n%s%s", name, result.returncode,
                     result.stdout[-6000:], result.stderr[-6000:])
        raise SystemExit(1)
    write_outputs(name, copy)
    if not keep:
        shutil.rmtree(copy)
    logger.info("wrote %s", MANIFESTS_DIR / name)


def default_network() -> str:
    project = os.environ.get("COMPOSE_PROJECT_NAME") or REPO_ROOT.name
    return f"{project}_default"


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Compile the fpshop fixture variants with dbt.")
    parser.add_argument("variants", nargs="*", help="variants to compile (default: all)")
    parser.add_argument("--network", default=default_network(),
                        help="docker network of the compose postgres (default: <compose project>_default)")
    parser.add_argument("--keep", action="store_true", help="keep .work/<variant>/ after the compile")
    args = parser.parse_args(argv)

    names = args.variants or list(VARIANTS)
    unknown = [n for n in names if n not in VARIANTS]
    if unknown:
        parser.error(f"unknown variants: {', '.join(unknown)}")
    for name in names:
        generate(name, args.network, args.keep)
    if WORK_DIR.exists() and not any(WORK_DIR.iterdir()):
        WORK_DIR.rmdir()
    return 0


if __name__ == "__main__":
    sys.exit(main())
