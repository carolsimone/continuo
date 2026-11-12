"""Manifest filtering and S3 upload logic."""
import json
import logging
import os
import shutil
import subprocess

import boto3

logger = logging.getLogger(__name__)


def filter_manifest(service_dir: str) -> None:
    """Keep only the node kinds topology-controller reads.

    Retains model, seed, snapshot, test and operation nodes and drops
    local_stub-tagged ones; every other manifest entry (sources, analyses)
    never reaches a topology node. Tests are kept because topology-controller
    emits a dbt-test node for each test of a tracked model, and the seeded
    baseline must carry the same nodes the release candidate does — otherwise
    every test node reads as newly changed against a test-less baseline.
    Operations are kept because each project hook is an operation node that
    topology-controller hashes into the nodes that run with it, so dropping
    them would give the baseline other hashes than the release candidate. The
    fine-grained tracking (a test only counts when its model is tracked, a node
    needs an owner and a schedule tag) still happens downstream in the parser;
    this is only the coarse resource-type gate.
    """
    manifest_path = os.path.join(service_dir, "target", "manifest.json")
    with open(manifest_path) as f:
        manifest = json.load(f)

    manifest["nodes"] = {
        k: v
        for k, v in manifest["nodes"].items()
        if v.get("resource_type") in ("model", "seed", "snapshot", "test", "operation")
        and "local_stub" not in v.get("tags", [])
    }

    with open(manifest_path, "w") as f:
        json.dump(manifest, f)


def upload_manifest(
    s3_client,
    service_dir: str,
    env: str,
    bucket: str,
    release_id: str = "",
) -> bool:
    """Upload target/manifest.json to S3 at the canonical key. Returns True on success.

    Writes to <service_name>/<release_id>/manifest.json — the same path that
    release-controller's Go CanonicalManifestKey function constructs. The
    release_id already namespaces the path, so no version suffix is needed.

    Raises ValueError if release_id is empty.
    """
    if not release_id:
        raise ValueError("release_id is required")

    service_name = os.path.basename(service_dir)
    manifest_path = os.path.join(service_dir, "target", "manifest.json")

    if not os.path.exists(manifest_path):
        logger.error("manifest.json not found at %s", manifest_path)
        return False

    key = f"{service_name}/{release_id}/manifest.json"
    try:
        s3_client.upload_file(manifest_path, bucket, key)
    except Exception:
        logger.exception("S3 upload failed for %s", service_name)
        return False
    logger.info("Uploaded %s -> s3://%s/%s", service_name, bucket, key)
    return True


COMPILE_EXTRAS_SCRIPT = "/compile_extras.py"


def dbt_interpreter() -> str:
    """The Python that runs dbt: the interpreter named by the shebang of the
    `dbt` console script on PATH (the program after `env` when the shebang
    goes through it).

    compile_extras.py reads the installed dbt versions and PyYAML, which live
    with dbt and not in this package's environment. Raises RuntimeError when
    no `dbt` is on PATH or its first line is not a shebang.
    """
    dbt = shutil.which("dbt")
    if dbt is None:
        raise RuntimeError("no dbt executable on PATH")
    with open(dbt, "rb") as f:
        first_line = f.readline().decode("utf-8", errors="replace").strip()
    if not first_line.startswith("#!"):
        raise RuntimeError(f"{dbt} has no shebang line")
    words = first_line[2:].split()
    if words and os.path.basename(words[0]) == "env":
        words = words[1:]
    if not words:
        raise RuntimeError(f"{dbt} has an empty shebang line")
    return words[0]


def write_compile_extras(service_dir: str, release_id: str) -> str:
    """Write target/compile-extras.json for a compiled service and return its path.

    Runs compile_extras.py with the interpreter that runs dbt, over the
    service's target/manifest.json and its dbt project directory. Raises
    subprocess.CalledProcessError when the script fails.
    """
    service_name = os.path.basename(service_dir)
    target_dir = os.path.join(service_dir, "target")
    out = os.path.join(target_dir, "compile-extras.json")
    subprocess.run(
        [
            dbt_interpreter(), COMPILE_EXTRAS_SCRIPT,
            "--manifest", os.path.join(target_dir, "manifest.json"),
            "--project-dir", service_dir,
            "--service", service_name,
            "--release-id", release_id,
            "--out", out,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return out


def upload_compile_extras(s3_client, service_dir: str, bucket: str, release_id: str) -> bool:
    """Upload target/compile-extras.json to <service_name>/<release_id>/compile-extras.json,
    beside the manifest, where topology-controller reads it. Returns True on success."""
    service_name = os.path.basename(service_dir)
    extras_path = os.path.join(service_dir, "target", "compile-extras.json")
    key = f"{service_name}/{release_id}/compile-extras.json"
    try:
        s3_client.upload_file(extras_path, bucket, key)
    except Exception:
        logger.exception("S3 upload of compile extras failed for %s", service_name)
        return False
    logger.info("Uploaded %s compile extras -> s3://%s/%s", service_name, bucket, key)
    return True


def upload_services(
    service_dirs: list[str], target_config: dict, release_id: str = ""
) -> tuple[list[str], list[str]]:
    """Filter and upload manifests for each service directory, each with its
    compile extras.

    Manifests are uploaded to the canonical per-release key
    <service_name>/<release_id>/manifest.json, and the compile-extras.json
    written from each one beside it. A service whose manifest or compile
    extras cannot be written or uploaded is failed. release_id must be
    non-empty; upload_manifest raises ValueError otherwise.

    Returns (succeeded_dirs, failed_dirs).
    """
    s3_client = boto3.client(
        "s3",
        endpoint_url=target_config["endpoint_url"],
        aws_access_key_id=target_config["access_key_id"],
        aws_secret_access_key=target_config["secret_access_key"],
        region_name=target_config["region"],
    )

    bucket = target_config["bucket"]

    succeeded: list[str] = []
    failed: list[str] = []

    for service_dir in service_dirs:
        service_name = os.path.basename(service_dir)
        logger.info("Uploading %s", service_name)

        try:
            filter_manifest(service_dir)
        except FileNotFoundError:
            logger.error("No compiled manifest for %s", service_name)
            failed.append(service_dir)
            continue

        if not upload_manifest(
            s3_client, service_dir, target_config.get("env", ""), bucket,
            release_id=release_id,
        ):
            failed.append(service_dir)
            continue

        try:
            write_compile_extras(service_dir, release_id)
        except (OSError, RuntimeError, subprocess.CalledProcessError) as exc:
            stderr = getattr(exc, "stderr", None) or ""
            logger.error("Writing compile extras failed for %s: %s %s", service_name, exc, stderr.strip())
            failed.append(service_dir)
            continue

        if upload_compile_extras(s3_client, service_dir, bucket, release_id):
            succeeded.append(service_dir)
        else:
            failed.append(service_dir)

    return succeeded, failed
