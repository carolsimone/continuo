"""Integration tests that pin the compile-pod parse-rehearsal markers against a
REAL dbt (not mocks). These tests exist to empirically prove three assumptions
that executor-controller's buildParseExportCommand (adapters/k8s/client.go)
hard-codes as shell logic for the rehearsal initContainer:

  1. A second `dbt parse` into the same --target-path hits the partial-parse
     cache (does not reprint "Unable to do partial parsing") once the first
     run has written partial_parse.msgpack.
  2. An env_var() read at parse time that changes between the two runs (the
     real-world case: DBT_TARGET_SCHEMA differing between the compile pod's
     prod and candidate rehearsal legs) invalidates the cache and DOES print
     "Unable to do partial parsing" on the second run.
  3. The DEPLOYED s3-sidecar scripts (/compile_uploader.py, /parse_cache_fetcher.py)
     round-trip a partial_parse.msgpack byte-for-byte through S3.

If any of these fail against the pinned dbt version (dbt-core==1.12.0b1,
dbt-postgres==1.10.0), Task 5's shell script markers are wrong and must be
corrected — this suite is the empirical source of truth for that shell logic,
not the other way around.

Run inside the dbt-compile-and-load image (bakes the pinned dbt + the
deployed s3-sidecar scripts at /compile_uploader.py, /parse_cache_fetcher.py,
and the shared macros at /project/macros):

  docker run --rm -v $(pwd)/dbt:/app -w /app --network <net> \
    -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test \
    -e AWS_DEFAULT_REGION=us-east-1 -e S3_ENDPOINT_URL=http://<localstack>:4566 \
    -e S3_BUCKET=continuo -e S3_ENV=local \
    <dbt-compile-and-load image> uv run --with pytest pytest tests/test_parse_rehearsal.py -v -m integration
"""
import os
import shutil
import subprocess
import sys

import boto3
import pytest

PARTIAL_PARSE_MARKER = "Unable to do partial parsing"

SERVICE_FIXTURE_DIR = "/app/services/service-1"
# Baked into every dbt project image (dbt-base's Dockerfile COPYs macros/ here);
# dbt-compile-and-load inherits FROM dbt-base so this path exists even though
# its own WORKDIR is /app. Copying from here (not from a live-mounted
# dbt/base/macros path) means the fixture project picks up the exact macro
# content the real service images ship, including generate_schema_name's
# DBT_TARGET_SCHEMA env_var() read.
BASE_MACROS_DIR = "/project/macros"

S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://localstack:4566")
S3_BUCKET = os.getenv("S3_BUCKET", "continuo")


def _fixture_project(tmp_path):
    """Copy the service-1 fixture project + the real generate_schema_name
    macro into an isolated tmp dir, replicating the production project layout
    (dbt_project.yml/profiles.yml/models/seeds from the service image, macros/
    from the base image) without mutating the shared fixture under
    /app/services."""
    project_dir = tmp_path / "project"
    shutil.copytree(SERVICE_FIXTURE_DIR, project_dir, ignore=shutil.ignore_patterns("target"))
    shutil.copytree(BASE_MACROS_DIR, project_dir / "macros")
    return project_dir


def _run_parse(project_dir, target_path, env):
    result = subprocess.run(
        ["dbt", "parse", "--profiles-dir", ".", "--target-path", str(target_path)],
        cwd=str(project_dir),
        capture_output=True,
        text=True,
        env=env,
    )
    return result


def _base_env():
    env = dict(os.environ)
    env.setdefault("DBT_POSTGRES_HOST", "postgres")
    env.setdefault("DBT_POSTGRES_PORT", "5432")
    env.setdefault("DBT_POSTGRES_DB", "continuo_dbt")
    env.setdefault("DBT_POSTGRES_USER", "continuo_svc")
    env.setdefault("DBT_POSTGRES_PASSWORD", "continuo")
    # Parse-time env_var() reads must resolve; the values themselves are
    # irrelevant since `dbt parse` never opens a connection.
    return env


@pytest.mark.integration
def test_second_parse_hits_cache(tmp_path):
    """Pins invariant 1: a same-env second `dbt parse` into the same
    --target-path hits the partial-parse cache written by the first run."""
    project_dir = _fixture_project(tmp_path)
    target = tmp_path / "target"
    env = _base_env()

    run1 = _run_parse(project_dir, target, env)
    assert run1.returncode == 0, f"run1 dbt parse failed:\nstdout={run1.stdout}\nstderr={run1.stderr}"
    partial_parse_path = target / "partial_parse.msgpack"
    assert partial_parse_path.exists(), (
        "run1 did not write partial_parse.msgpack — either partial parsing is "
        "disabled for this dbt version/project or the fixture project setup is wrong"
    )

    run2 = _run_parse(project_dir, target, env)
    assert run2.returncode == 0, f"run2 dbt parse failed:\nstdout={run2.stdout}\nstderr={run2.stderr}"
    combined = run2.stdout + run2.stderr
    assert PARTIAL_PARSE_MARKER not in combined, (
        f"run2 unexpectedly reprinted the partial-parse-miss marker with an "
        f"unchanged env; Task 5's cache-hit assumption is WRONG.\ncombined output:\n{combined}"
    )


@pytest.mark.integration
def test_env_change_invalidates_cache(tmp_path):
    """Pins invariant 2: changing DBT_TARGET_SCHEMA between two parses into the
    same --target-path (the real prod-vs-candidate rehearsal condition —
    generate_schema_name.sql reads it via env_var() at parse time) invalidates
    dbt's partial-parse cache and reprints the marker Task 5 greps for."""
    project_dir = _fixture_project(tmp_path)
    target = tmp_path / "target"
    env = _base_env()

    run1 = _run_parse(project_dir, target, env)
    assert run1.returncode == 0, f"run1 dbt parse failed:\nstdout={run1.stdout}\nstderr={run1.stderr}"
    assert (target / "partial_parse.msgpack").exists()

    env2 = dict(env)
    env2["DBT_TARGET_SCHEMA"] = "_candidate_x"
    run2 = _run_parse(project_dir, target, env2)
    # dbt still exits 0 even when it falls back to a full (non-partial) parse.
    assert run2.returncode == 0, f"run2 dbt parse failed:\nstdout={run2.stdout}\nstderr={run2.stderr}"
    combined = run2.stdout + run2.stderr
    assert PARTIAL_PARSE_MARKER in combined, (
        f"run2 (with DBT_TARGET_SCHEMA changed) did NOT print the partial-parse-miss "
        f"marker; Task 5's cache-invalidation assumption is WRONG.\ncombined output:\n{combined}"
    )


@pytest.mark.integration
def test_fetcher_round_trip_through_deployed_script():
    """Pins invariant 3: the DEPLOYED /compile_uploader.py and
    /parse_cache_fetcher.py round-trip a partial_parse.msgpack byte-for-byte
    through real S3 (localstack), and the fetcher's termination message is
    exactly "hydrated" on success."""
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )
    try:
        s3.create_bucket(Bucket=S3_BUCKET)
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass
    except Exception:
        pass

    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        manifest_path = os.path.join(tmp, "manifest.json")
        with open(manifest_path, "w") as f:
            f.write('{"nodes": {}}')

        msgpack_bytes = b"\x81\xa4test\xa9partial_parse_payload_bytes_1234567890"
        local_msgpack = os.path.join(tmp, "partial_parse.msgpack")
        with open(local_msgpack, "wb") as f:
            f.write(msgpack_bytes)

        release_id = "test-rel-fetcher-roundtrip"
        manifest_key = f"service-1/{release_id}/manifest.json"
        parse_key = f"service-1/parse-cache/test-tag-fetcher/partial_parse.msgpack"
        parse_uri = f"s3://{S3_BUCKET}/{parse_key}"

        upload_env = {
            **os.environ,
            "COMPILE_MANIFEST_PATH": manifest_path,
            "MANIFEST_S3_URI": f"s3://{S3_BUCKET}/{manifest_key}",
            "PARSE_PROD_LOCAL_PATH": local_msgpack,
            "PARSE_PROD_S3_URI": parse_uri,
            "S3_ENDPOINT_URL": S3_ENDPOINT,
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "test"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        }
        uploaded = subprocess.run(
            [sys.executable, "/compile_uploader.py"], capture_output=True, text=True, env=upload_env,
        )
        assert uploaded.returncode == 0, f"compile_uploader failed:\n{uploaded.stderr}"

        dest = os.path.join(tmp, "hydrated_partial_parse.msgpack")
        term_log = os.path.join(tmp, "termination-log")
        fetch_env = {
            **os.environ,
            "PARSE_CACHE_S3_URI": parse_uri,
            "PARSE_CACHE_DEST": dest,
            "TERMINATION_LOG_PATH": term_log,
            "S3_ENDPOINT_URL": S3_ENDPOINT,
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "test"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        }
        fetched = subprocess.run(
            [sys.executable, "/parse_cache_fetcher.py"], capture_output=True, text=True, env=fetch_env,
        )
        assert fetched.returncode == 0, f"parse_cache_fetcher failed:\n{fetched.stderr}"

        with open(dest, "rb") as f:
            got_bytes = f.read()
        assert got_bytes == msgpack_bytes, "fetched msgpack bytes differ from the uploaded original"

        with open(term_log) as f:
            termination = f.read()
        assert termination == "hydrated", f"expected termination message 'hydrated', got {termination!r}"
