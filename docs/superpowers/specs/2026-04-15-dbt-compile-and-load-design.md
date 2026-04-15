# dbt Compile and Load to Object Storage

**Date:** 2026-04-15
**Branch:** `feat-compile-load-to-s3`

## Goal

Replace the flat `compile_only.py` and `upload_manifests.py` scripts with a modular Python package (`dbt_upload`) that compiles dbt manifests and uploads them to S3-compatible object storage. The user must be able to run the workflow locally from their host (containerized), targeting either LocalStack (dev/CI) or Hetzner Object Storage (production) via a named target profile.

## Non-goals

- Backward compatibility with the old flat scripts.
- Running outside Docker (the scripts always run inside the `dbt-compile-and-load` container).
- Changes to `dbt/services/*`, `dbt/base/*`, `manifest-controller/`, or any other service.

## Package structure

```
dbt/
  dbt_upload/
    __init__.py        # empty
    __main__.py        # entrypoint: delegates to cli
    cli.py             # argparse with subparsers: compile, upload, load
    compile.py         # compile_service(), compile_services()
    upload.py          # filter_manifest(), upload_manifest(), upload_services()
    config.py          # load_target(), resolve_service_dirs()
  targets.yaml         # named S3 target profiles
  .env.hetzner         # gitignored — local Hetzner credentials
  pyproject.toml       # package = dbt_upload, deps: boto3, pyyaml
  Dockerfile.upload    # updated entrypoint
  tests/
    __init__.py
    test_upload.py     # updated imports
```

Old files deleted: `compile_only.py`, `upload_manifests.py`.

## CLI interface

Invoked as `python -m dbt_upload <subcommand>`:

```bash
# Compile specific services (no upload)
python -m dbt_upload compile ./services/service-1 ./services/service-3

# Compile all services in a directory
python -m dbt_upload compile --services-dir ./services

# Upload already-compiled manifests (no compile step)
python -m dbt_upload upload ./services/service-1 --target hetzner

# Compile + filter + upload (primary workflow)
python -m dbt_upload load ./services/service-1 ./services/service-3 --target hetzner

# Compile + upload all services
python -m dbt_upload load --services-dir ./services --target localstack
```

### Arguments

**Common (all subcommands):**
- Positional `paths` — one or more service directories. Mutually exclusive with `--services-dir`.
- `--services-dir` — discover all subdirectories. Mutually exclusive with positional paths.
- At least one of the two is required; error with clear message if neither provided.

**Target-aware (`upload` and `load`):**
- `--target` — name of a profile in `targets.yaml` (default: `localstack`).
- `--env` — S3 key prefix override (e.g. `dev`, `local`). Overrides value from `targets.yaml`.

## Target configuration

### `targets.yaml`

```yaml
targets:
  localstack:
    endpoint_url: http://localstack:4566
    bucket: continuo
    region: us-east-1
    env: local
    access_key_id: test
    secret_access_key: test

  hetzner:
    endpoint_url: https://nbg1.your-objectstorage.com
    bucket: continuo-dev
    region: eu-central-1
    env: dev
```

### Credential resolution

For any target, credentials are resolved in this order:
1. Environment variables `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` — always win.
2. `access_key_id` / `secret_access_key` fields in `targets.yaml` — fallback.

This means:
- LocalStack works out of the box (credentials in YAML).
- Hetzner requires `--env-file .env.hetzner` passed to `docker run`.

### `.env.hetzner` (gitignored)

```
AWS_ACCESS_KEY_ID=<hetzner-access-key>
AWS_SECRET_ACCESS_KEY=<hetzner-secret-key>
```

## Module internals

### `compile.py`

- `compile_service(service_dir: str) -> bool` — runs `dbt compile --profiles-dir .` in the given directory. Returns True on success, logs stderr on failure.
- `compile_services(service_dirs: list[str]) -> tuple[list[str], list[str]]` — compiles each, returns `(succeeded_dirs, failed_dirs)`.

### `upload.py`

- `filter_manifest(service_dir: str) -> None` — reads `target/manifest.json`, keeps only nodes with `resource_type` in `("model", "seed")` and without `"local_stub"` tag. Writes back in place.
- `upload_manifest(s3_client, service_dir: str, env: str, bucket: str) -> bool` — uploads `target/manifest.json` to S3 key `{env}/manifest/{service_name}/manifest.json`. Returns True on success.
- `upload_services(service_dirs: list[str], target_config: dict) -> tuple[list[str], list[str]]` — creates boto3 S3 client from target config, filters + uploads each, returns `(succeeded_dirs, failed_dirs)`.

### `config.py`

- `load_target(targets_path: str, name: str) -> dict` — reads YAML, returns the named target dict with credentials resolved (env vars override YAML). Raises if target name not found.
- `resolve_service_dirs(paths: list[str] | None, services_dir: str | None) -> list[str]` — resolves the mutually exclusive CLI arguments into a sorted list of absolute service directory paths. Raises if neither argument provided.

### `cli.py`

Argparse with subparsers:
- `compile` — calls `resolve_service_dirs()`, then `compile_services()`. Exits non-zero if any fail.
- `upload` — calls `resolve_service_dirs()`, `load_target()`, then `upload_services()`. Exits non-zero if any fail.
- `load` — calls `resolve_service_dirs()`, `load_target()`, `compile_services()`, then `upload_services()` on the succeeded list. Exits non-zero if any fail.

### `__main__.py`

```python
from dbt_upload.cli import main
main()
```

## Dockerfile

```dockerfile
FROM dbt-base:latest
ENTRYPOINT []
WORKDIR /app

RUN pip install uv --quiet

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

COPY dbt_upload/ ./dbt_upload/
COPY targets.yaml .
COPY tests/ ./tests/

ENTRYPOINT ["uv", "run", "python", "-m", "dbt_upload"]
CMD ["load", "--services-dir", "./services"]
```

Subcommands map naturally to docker invocations:

```bash
# Default: compile + upload all to LocalStack
docker run ... dbt-compile-and-load:latest

# Compile + upload to Hetzner
docker run --env-file .env.hetzner ... dbt-compile-and-load:latest load --target hetzner ./services/service-1

# Compile only
docker run ... dbt-compile-and-load:latest compile --services-dir ./services

# Run tests
docker run ... dbt-compile-and-load:latest uv run pytest tests/ -v
```

## Docker Compose

The existing `compile-and-upload` service in `docker-compose.yml` gets renamed to `dbt-compile-and-load`. Same dependencies (localstack healthy, postgres started), same volume mounts. Only the image/service name and entrypoint change.

## Tests

`tests/test_upload.py` is updated with new imports:

```python
from dbt_upload.compile import compile_service
from dbt_upload.upload import upload_manifest, filter_manifest
```

Same test logic:
- `test_dbt_compile_service1_succeeds` — dbt compile works for service-1.
- `test_upload_and_read_back` — compile + upload + read back from S3, validate nodes.
- `test_all_valid_services_upload` — service-1, 2, 3 all compile and upload.
- `test_service3_broken_compile_fails` — broken service correctly fails.

## File changes summary

**New:**
- `dbt/dbt_upload/__init__.py`
- `dbt/dbt_upload/__main__.py`
- `dbt/dbt_upload/cli.py`
- `dbt/dbt_upload/compile.py`
- `dbt/dbt_upload/upload.py`
- `dbt/dbt_upload/config.py`
- `dbt/targets.yaml`
- `dbt/.env.hetzner`

**Modified:**
- `dbt/Dockerfile.upload` — new entrypoint, copies package
- `dbt/pyproject.toml` — package name, add pyyaml dep
- `dbt/tests/test_upload.py` — updated imports
- `.gitignore` — add `.env.hetzner`
- `docker-compose.yml` — rename service to `dbt-compile-and-load`
- `deploy/app/values.yaml` — fix endpoint to `nbg1.your-objectstorage.com`, bucket to `continuo-dev`
- `docs/arch/` — reconcile with new flow

**Deleted:**
- `dbt/compile_only.py`
- `dbt/upload_manifests.py`

## What stays untouched

- `dbt/services/*` — dbt projects unchanged
- `dbt/base/*` — base Docker image unchanged
- `manifest-controller/` — consumes S3 manifests the same way (key format `{env}/manifest/{service_name}/manifest.json` is unchanged)
- All other services
- Existing e2e tests (same flow, container name changes)
