# dbt

Dockerized dbt (DuckDB) services, each running isolated dbt models as containerized jobs.

## Structure

```
dbt/
  dbt_upload/               # Python package: compile + upload CLI
    __init__.py
    __main__.py              # Entry point: python -m dbt_upload
    cli.py                   # Argparse: compile, upload, load subcommands
    config.py                # Target profile loading, service dir resolution
    compile.py               # dbt compile wrapper
    upload.py                # Manifest filtering + S3 upload
  targets.yaml              # Named S3 target profiles (localstack, hetzner)
  .env.hetzner              # Hetzner credentials (gitignored)
  Dockerfile.upload         # Image for dbt-compile-and-load service
  base/                     # Shared Docker base image (Python 3.12 + dbt-duckdb)
  services/
    service-1/              # models: table_a, table_b, table_c
    service-2/              # models: table_g, table_h
    service-3/              # models: table_d, table_e, table_f, table_i, table_j
  tests/
    test_config.py           # Unit tests for config module
    test_compile.py          # Unit tests for compile module
    test_cli.py              # Unit tests for CLI
    test_upload.py           # Integration tests (require Docker + localstack)
```

## dbt_upload CLI

The `dbt_upload` package provides three subcommands for compiling dbt services and uploading manifests to S3.

### Subcommands

| Command   | What it does |
|-----------|-------------|
| `compile` | Run `dbt compile` for each service |
| `upload`  | Filter manifests (keep models + seeds, strip `local_stub`) and upload to S3 |
| `load`    | Compile + upload in one step (primary workflow) |

### Usage

The container starts idle (`tail -f /dev/null`). Use `docker exec` to run any subcommand:

```bash
# Compile + upload (full load)
docker exec dbt-compile-and-load \
  uv run python -m dbt_upload load --services-dir /app/services

# Upload only (skip compile) to hetzner
docker exec dbt-compile-and-load \
  uv run python -m dbt_upload upload --services-dir /app/services --target hetzner

# Compile only
docker exec dbt-compile-and-load \
  uv run python -m dbt_upload compile --services-dir /app/services

# Compile specific services
docker exec dbt-compile-and-load \
  uv run python -m dbt_upload compile /app/services/service-1 /app/services/service-3
```

### S3 Target Profiles

Targets are defined in `targets.yaml`:

```yaml
targets:
  localstack:              # Local development (docker-compose)
    endpoint_url: http://localstack:4566
    bucket: continuo
    region: us-east-1
    env: local
    access_key_id: test
    secret_access_key: test

  hetzner:                 # Hetzner Object Storage (production/dev)
    endpoint_url: https://nbg1.your-objectstorage.com
    bucket: continuo-dev
    region: eu-central-1
    env: dev
    # Credentials come from env vars (see below)
```

**Credential resolution order:**
1. Environment variables `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (highest priority)
2. Values in `targets.yaml`
3. If neither is set, the CLI exits with a clear error

### Targeting LocalStack (local dev)

LocalStack credentials are baked into `targets.yaml`. No extra setup needed:

```bash
# Start the container (it idles until exec'd)
docker compose up -d dbt-compile-and-load

# Then run the load
docker exec dbt-compile-and-load \
  uv run python -m dbt_upload load --services-dir /app/services
```

### Targeting Hetzner Object Storage

1. Create `dbt/.env.hetzner` (gitignored):

```
AWS_ACCESS_KEY_ID=<your-hetzner-access-key>
AWS_SECRET_ACCESS_KEY=<your-hetzner-secret-key>
```

2. Run with `--target hetzner`:

```bash
docker exec --env-file dbt/.env.hetzner \
  dbt-compile-and-load \
  uv run python -m dbt_upload load --services-dir /app/services --target hetzner
```

Manifests are uploaded to `s3://continuo-dev/dev/manifest/<service-name>/manifest_v{N}.json`.

### S3 Key Structure

```
s3://<bucket>/<env>/manifest/<service-name>/manifest_v{N}.json
```

Each upload increments `N` from the highest existing version (1 if none exist). Example keys for localstack after three load runs:

```
s3://continuo/local/manifest/service-1/manifest_v1.json
s3://continuo/local/manifest/service-1/manifest_v2.json
s3://continuo/local/manifest/service-1/manifest_v3.json
```

## How dbt services work

Each service has its own `Dockerfile`, `dbt_project.yml`, and `profiles.yml`. The shared base image (`dbt-base`) provides the entrypoint, which:

1. Logs the job parameters: `schedule_name`, `table_name`, `schema_name`, `job_name`, `service_name`
2. Runs `dbt run --select $TABLE_NAME` against a local DuckDB file at `/tmp/dev.duckdb`

Each k8s Job pod starts with a fresh DuckDB file. Models use direct `SELECT ... FROM a JOIN b USING (id)` SQL so that sqlglot can extract real cross-service upstream dependencies when the manifest-controller resolves the graph.

### Model metadata

Every service's `dbt_project.yml` declares the required `+tags` (schedule name) and `+meta` (`owner`, `criticality`) under the `models:` key, and `profiles.yml` sets `schema: e2e_schema`. These fields are embedded in `manifest.json` by `dbt compile` and consumed by the manifest-controller parser.

## Building

Build the base image first (required once before building any service):

```bash
DOCKER_BUILDKIT=1 docker build -t dbt-base:latest ./base
```

Build the compile-and-load image:

```bash
DOCKER_BUILDKIT=1 docker build -t dbt-compile-and-load:latest -f Dockerfile.upload .
```

Build individual service images:

```bash
DOCKER_BUILDKIT=1 docker build -t service-1:latest ./services/service-1
DOCKER_BUILDKIT=1 docker build -t service-2:latest ./services/service-2
DOCKER_BUILDKIT=1 docker build -t service-3:latest ./services/service-3
```

## Running tests

```bash
# Unit tests (no Docker needed)
cd dbt && uv run pytest tests/test_config.py tests/test_compile.py tests/test_cli.py -v

# Integration tests (requires docker compose up -d localstack postgres dbt-compile-and-load)
docker exec \
  -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test \
  -e AWS_DEFAULT_REGION=us-east-1 \
  -e S3_ENDPOINT_URL=http://localstack:4566 \
  -e S3_BUCKET=continuo -e S3_ENV=local \
  -e DBT_POSTGRES_HOST=postgres -e DBT_POSTGRES_PORT=5432 \
  -e DBT_POSTGRES_DB=continuo_dbt -e DBT_POSTGRES_USER=continuo_svc \
  -e DBT_POSTGRES_PASSWORD=continuo \
  dbt-compile-and-load \
  uv run --with pytest pytest tests/test_upload.py -v
```


## Deploy-time manifest + sidecar publish (production)

Every push to `main` that triggers `.github/workflows/deploy.yml` also
runs a `publish-dbt-manifests` job that:

1. Spins up an ephemeral postgres service container.
2. Installs `dbt-postgres` + the `dbt_upload` tool's deps.
3. Compiles each service via `dbt compile`.
4. Calls `python -m dbt_upload load --target hetzner --services-dir services`
   with `IMAGE_TAG_PER_SERVICE=service-1=<commit-sha>,...` so each
   `service_metadata.json` sidecar pins to the same SHA the
   `build-publish-dbt` job pushed to Docker Hub in the same workflow run.
5. Publishes `update.graph:v1` to the in-cluster Redis via SSH +
   `kubectl exec`, so manifest-controller picks up the fresh manifests
   immediately — no operator action required after deploy.

### Required GitHub repo secrets

| Secret | Source | Used by |
|---|---|---|
| `HETZNER_S3_ACCESS_KEY_ID` | same value as `manifest-controller` pod's `AWS_ACCESS_KEY_ID` env | upload step |
| `HETZNER_S3_SECRET_ACCESS_KEY` | same value as `manifest-controller` pod's `AWS_SECRET_ACCESS_KEY` env | upload step |
| `HETZNER_HOST` | (already exists, used by the `deploy` job) | SSH-tunnel Redis publish |
| `HETZNER_SSH_KEY` | (already exists) | same |

### Reproducing the deploy-time upload locally

If the CI step fails, reproduce the upload portion locally before
pushing a fix.

Prerequisites:

- Local `dbt-compile-and-load` container running (per `docker-compose.yml`).
- AWS credentials for Hetzner Object Storage exported in your shell:

```bash
export AWS_ACCESS_KEY_ID=...   # = HETZNER_S3_ACCESS_KEY_ID GHA secret
export AWS_SECRET_ACCESS_KEY=...
```

Run:

```bash
COMMIT_SHA=$(git rev-parse HEAD)
docker exec \
  -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  -e IMAGE_TAG_PER_SERVICE="service-1=${COMMIT_SHA},service-2=${COMMIT_SHA},service-3=${COMMIT_SHA}" \
  dbt-compile-and-load \
  uv run python -m dbt_upload load --target hetzner --services-dir /app/services
```

Expected: `Load done: 3 compiled, 3 uploaded, 0 failed` plus six
`Uploaded ...` lines (3 manifests + 3 sidecars).

### Reduced `publish-dbt-images.yml`

The legacy `publish-dbt-images.yml` workflow is now `workflow_dispatch`
only; it builds dbt images at `:latest` (no SHA) as a manual safety
valve. The default path for dbt image builds is `deploy.yml`'s
`build-publish-dbt` job, which produces both `:latest` and
`:${{ github.sha }}`.
