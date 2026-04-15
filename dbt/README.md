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
    service-3-broken/       # intentionally failing table_e (e2e failure-path test)
  tests/
    test_config.py           # Unit tests for config module
    test_compile.py          # Unit tests for compile module
    test_upload_unit.py      # Unit tests for upload module
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

```bash
# Compile all services in a directory
python -m dbt_upload compile --services-dir ./services

# Compile specific services
python -m dbt_upload compile ./services/service-1 ./services/service-3

# Upload already-compiled manifests to localstack (default target)
python -m dbt_upload upload --services-dir ./services

# Upload to hetzner
python -m dbt_upload upload --services-dir ./services --target hetzner

# Compile + upload in one step (most common)
python -m dbt_upload load --services-dir ./services

# Override the S3 env prefix
python -m dbt_upload load --services-dir ./services --target hetzner --env staging
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
# Via docker-compose (runs automatically with e2e profile)
docker compose --profile e2e up dbt-compile-and-load

# Or manually inside the container
docker run --rm --network continuo_default \
  -e DBT_POSTGRES_HOST=postgres \
  -e DBT_POSTGRES_PORT=5432 \
  -e DBT_POSTGRES_DB=continuo_dbt \
  -e DBT_POSTGRES_USER=continuo_svc \
  -e DBT_POSTGRES_PASSWORD=continuo \
  -v "$(pwd)/dbt/services:/app/services" \
  dbt-compile-and-load:latest
```

### Targeting Hetzner Object Storage

1. Create `dbt/.env.hetzner` (gitignored):

```
AWS_ACCESS_KEY_ID=<your-hetzner-access-key>
AWS_SECRET_ACCESS_KEY=<your-hetzner-secret-key>
```

2. Run with `--target hetzner` and `--env-file`:

```bash
docker run --rm --network continuo_default \
  --env-file dbt/.env.hetzner \
  -e DBT_POSTGRES_HOST=postgres \
  -e DBT_POSTGRES_PORT=5432 \
  -e DBT_POSTGRES_DB=continuo_dbt \
  -e DBT_POSTGRES_USER=continuo_svc \
  -e DBT_POSTGRES_PASSWORD=continuo \
  -v "$(pwd)/dbt/services:/app/services" \
  dbt-compile-and-load:latest load --services-dir ./services --target hetzner
```

Manifests are uploaded to `s3://continuo-dev/dev/manifest/<service-name>/manifest.json`.

### S3 Key Structure

```
s3://<bucket>/<env>/manifest/<service-name>/manifest.json
```

Example keys for localstack:
```
s3://continuo/local/manifest/service-1/manifest.json
s3://continuo/local/manifest/service-2/manifest.json
s3://continuo/local/manifest/service-3/manifest.json
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
cd dbt && uv run pytest tests/test_config.py tests/test_compile.py tests/test_upload_unit.py tests/test_cli.py -v

# Integration tests (requires Docker + localstack + PostgreSQL)
docker run --rm --network <compose-network> --workdir /app \
  --entrypoint "" \
  -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test \
  -e DBT_POSTGRES_HOST=postgres -e DBT_POSTGRES_PORT=5432 \
  -e DBT_POSTGRES_DB=continuo_dbt -e DBT_POSTGRES_USER=continuo_svc \
  -e DBT_POSTGRES_PASSWORD=continuo \
  -v "$(pwd)/dbt/services:/app/services" \
  dbt-compile-and-load:latest \
  sh -c "uv sync --frozen --extra dev && uv run pytest tests/ -v"
```

## service-3-broken

`service-3-broken` is a copy of `service-3` whose `table_e` model references a nonexistent table (`public.wrong_table`). This causes `dbt run --select table_e` to fail at runtime, which the e2e failure-path test uses to verify that continuo correctly exhausts retries and drains the downstream DAG.
