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
    test_upload.py           # Manifest filter + S3 key unit tests, plus
                             #   localstack integration tests (@pytest.mark.integration)
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
  -e POSTGRES_HOST=postgres -e POSTGRES_PORT=5432 \
  -e POSTGRES_DB=continuo_dbt -e POSTGRES_USER=continuo_svc \
  -e POSTGRES_PASSWORD=continuo \
  dbt-compile-and-load \
  uv run --with pytest pytest tests/test_upload.py -v
```


## Producing a release for continuo

`dbt_upload` compiles a dbt service and uploads its filtered manifest to the
canonical S3 key `s3://<bucket>/<service>/<release_id>/manifest.json`. Uploading
the manifest is one step of shipping a change into continuo's blue/green
pipeline; it does not, by itself, promote anything. To register the change,
build and push the service image, upload the manifest, then `POST /releases` on
the release-controller HTTP API. Only one object is uploaded per release — the
filtered `manifest.json`. There is no `service_metadata.json` sidecar, and the
image tag travels in the HTTP request body, not in object storage.

The full producer contract — image naming, the canonical manifest key, the
release-controller HTTP API (`GET /current-prod`, `POST /releases`,
`GET /releases/{id}`, `GET /releases`), bootstrap detection, and polling to a
terminal status — is documented in
[`docs/loading-releases.md`](../docs/loading-releases.md).
A runnable reference producer lives at
<https://github.com/carolsimone/continuo-dbt-demo>.
