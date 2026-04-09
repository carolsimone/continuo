# dbt

Dockerized dbt (DuckDB) services, each running isolated dbt models as containerized jobs.

## Structure

```
base/                   # Shared Docker base image (Python 3.12 + dbt-duckdb)
services/
  service-1/            # models: table_a, table_b, table_c (level 0)
  service-2/            # models: table_g, table_h (level 2)
  service-3/            # models: table_d, table_e, table_f, table_i, table_j (levels 1 and 3)
  service-3-broken/     # intentionally failing table_e (used by the e2e failure-path test)
```

## How it works

Each service has its own `Dockerfile`, `dbt_project.yml`, and `profiles.yml`. The shared base image (`dbt-base`) provides the entrypoint, which:

1. Logs the job parameters: `schedule_name`, `table_name`, `schema_name`, `job_name`, `service_name`
2. Runs `dbt run --select $TABLE_NAME` against a local DuckDB file at `/tmp/dev.duckdb`

Each k8s Job pod starts with a fresh DuckDB file. Models use direct `SELECT … FROM a JOIN b USING (id)` SQL so that sqlglot can extract real cross-service upstream dependencies when the manifest-controller resolves the graph.

### Model metadata

Every service's `dbt_project.yml` declares the required `+tags` (schedule name) and `+meta` (`owner`, `criticality`) under the `models:` key, and `profiles.yml` sets `schema: e2e_schema`. These fields are embedded in `manifest.json` by `dbt compile` and consumed by the manifest-controller parser.

### Regenerating `manifest.json`

Whenever a model's SQL or project config changes, regenerate the compiled manifest:

```bash
# From the repo root — runs dbt compile inside the dbt-base image
docker run --rm \
  -v "$(pwd)/dbt/services/service-1:/project" \
  dbt-base:latest \
  bash -c "cd /project && dbt compile --profiles-dir ."
cp dbt/services/service-1/target/manifest.json dbt/services/service-1/manifest.json
```

Repeat for `service-2`, `service-3`, etc. The `manifest.json` files checked into the repo are the authoritative compiled output used by the manifest-controller at runtime.

## Building

Build the base image first (required once before building any service):

```bash
DOCKER_BUILDKIT=1 docker build -t dbt-base:latest ./base
```

Then build individual services:

```bash
DOCKER_BUILDKIT=1 docker build -t service-1:latest ./services/service-1
DOCKER_BUILDKIT=1 docker build -t service-2:latest ./services/service-2
DOCKER_BUILDKIT=1 docker build -t service-3:latest ./services/service-3
DOCKER_BUILDKIT=1 docker build -t service-3-broken:latest ./services/service-3-broken
```

## Running a service

```bash
docker run --rm \
  -e TABLE_NAME=table_a \
  -e SCHEDULE_NAME=my-schedule \
  -e SCHEMA=my_schema \
  -e JOB_NAME=svc1-my-schema-table-a \
  -e SERVICE_NAME=service-1 \
  service-1:latest
```

## Requirements

- Docker
- `TABLE_NAME` env var (required)
- `SCHEDULE_NAME`, `SCHEMA`, `JOB_NAME`, `SERVICE_NAME` (logged by entrypoint; checked by e2e tests)

## service-3-broken

`service-3-broken` is a copy of `service-3` whose `table_e` model raises a dbt compiler error:

```sql
{{ exceptions.raise_compiler_error("intentional failure: service-3-broken table_e") }}
```

This causes `dbt run --select table_e` to exit non-zero on every run, which the e2e failure-path test uses to verify that continuo correctly exhausts retries and drains the downstream DAG.
