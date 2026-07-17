# dbt image contract

What a team's dbt image must provide to run under Continuo.
executor-controller launches your image as Kubernetes Jobs for scheduled
runs, seed builds, and release-time compiles; this page is the contract
those Jobs assume. The reference implementation is the
[`continuo-dbt-demo`](https://github.com/carolsimone/continuo-dbt-demo)
repository.

## Image resolution

executor-controller composes your image reference as
`<teamImagePrefix>/<service-name>:<IMAGE_TAG>` — e.g. Docker Hub user +
repository named after the service. `global.teamImagePrefix` in the chart
feeds the prefix; with an empty prefix the reference is the bare
`<service-name>:<tag>` (side-loaded images). The tag is always explicit;
there is no `:latest` fallback.

## What your container must do

The Job runs a command resolved from the chart's `dbt-commands.yaml`
(ConfigMap, see `deploy/continuo/files/dbt-commands.yaml`). The built-in
default is plain dbt — e.g. `dbt run --select <node>` — and a per-service
override block can map every operation to your own wrapper CLI instead.
The contract is **fail-closed**: an override block must define all seven
operations (`run`, `seed`, `snapshot`, `seed_build`, `test`, `build`,
`compile`) or executor-controller refuses to boot. `{{ node }}` and
`{{ target_schema }}` placeholders are substituted at dispatch time.

Your image must therefore contain a working dbt project (or a wrapper that
behaves like one) at the path your commands assume, with a `profiles.yml`
that connects using the environment below.

## Environment your container receives

Database (every Job):

| Variable | Meaning |
|---|---|
| `DBT_POSTGRES_HOST` / `DBT_POSTGRES_PORT` | warehouse Postgres endpoint |
| `DBT_POSTGRES_DB` | warehouse database (`continuo_dbt`) |
| `DBT_POSTGRES_USER` / `DBT_POSTGRES_PASSWORD` | warehouse credentials |

Run context (scheduled runs only): `TASK_ID`, `SCHEDULE_ID`,
`SCHEDULE_NAME`, `SERVICE_NAME`, `SCHEMA`, `TABLE_NAME`, `JOB_NAME`.

Release legs (seed-build) receive `SERVICE_NAME`, `SCHEMA`, `TABLE_NAME`,
`JOB_NAME` — not `TASK_ID`/`SCHEDULE_ID`/`SCHEDULE_NAME` — plus
`RELEASE_ID`, `NODE_ID`, and `DBT_TARGET_SCHEMA` (the candidate schema for
blue/green validation).

Your image never receives S3 credentials. Compile-time manifest upload is
performed by a Continuo-owned sidecar container in the same Job (it reads
the `manifest.json` your compile command produces and uploads it to
`s3://<bucket>/<service>/<release-id>/manifest.json`); your only obligation
is that the `compile` command writes the manifest to the path declared as
`compile.manifest_path` in `dbt-commands.yaml`.

## The `generate_schema_name` macro (required)

Your dbt project MUST route schema resolution through this macro (verbatim
from `dbt/base/macros/generate_schema_name.sql`):

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set override = env_var('DBT_TARGET_SCHEMA', '') -%}
    {%- if override | length > 0 -%}
        {{ override }}
    {%- elif custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

dbt has no `--target-schema` flag, so Continuo's blue/green release
validation passes the candidate schema via `DBT_TARGET_SCHEMA`; with the
macro in place every model materializes there during validation, and when
the variable is unset behavior is byte-identical to a project without the
macro. Without it, validation runs would write into production schemas.

## Base image

`dbt/base` in this repo (`FROM python:3.12-slim`) pins
`dbt-core==1.12.0b1` and `dbt-postgres==1.10.0` (an unpinned install now
resolves to dbt Fusion, which dropped the Postgres adapter), bakes the
macro above into `/project/macros/`, and is what the in-repo e2e fixture
images build from. Real team images may build from it or replicate its
contract on any base — `continuo-dbt-demo` builds `FROM python:3.12-slim`
directly.
