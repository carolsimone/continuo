# dbt image contract

What a team's dbt image must provide to run under continuo.
execution-controller launches your image as Kubernetes Jobs for scheduled
runs, seed builds, and release-time compiles; this page is the contract
those Jobs assume. The reference implementation is the
[`continuo-demo`](https://github.com/carolsimone/continuo-demo)
repository.

For `python-node` nodes, whose images are resolved and configured differently,
see [`python-image-contract.md`](python-image-contract.md).

## Image resolution

execution-controller composes your image reference as
`<teamImagePrefix>/<service-name>:<IMAGE_TAG>` — e.g. Docker Hub user +
repository named after the service. `global.teamImagePrefix` in the chart
feeds the prefix; with an empty prefix the reference is the bare
`<service-name>:<tag>` (side-loaded images). The tag is always explicit;
there is no `:latest` fallback.

## What your container must do

The Job runs a command resolved from the chart's `dbt-commands.yaml`
(ConfigMap, see `deploy/continuo/files/dbt-commands.yaml`). The built-in default
is plain dbt — e.g. `dbt run --select <node>`. You can also override these
commands with your own — say, a CLI that wraps dbt inside an internal tool — so a
non-standard dbt container runs under continuo without change.

The contract is **fail-closed**: an override block must define all eight
operations (`run`, `seed`, `snapshot`, `seed_build`, `test`, `build`,
`compile`, `parse`) or execution-controller refuses to boot. `{{ node }}` and
`{{ target_schema }}` placeholders are substituted at dispatch time.

Your `seed` command is the plain load, plain `dbt seed`, not `--full-refresh`.
It is what execution-controller runs for every automatic, scheduled or
promoted-release seed load. On Postgres it truncates the table and inserts the
rows, or drops the table with cascade and creates it again when the seed sets
`+full_refresh: true`. On Trino it drops the table and creates it again. When a
load drops the table, continuo rebuilds the views and materialized views that
read it in the same run.

Your image should also provide a model-rebuild verb (backs the `full_refresh`
command, e.g. `dbt run --full-refresh --select <node>`) and a seed-rebuild verb
(backs the `seed_full_refresh` command, e.g. `dbt seed --full-refresh --select
<node>`). Unlike the required operations above, `full_refresh` and
`seed_full_refresh` are both optional — a block that omits either still boots —
but a service missing the one an operation needs fails that task with
`full_refresh_undefined` or `seed_full_refresh_undefined` before any container
starts, with no fallback to another block's command. continuo runs them for an
operator's single-node full refresh, for a Postgres seed whose columns change
while it leaves `+full_refresh` unset (`seed_full_refresh`), and for a Trino
materialized view below a reloaded seed (`full_refresh`).

Your image must therefore contain a working dbt project (or a wrapper that
behaves like one) at the path your commands assume, with a `profiles.yml`
that connects using the environment below.

## Environment your container receives

Database (every Job): the operator-owned warehouse Secret
(`validation.warehouseSecret`) is attached to your container verbatim via
`envFrom`, so your `profiles.yml` reads the deployment engine's native keys and
your image bakes that engine's dbt adapter. It carries the company's warehouse
login, never a login of continuo's own services; in the bundled quickstart it
is the chart's `<release>-warehouse-validation`, for the login
`continuo_warehouse`, which owns the warehouse database.

| Variable (postgres engine) | Meaning |
|---|---|
| `POSTGRES_HOST` / `POSTGRES_PORT` | warehouse Postgres endpoint |
| `POSTGRES_DB` | warehouse database (`continuo_dbt`) |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | the warehouse login and its password |

On a non-postgres deployment the Secret carries that engine's keys instead
(e.g. trino: `TRINO_HOST`, `TRINO_CATALOG`, optional `TRINO_PORT`/`TRINO_USER`/
`TRINO_HTTP_SCHEME`/`TRINO_PASSWORD`).

Run context (scheduled runs only): `TASK_ID`, `SCHEDULE_ID`,
`SCHEDULE_NAME`, `SERVICE_NAME`, `SCHEMA`, `TABLE_NAME`, `JOB_NAME`.

Release legs (seed-build) receive `SERVICE_NAME`, `SCHEMA`, `TABLE_NAME`,
`JOB_NAME` — not `TASK_ID`/`SCHEDULE_ID`/`SCHEDULE_NAME` — plus
`RELEASE_ID`, `NODE_ID`, and `DBT_TARGET_SCHEMA` (the candidate schema for
blue/green validation).

Your image never receives S3 credentials. Compile-time manifest upload is
performed by a continuo-owned sidecar container in the same Job (it reads
the `manifest.json` your compile command produces and uploads it to
`s3://<bucket>/<service>/<release-id>/manifest.json`); your only obligation
is that the `compile` command writes the manifest to the path declared as
`compile.manifest_path` in `dbt-commands.yaml`.

## What the compile Job needs from your image

After `dbt compile`, the compile Job runs continuo's `compile_extras.py` with
your image's `python3`, and the sidecar uploads the `compile-extras.json` it
writes beside `manifest.json`. The file carries the inputs a node's code
fingerprint needs and the manifest lacks: raw seed bytes and headers, the
`flags:` block and library versions. So:

- `python3` must be on the image's `PATH`, and must be the Python that runs
  dbt, so that it imports the same dbt packages your project uses. An image
  without `python3` fails its compile.
- The script reads the dbt project the `compile` command reads: the
  command's own `--project-dir` when it has one (a relative one under the
  directory the command runs in), otherwise the directory the command runs in,
  your image's `WORKDIR`. It reads `dbt_project.yml` and the seed files there,
  wherever `compile.manifest_path` puts the manifest.
- Every run command (`run`, `seed`, `snapshot`, `seed_build`, `test`, `build`,
  and the optional `full_refresh` and `seed_full_refresh`) must parse with the
  same `--vars`, `--target`, `--profile`, `--profiles-dir` and `--project-dir`
  values as the `compile` command, because the fingerprint is computed from
  the compiled project and a run that parsed another project would execute
  code the hash does not cover. execution-controller refuses to boot when they
  differ; `--profiles-dir /project` and `--project-dir /project` count as the
  image default, equal to leaving the flag out, and `-t` counts as `--target`.

## The `generate_schema_name` macro required for validation/releases

Your dbt project MUST route schema resolution through a `generate_schema_name`
that honours `DBT_TARGET_SCHEMA`. dbt has no `--target-schema` flag, so
continuo's blue/green release validation passes the candidate schema through
that variable; the macro must materialize into it when it is set. Without this,
validation runs would write into production schemas.

A project with no schema customization can ship the reference macro verbatim
from `dbt/base/macros/generate_schema_name.sql`:

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

When `DBT_TARGET_SCHEMA` is unset, behavior is byte-identical to a project
without the macro, so production runs are unaffected.

A project that **already defines its own** `generate_schema_name` must not
replace it — dbt prefers the project's macro over any packaged one anyway. Add
the override as the first branch and keep the existing logic below it:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set override = env_var('DBT_TARGET_SCHEMA', '') -%}
    {%- if override | length > 0 -%}
        {{ override }}
    {%- else -%}
        {# the project's existing schema logic, unchanged #}
    {%- endif -%}
{%- endmacro %}
```

The walkthrough in
[docs/run-projects-in-continuo.md](../docs/run-projects-in-continuo.md) covers
this case, including the caveat that a project already using a `DBT_TARGET_SCHEMA`
env var for another purpose must rename one of the two.

## Base image

`dbt/base` in this repo (`FROM python:3.12-slim`) pins
`dbt-core==1.12.0b1`, `dbt-postgres==1.10.0`, `dbt-adapters==1.24.5` and
`dbt-common==1.39.0` (an unpinned install now resolves to dbt Fusion, which
dropped the Postgres adapter; dbt-adapters and dbt-common hold dbt's built-in
macros, which a node's code fingerprint leaves out, so an unpinned upgrade of
either would change what runs with no change to any hash), bakes the
macro above into `/project/macros/`, and is what the in-repo e2e fixture
images build from. Real team images may build from it or replicate its
contract on any base — `continuo-demo` builds `FROM python:3.12-slim`
directly.
