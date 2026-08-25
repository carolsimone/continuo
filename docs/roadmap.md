# Roadmap

What Continuo does **today** is in the [README](../README.md#-what-it-delivers).
This page is what comes **next**.

Nothing here is a commitment to a date. Each item carries a **Target date** —
our best current estimate of when it ships. A blank target date (`—`) means it
is on the list but not yet scheduled.

## Planned

### More warehouse engines for the Python runtime
**Target date:** —

`continuo-python-runtime` runs validation and Python nodes on **Postgres** and
**Trino** today, both behind a single `WarehouseAdapter` port — one adapter per
engine, discovered at runtime. The next engines are **Spark**, **BigQuery**, and
**Snowflake**. Each is a new adapter against that same port, plus an
engine-matched runtime image and a `validation.engine` value in the Helm chart.
The dbt materialization leg moves to the selected engine too, so a
Spark/BigQuery/Snowflake install is a full deployment, not a half-Postgres one.

### Agentic remediation for production run failures
**Target date:** —

The remediation agent already proposes a fix when a *release* is rejected at
validation. The next step is the same path for a **scheduled run that fails in
production**: the failure is classified as an event, a fixable one gets an
LLM-proposed diff, and a human reviews and merges it. Merging stays a human
decision by design.

### Standardize every node on the Open Data Contract Standard (ODCS)
**Target date:** —

Every node — dbt models, Python nodes, seeds, and streaming producers — will
carry a data contract in the Open Data Contract Standard (ODCS) format: schema,
data-quality expectations, and ownership expressed the same way regardless of
runtime. One contract format across the whole graph replaces the per-runtime
metadata we derive today and gives the control plane and the remediation agents
a single, portable contract to enforce and reason about.

### One control plane for streaming and batch
**Target date:** —

A single view — with contract enforcement and lineage — for both streaming and
batch. Contracts stay in sync by design, so a software engineer changing a
streaming producer sees the downstream impact on batch models and the teams that
own them. The goal is for agents to propose the downstream changes
automatically, reusing the `agent-remediation` service and the diffs, data
types, historical diffs, and documentation already stored in the graph database.

### Automatic detection of performance regressions
**Target date:** —

The graph database already stores every diff and the code of each node, and the
`state` database has the run history — each node's ancestry and history are
queryable. The missing piece is using that to flag a node whose run time is
getting worse and to point at the change that caused it.

### Show who pushed a change in the UI
**Target date:** —

Each release will show the GitHub handle of the user who pushed it, next to the
validation result.

### Test suite for circular dependencies
**Target date:** —

Cycles across projects already fail at CD. The check needs its own test suite
covering the cross-project and cross-runtime cases.
