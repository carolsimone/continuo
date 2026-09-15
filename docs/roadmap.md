# Roadmap

What continuo does **today** is in the [README](../README.md#-what-it-delivers).
This page is what comes **next**.

Nothing here is a commitment to a date. Each item carries a **Target date** —
our best current estimate of when it ships. `TODO` means it is on the list but
not yet scheduled. Where an item has a tracking issue, it is linked next to the
date.

## Planned

### More warehouse engines for the Python runtime
**Target date:** TODO · **Tracking:** [#517](https://github.com/carolsimone/continuo/issues/517)

`continuo-python-runtime` runs validation and Python nodes on **Postgres** and
**Trino** today, both behind a single `WarehouseAdapter` port — one adapter per
engine, discovered at runtime. The next engines are **Spark**, **BigQuery**, and
**Snowflake**. Each is a new adapter against that same port, plus an
engine-matched runtime image and a `validation.engine` value in the Helm chart.
The dbt materialization leg moves to the selected engine too, so a
Spark/BigQuery/Snowflake install is a full deployment, not a half-Postgres one.

### Agentic remediation for production run failures
**Target date:** TODO

The remediation agent already proposes a fix when a *release* is rejected at
validation. The next step is the same path for a **scheduled run that fails in
production**: the failure is classified as an event, a fixable one gets an
LLM-proposed diff, and a human reviews and merges it. Merging stays a human
decision by design.

### Standardize every node on the Open Data Contract Standard (ODCS)
**Target date:** TODO

Every node — dbt models, Python nodes, seeds, and streaming producers — will
carry a data contract in the Open Data Contract Standard (ODCS) format: schema,
data-quality expectations, and ownership expressed the same way regardless of
runtime. One contract format across the whole graph replaces the per-runtime
metadata we derive today and gives the control plane and the remediation agents
a single, portable contract to enforce and reason about.

### Data-quality gate in the deployment pipeline
**Target date:** TODO

Today a candidate release is gated by compilation, manifest parsing, and schema
validation before it can be promoted. The next gate is **data quality**: once a
node materializes in the candidate release, run its data-quality expectations —
row-level tests, freshness, and drift against the current production baseline —
and block promotion on a breach. This catches data drift and quality
regressions that compile and schema-validate cleanly but would still ship bad
data.

### One control plane for streaming and batch
**Target date:** TODO

A single view — with contract enforcement and lineage — for both streaming and
batch. Contracts stay in sync by design, so a software engineer changing a
streaming producer sees the downstream impact on batch models and the teams that
own them. The goal is for agents to propose the downstream changes
automatically, reusing the `agent-remediation` service and the diffs, data
types, historical diffs, and documentation already stored in the graph database.

### Automatic detection of performance regressions
**Target date:** TODO

The graph database already stores every diff and the code of each node, and the
`state` database has the run history — each node's ancestry and history are
queryable. The missing piece is using that to flag a node whose run time is
getting worse and to point at the change that caused it.


### Test suite for circular dependencies
**Target date:** TODO

Cycles across projects already fail at CD. The check needs its own test suite
covering the cross-project and cross-runtime cases.

### Every UI action available through the CLI
**Target date:** TODO

`agent-chat` builds its tool catalog from `continuo describe` at boot, so the
CLI's command tree *is* the set of operations the chat agent can perform. Today
the CLI covers schedules (list, status, graph, build, test, trigger, cancel),
single nodes (build, test, trigger, diff, history, versions, upstream changes,
code units), and failure precedents. The UI can do more: browse releases and
their verification attempts, read the release log and the current production
pointer, list and inspect remediation proposals, open a proposal's fix PR, retry
remediation on a rejected release, rerun or rebase a run, and read a task's
execution logs. The goal is one CLI command for every read and every action the
UI offers, so the chat agent can do anything an operator can do from the
dashboard. Mutating commands carry the `mutating` annotation in `describe`, so
the chat's human-confirmation gate applies to each new action without changes
to `agent-chat`.

### Validate single sign-on against a real Okta tenant
**Target date:** TODO · **Tracking:** [#564](https://github.com/carolsimone/continuo/issues/564)

The UI signs users in through standard OpenID Connect (OIDC) discovery, so any
provider that serves `/.well-known/openid-configuration` plugs in with an issuer
URL, a client id and a client secret, and roles come from the provider's groups
claim. That flow is exercised in CI against the bundled Dex only. The next step
is a manual validation against an Okta Integrator Free Plan org: discovery over
https, operator and viewer group mapping, denied login for an unmapped user,
logout, and both the org and custom authorization-server issuer forms. What
works becomes an Okta section in the deploy README; anything that does not
becomes a fix with a regression test.
