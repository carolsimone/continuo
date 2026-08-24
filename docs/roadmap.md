# Roadmap

What Continuo does today is in the [README](../README.md#-what-it-delivers).
This page is the rest: work that is started, work that is planned, and ideas
that are still open. Nothing here is a commitment to a date.

## In progress

**Agentic remediation for failed production runs.**
Today the remediation agent proposes a fix when a *release* is rejected. The
next step is the same path for a scheduled run that fails in production: the
failure is classified as an event, a fixable one gets an LLM-proposed diff, and
a human reviews and merges it. Merging stays a human decision by design.

**One control plane for streaming and batch.**
A single view, with contract enforcement and lineage, for both. Contracts stay
in sync by design, so a software engineer changing a streaming producer sees
the downstream impact on batch models and the teams that own them. The goal is
for agents to propose the downstream changes automatically, reusing the
`agent-remediation` service and the diff, data types, historical diffs, and
documentation already stored in the graph database.

**Automatic detection of performance regressions.**
The graph database already stores every diff and the code of each node, and
the `state` database has the run history. Each node's ancestry and history are
queryable. The missing piece is using that to flag a node whose run time is
getting worse and to point at the change that caused it.

## Planned

**Show who pushed a change in the UI.**
Each release will show the GitHub handle of the user who pushed it, next to
the validation result.

**Test suite for circular dependencies.**
Cycles across projects already fail at CD. The check needs its own test suite
covering the cross-project and cross-runtime cases.

## Ideas

**Better local development.**
The current stance is that dbt and the Python runtime own local development,
and Continuo does not. That may change. Continuo has three things dbt does not:
the GitHub history of every change, when each change happened, and the full
upstream and downstream graph across projects. That is enough to help an agent
working locally understand the blast radius of a change before it is pushed.

**A single query engine.**
One implementation on top of Trino, so validation and runs behave the same on
every warehouse.

**Managed compute, your data.**
Data stays inside the company's data-center perimeter; Continuo runs the
control plane and the compute.

## Not planned

**A schema registry.**
Contracts are already enforced on every node, so they act as a schema registry
by design. A separate one is not needed.
