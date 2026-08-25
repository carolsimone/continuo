# Roadmap

What Continuo does today is in the [README](../README.md#-what-it-delivers).
This page is the rest: work that is started and work that is planned.
Nothing here is a commitment to a date.

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

**Spreadsheet nodes.**
A spreadsheet becomes a first-class node type, on the same integration contract
as dbt, Python, and CSV. Its column contract feeds the same one graph, and a
change to it is validated against its downstream lineage the same way — so a
spreadsheet another team edits can no longer silently break a model that reads
from it.

**Show who pushed a change in the UI.**
Each release will show the GitHub handle of the user who pushed it, next to
the validation result.

**Test suite for circular dependencies.**
Cycles across projects already fail at CD. The check needs its own test suite
covering the cross-project and cross-runtime cases.
