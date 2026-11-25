# continuo

continuo runs a data platform's dbt and Python nodes on schedules, checks every code change in a candidate release before it reaches production, and proposes fixes when a release fails.

This glossary holds the words more than one component uses. A word only one component uses is in that component's `GLOSSARY.md` (see `GLOSSARY-MAP.md`).

## Language

### Nodes and schedules

**Node**:
One table, view or other object continuo builds or reads, together with the code that builds it: a dbt model, seed or snapshot, or a Python node.
_Avoid_: model (a model is one kind of dbt node), asset, job, step

**Project**:
The code a team releases as one unit: one dbt project, or one Python package of nodes. One repository can hold several projects.
_Avoid_: service (the name code, API and storage use for it), repo, team repo, job

**Seed**:
A node whose data is a CSV file in the project, loaded as a table.
_Avoid_: static table, lookup load

**Unknown-external node**:
A table that nodes read but no node builds. continuo never builds, checks or runs it.
_Avoid_: ingested node, source, external table

**Dormant node**:
A node continuo builds that belongs to no schedule, so only a run started by hand executes it.
_Avoid_: orphan node, unscheduled model, inactive node

**Owner**:
The team that answers for a node and reviews the fixes proposed for it.
_Avoid_: maintainer, domain team

**Schedule**:
A named set of nodes that run together, with an optional cron that starts a run of it.
_Avoid_: DAG, pipeline, job, tag

**Contract**:
The published description of what a node's table holds: its columns, types, keys and owner.
_Avoid_: schema file, data contract spec

**Ontology**:
The company's one file naming the things its data is about and which team owns each, which contracts can bind their tables to.
_Avoid_: taxonomy, business glossary

### Releases and runs

**Release**:
One version of the platform's nodes, their code and the edges between them. Every run executes on exactly one release.
_Avoid_: snapshot, topology version

**Candidate release**:
A release still in its release check. It becomes the live release only if the release check passes.
_Avoid_: blue/green, staging release, pending release

**Release check**:
The checks a release's code passes through before it can reach production: parse, build and validate. A fix passes through the same release check before it becomes a pull request.
_Avoid_: pipeline run, release pipeline, verification run, validation run

**Live release**:
The release production is on now. A new run uses it unless it repeats an earlier run's release.
_Avoid_: latest snapshot, current topology

**Promotion**:
The moment a candidate release becomes the live release.
_Avoid_: deploy, go-live, publish

**Rejection**:
The end of a candidate release that failed its release check. It never becomes live, and its failures can be fixed.
_Avoid_: failed release, rollback

**Run**:
One execution of a set of nodes on one release, started by a schedule, by hand, or from an earlier run that failed.
_Avoid_: DAG run, scheduler

**Task**:
The work one run does at one node.
_Avoid_: job, step, node run

**Attempt**:
One try at a task. A task that fails for a reason worth retrying gets another attempt.
_Avoid_: retry, job, execution

**Operation**:
What a task does at its node: run it, test it, build it (run then test), or full-refresh it.
_Avoid_: command, mode, action

**Full refresh**:
The operation that rebuilds a node's table from nothing instead of adding to it. For a seed it drops and reloads the table.
_Avoid_: rebuild, reset, reload

**Dispatch**:
Handing one attempt of a task to the component that runs it.
_Avoid_: send, enqueue, schedule

**Failure class**:
Why an attempt failed, in the words that decide whether it is tried again: the task's own code, resources, infrastructure, a pod that never started, a timeout, or an invalid request.
_Avoid_: error type, failure kind

**Seed reload**:
The load of a seed whose content changed. A load that drops the seed's table also takes away every view built on it, so the same run rebuilds them.
_Avoid_: seed refresh, seed promotion

**Dependents check**:
The check, before a seed reload drops the seed's table, that lists every object in the warehouse reading that table. It stops the load when an object continuo does not rebuild would break, unless an operator has acknowledged that object.
_Avoid_: seed preflight, preflight, pre-flight

**Rerun**:
A new run that executes again the nodes a failed or cancelled run did not finish, and the nodes below them, on that run's own release.
_Avoid_: rerun failed, rerun on this snapshot, retry

**Rebase**:
A new run that restarts a failed or cancelled run from its failure on the live release, also running the nodes the live release added below it.
_Avoid_: rerun on latest snapshot, rerun failed (latest snapshot)

**Single-node run**:
A run of one node, started by hand.
_Avoid_: node trigger, ad-hoc run

### Fixing a rejected release

**Fix**:
A change to a rejected release's code that continuo proposes and that has passed a release check, offered to the owner as a pull request.
_Avoid_: proposal, patch, heal

**Hand-over**:
A failure continuo leaves to a person, with the reason it could not fix it.
_Avoid_: escalation, give-up

### Operating continuo

**Component**:
One of continuo's own programs, such as run-controller or catalog.
_Avoid_: service, microservice

**Tenant**:
One organisation whose nodes, runs and releases continuo keeps apart from every other organisation's.
_Avoid_: workspace, account, org

**Maintenance mode**:
A state in which continuo starts no run and accepts no schedule change.
_Avoid_: freeze, read-only mode

**Dead letter**:
A message no component could process, kept until an operator sends it again.
_Avoid_: poison message, failed event
