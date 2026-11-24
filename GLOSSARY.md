# continuo

continuo runs a data platform's dbt and Python nodes on schedules, checks every code change in a candidate release before it reaches production, and proposes fixes when a release fails.

## Language

### Releases and runs

**Release**:
One version of the platform's nodes, their code and the edges between them. Every run executes on exactly one release.
_Avoid_: snapshot, topology version

**Live release**:
The release production is on now. A new run uses it unless it repeats an earlier run's release.
_Avoid_: latest snapshot, current topology

**Run**:
One execution of a set of nodes on one release, started by a schedule, by hand, or from an earlier run that failed.
_Avoid_: DAG run, scheduler

**Rerun**:
A new run that executes again the nodes a failed or cancelled run did not finish, and the nodes below them, on that run's own release.
_Avoid_: rerun failed, rerun on this snapshot, retry

**Rebase**:
A new run that restarts a failed or cancelled run from its failure on the live release, also running the nodes the live release added below it.
_Avoid_: rerun on latest snapshot, rerun failed (latest snapshot)
