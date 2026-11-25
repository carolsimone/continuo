# execution-controller

execution-controller runs each attempt, and each leg of a release check, as a Kubernetes Job of the project's image, and reports how it went. It never decides whether an attempt is tried again. The shared words (attempt, task, dispatch, failure class, task timeout, overlay, dependents check) are in the root `GLOSSARY.md`.

## Language

### Running an attempt

**Image contract**:
The agreement between an install and a project's image: the install names the image and the command for each operation, and the image must run them as described, against the warehouse and schema it is given.
_Avoid_: image requirements, image spec


**Job**:
The Kubernetes Job that runs one attempt, or one leg of a release check, from the project's image. Outside execution-controller, say the attempt.
_Avoid_: deployment, workload, container

**Start timeout**:
How long an attempt's Job may take to start its container before the attempt fails.
_Avoid_: boot timeout, pending timeout

**Deadline**:
The latest moment an attempt's Job may still be running: its start timeout plus its task timeout.
_Avoid_: hard timeout, TTL

**Adopted Job**:
A Job already running for an attempt, found after a restart or a repeated dispatch, that execution-controller takes over instead of starting a second one.
_Avoid_: reclaimed Job, orphan Job

**Lost attempt**:
An attempt run-controller knows was taken in, of which execution-controller holds neither a record nor a Job. It is reported failed so run-controller can try again.
_Avoid_: missing attempt, ghost attempt

### Stopping work

**Fence**:
execution-controller's record that a run was cancelled. While it stands, nothing of that run starts, and any Job of it found later is removed.
_Avoid_: tombstone, cancel flag, block list

### After a seed reload

**View probe**:
A short check, after a seed reload in a warehouse that keeps views across a table's drop, that asks each view outside continuo whether it still answers.
_Avoid_: health check, view test

### Capacity

**Job capacity**:
How many Jobs may run at once across the install. An attempt beyond it waits until a Job ends.
_Avoid_: slot, execution slot, concurrency limit
