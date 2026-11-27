# agent-remediation

agent-remediation proposes fixes for a rejected release: it groups the release's failures, asks a model for a fix to each group, checks every fix and proves it with a release check before offering it to the owner. The shared words (release, rejection, fix, hand-over, owner, release check, heal trigger, round, unfixable node, error signature) are in the root `GLOSSARY.md`.

## Language

### The work on one rejected release

**Remediation**:
The whole effort to fix one rejected release in one round, in answer to one heal trigger, made of one or more tries.
_Avoid_: remediation run, healing, job

**Try**:
One go at fixing every failing node of a remediation: draft a fix for each cluster, check the drafts, then prove them together with a release check. A remediation makes a fixed number of tries before it hands over. A try is named by its round and number: round 2, try 1.
_Avoid_: attempt (an attempt is a task's, in a run), iteration, pass

**Skipped try**:
A try that did no work and says why, such as maintenance mode or a heal trigger that named no repository.
_Avoid_: empty attempt, skipped attempt

**Cluster**:
The failing nodes one draft fixes together, because they fail the same way and share the changed node the fix belongs in.
_Avoid_: group, batch, bucket

**System failure**:
The end of a remediation that stopped because continuo itself kept failing, not because no fix was found. Sending its heal trigger again resumes it.
_Avoid_: crash, error state

**Approver**:
The owner asked to review what the remediation did with a node, fixed or handed over.
_Avoid_: reviewer, assignee

### How a cluster is worked

**Flow**:
The recipe a remediation follows: which types it knows, which checks run across clusters, how many tries it gets and what it does when it passes or gives up.
_Avoid_: workflow, pipeline, playbook

**Type**:
A kind of failure a cluster can be, such as a dbt parse error or a duplicate table, deciding which evidence is gathered, which prompt the model answers and which checks its draft must pass.
_Avoid_: lane, typology, category

**Step**:
One action a flow or type names: gather evidence, ask the model, check a draft, prove the drafts, or end the remediation.
_Avoid_: task, stage, node

**Evidence**:
What a cluster's steps gather for the model to read: the failure, the code, its upstream changes, earlier tries.
_Avoid_: context, facts

**Data class**:
The kind of evidence a value is, code, logs or rows, which decides whether a model may be shown it.
_Avoid_: sensitivity, data category

**Draft**:
The edits the model proposes for one cluster, before they are checked.
_Avoid_: answer, suggestion, patch

**Pre-flight**:
The checks one cluster's draft must pass before the try goes on.
_Avoid_: lint, validation, precheck

**Revision**:
The model's new draft for a cluster after pre-flight refused the last one, made from the refused draft and what pre-flight found. A type sets how many drafts a cluster gets in one try (its `drafts` key counts them).
_Avoid_: round, retry, re-prompt

**Cross-cluster check**:
A check across every draft of a try, such as two drafts editing the same file.
_Avoid_: global check, try check

**Gate**:
The release check the try's drafts must pass, together, before they become a fix.
_Avoid_: verification run, verify step

**Fall back**:
What happens to a cluster fixed at a shared upstream node when that fix fails: its members are worked on their own instead.
_Avoid_: split, retry

**Handover reason**:
The code saying why a node was left to a person, such as declined, no type matched or tries spent.
_Avoid_: failure reason, give-up code

### Install files

**Fixture**:
A scripted scenario that plays a type or flow with a made-up release, repository and model answers, to show it handles them.
_Avoid_: test case, mock
