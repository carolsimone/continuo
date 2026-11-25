# run-controller

run-controller decides when runs start, what each run executes, and when every task and run has ended. The shared words (run, task, attempt, schedule, rerun, rebase, seed reload, failure class) are in the root `GLOSSARY.md`.

## Language

### Starting runs

**Cron**:
The rule, with its timezone, that says when a schedule's runs start on their own. A schedule may have none.
_Avoid_: trigger, timer, scheduler

**Slot**:
One moment a cron says a schedule should run. A slot gets at most one run.
_Avoid_: tick, fire time, occurrence

**Missed slot**:
A slot that passed while run-controller could not act on it. Only the latest due slot gets a run; the older ones are counted and dropped.
_Avoid_: skipped run, backfill

**Paused schedule**:
A schedule whose cron is kept but starts no run until it is resumed.
_Avoid_: disabled schedule, stopped schedule

**Scheduled run**:
A run its schedule's cron started.
_Avoid_: cron run, automatic run

**Manual run**:
A run of a whole schedule that someone started by hand.
_Avoid_: trigger, trigger run, ad-hoc run

**Promote-seed run**:
A run run-controller starts by itself to load a seed whose content changed in the live release.
_Avoid_: seed run, seed sync

### Running tasks

**Retry budget**:
How many more attempts a task gets after a failure worth retrying. It is fixed when the run starts.
_Avoid_: max retries, retry count

**Write lease**:
The right of one task at a time to write a node's table, so two runs never build the same node together. A test task needs none.
_Avoid_: lock, node lock, mutex

**Lease wait**:
A task that is ready but waits because another run's task holds its node's write lease. It starts when the lease is free and its turn comes.
_Avoid_: blocked task, queued task

**Rebuild task**:
A task that rebuilds a view downstream of a seed whose reload dropped the seed's table, whatever the view's schedule.
_Avoid_: repair task, dependent refresh

**View outage**:
The time a view is missing, or stale, in production because a seed reload dropped the table under it and its rebuild has not yet succeeded.
_Avoid_: broken view, downtime

### Ending runs

**Stall**:
The end of a run that has nothing in flight and has made no progress for a while. It is failed so its schedule can run again.
_Avoid_: hang, timeout, stuck run

**Cancelling run**:
A run that was cancelled while some of its tasks were still running. It starts nothing more and ends cancelled once each running task has stopped.
_Avoid_: aborting run, killed run

**Stopping task**:
A task of a cancelling run whose attempt is still running and has been told to stop.
_Avoid_: killing task, aborting task
