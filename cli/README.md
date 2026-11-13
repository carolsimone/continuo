# continuo CLI

A command-line client for continuo. Every command prints JSON, so people, scripts and the in-UI assistant use it the same way. `continuo describe` lists every command with its arguments, flags and output.

## Build

Run these from `cli/`:

```bash
make install           # puts continuo in $GOBIN (~/go/bin by default)
make build             # builds bin/continuo instead
make test              # unit tests
make test-integration  # runs the binary against an in-process gRPC stub
```

The `Dockerfile` builds a small image with the binary at `/usr/local/bin/continuo`.

## Connect

The CLI calls four services over gRPC. On a local install, forward them to the default ports:

```bash
kubectl -n continuo port-forward svc/run-controller 50051:50051 &
kubectl -n continuo port-forward svc/catalog 50052:50052 &
kubectl -n continuo port-forward svc/dead-letter-controller 50055:50055 &
kubectl -n continuo port-forward svc/release-controller 50056:50056 &
```

| Flag | Environment variable | Default | What it sets |
|---|---|---|---|
| `--endpoint` | `CONTINUO_RUN_CONTROLLER_ADDR` | `localhost:50051` | run-controller: schedules, runs and nodes |
| `--catalog-endpoint` | `CONTINUO_CATALOG_ADDR` | `localhost:50052` | catalog: code versions and failure precedents |
| `--dead-letter-endpoint` | `CONTINUO_DEAD_LETTER_ADDR` | `localhost:50055` | dead-letter-controller: dead letters |
| `--release-endpoint` | `CONTINUO_RELEASE_CONTROLLER_ADDR` | `localhost:50056` | release-controller: seed reload notices of a candidate release |
| `--timeout` | `CONTINUO_TIMEOUT` | `10s` | Time limit for each call. A value that does not parse is ignored |
| `--human` | | off | Prints one short line on stderr instead of JSON on stdout |

A flag beats its environment variable, which beats the default.

```bash
continuo schedule list
continuo schedule trigger daily
continuo node history finance analytics fx_transactions_eur
```

## Output and exit codes

A command that works prints JSON on stdout and exits 0. A command that fails prints an error on stdout and exits with a code that says why:

```json
{"error":{"code":"conflict","message":"schedule already has an active run","retryable":true}}
```

`retryable` says whether sending the same command again can work.

| Exit | `code` | Meaning |
|--:|---|---|
| 0 | | It worked |
| 2 | `usage` | Wrong arguments or flags. Fix the command |
| 3 | `not_found` | The schedule, run, node or id does not exist |
| 4 | `conflict` | The request clashes with the current state, for example the schedule already has an active run |
| 5 | `unavailable` | A service is unreachable or too slow |
| 6 | `internal` | A bug or an unexpected server error |
| 7 | `maintenance` | continuo is in maintenance mode and refuses new work |
| 1 | | Any other error |

## Request ids

`schedule trigger`, `test`, `build` and `cancel`, `node trigger`, `test`, `build` and `full-refresh`, `run rerun` and `run rebase` send a request id: `CONTINUO_REQUEST_ID` (it must be a UUID) or a fresh one. Sending the same id with the same arguments again returns the first run and starts nothing, so a lost answer is safe to retry. The same id with other arguments is a `conflict`. The CLI tries these commands up to three times in all when the service is unavailable, with the same id.

The other commands that change state send no request id.

## Commands

The last column says whether the command changes state.

### Schedules

A schedule is a group of models that run together, picked by their first dbt tag.

| Command | What it does | Changes state |
|---|---|:--:|
| `schedule list` | Lists every schedule with its cron, whether it is paused, its model count, whether a run is active, and its last run. | |
| `schedule graph <name>` | Shows the nodes of a schedule and the dependencies between them. Each node says whether it is `dormant`: a node continuo builds that has no schedule tag. A run started by hand runs it, and any run whose load of a seed drops the seed's table includes it only to rebuild it, as a view or materialized view below that seed. An unknown name gives an empty graph. | |
| `schedule runs <name>` | Lists the schedule's finished runs, newest first. | |
| `schedule status <name>` | Shows the schedule's latest run and the status of each of its tasks. Prints `{"run":null,"tasks":[]}` if it never ran. | |
| `schedule trigger <name>` | Starts a run of the whole schedule and prints its `run_id`. Exits 4 if the schedule already has an active run, and 3 if no model carries the name. | yes |
| `schedule test <name>` | Starts a run that runs `dbt test` for every model of the schedule. | yes |
| `schedule build <name>` | Starts a run that runs `dbt build` for every model of the schedule. | yes |
| `schedule cancel <name> <reason>` | Cancels the schedule's active run. Exits 4 if there is none. | yes |
| `schedule set-cron <name> <cron> <timezone> [description]` | Sets when the schedule runs on its own. Give the five cron fields as one quoted argument. Prints the cron in words and the next three fire times. | yes |
| `schedule remove-cron <name>` | Removes the cron. The schedule then runs only when triggered. | yes |
| `schedule pause <name>` | Stops the cron from starting runs. Prints `"changed":false` if it is already paused. Exits 3 if the schedule has no cron. | yes |
| `schedule resume <name>` | Lets the cron start runs again and prints the next fire time. | yes |

`set-cron`, `pause` and `resume` read the schedule first. If someone else saved a change in between, `pause` and `resume` exit 4, and `set-cron` reads again and saves once more.

### Runs

| Command | What it does | Changes state |
|---|---|:--:|
| `run show <run-id>` | Shows one run and, for each task, its operation, why the planner chose it, its status, attempt, failure, and the run it waits for if another run holds its node. | |
| `run graph <run-id>` | Shows the run's nodes with their status, and the dependencies between them. | |
| `run attempts <run-id> [task-id]` | Lists the attempts of the run's tasks, or of one task: job name, status, exit code, times and log location. | |
| `run cancel <run-id> <reason>` | Cancels a run. It shows `cancelling` until its Jobs stop, then `cancelled`. Cancelling again changes nothing. | yes |
| `run rerun <run-id>` | Starts a new run of what did not succeed in a failed or cancelled run, on the release that run used. | yes |
| `run rebase-preview <run-id> [with-changed-upstream]` | Shows, as counts, what a rebase on the live release would run again. Only for a failed or cancelled run, otherwise it exits 4. `with-changed-upstream` is `true` or `false` (default `false`) and adds changed nodes upstream. | |
| `run rebase <run-id> <release-id> [with-changed-upstream]` | Restarts a failed or cancelled run from its failure on the live release. `<release-id>` is the one `run rebase-preview` printed. If the live release has changed since, it exits 4 and starts nothing. Pass the same `with-changed-upstream` as in the preview. | yes |

### Nodes

A node is one model, seed or python script. Node commands take it as `<service> <schema> <table>`, and `node list` shows these three for every node the live release builds.

| Command | What it does | Changes state |
|---|---|:--:|
| `node list` | Lists nodes with run statistics: success rate, durations, last status and task timeout. Every node the live release builds is listed (never an `unknown-external` node or a dbt test), one that never ran included (`task_count` 0, no `last_run_at`), and each says its `node_type` and whether it is `dormant`. Filter with `--search` (an exact table name), `--service`, `--dormant` (only the nodes with no schedule, which `node trigger` runs by hand) and `--operation` (`run`, `test` or `build`, default `run`). Page with `--limit` (1 to 200, default 50) and `--offset`. | |
| `node history <service> <schema> <table>` | Lists the node's recent runs: status, image tag, times, log location and content hash, and on a seed task whose load stopped with `unowned_dependents` the `blocking_objects` to name with `node full-refresh --acknowledge`. `--operation` picks `run`, `test` or `build` (default `run`). | |
| `node trigger <service> <schema> <table> [source-run-id]` | Starts a run of one node (`dbt run`) and prints its `run_id`. With the id of a past run, it uses that run's image and manifest instead of the latest. Exits 3 if the live release has no such node. | yes |
| `node test <service> <schema> <table> [source-run-id]` | The same, with `dbt test`. | yes |
| `node build <service> <schema> <table> [source-run-id]` | The same, with `dbt build`. | yes |
| `node full-refresh <service> <schema> <table> [source-run-id]` | The same, with `dbt --full-refresh`: rebuilds the model or seed from scratch. A seed's full refresh also rebuilds the continuo views that read it; `--acknowledge <schema>.<name>` (repeatable) names an object outside continuo that the refresh may drop. | yes |
| `node versions <service> <schema> <table>` | Lists the code versions continuo recorded for the node, newest first, with their hashes. `--include-code` adds the SQL, and `--limit` caps the list (default 20). | |
| `node diff <service> <schema> <table> --from N --to M` | Shows the SQL diff between two recorded versions, given by the `version_seq` numbers of `node versions`. Both flags are required. | |
| `node upstream-changes <service> <schema> <table>` | Lists the latest code change of each ancestor of the node, with its distance (`depth`) and diff. `--depth` sets how far up to look (default 3, at most 10), and `--since` (a timestamp) hides older changes. | |
| `node code-units [<unit-id>]` | Lists the version history of shared-code units, by unit id or for one node with `--service`, `--schema` and `--table`. Empty for a node without shared code. Exits 3 for an unknown unit id. | |

### Releases

| Command | What it does | Changes state |
|---|---|:--:|
| `release notices <release-id>` | Lists the seeds whose load a candidate release changes: what each load does, the views it rebuilds (a dormant one included), what refreshes on its schedule (a dormant node, whose `schedule` is empty, only when run by hand), what the warehouse holds outside continuo, and whether the load is predicted to go ahead or stop. A notice predicts, the run's preflight decides. | |

### Failures and dead letters

A dead letter is a message a service gave up on.

| Command | What it does | Changes state |
|---|---|:--:|
| `precedents [signature]` | Looks up how earlier rejections with the same failure ended: release, node, stage, error excerpt, and whether and how it was fixed. Give a signature, or `--category` with `--reason`. `--include-code` adds the code, and `--limit` caps the list (default 5). | |
| `dlq list [source] [stream]` | Lists open dead letters, newest first, at most 50. `source` is `consumer`, `outbox`, `quarantine` or `all`, and `stream` filters by stream. `total_open` counts all open ones. | |
| `dlq show <id>` | Shows one dead letter with the message fields a redrive would send. | |
| `dlq redrive <ids> <reason>` | Sends dead letters back to the stream they came from, so their consumer tries again. `<ids>` is one id or several, comma separated: all are redriven or none. The reason is recorded. | yes |

### Help

| Command | What it does | Changes state |
|---|---|:--:|
| `describe` | Prints the catalog of every command with its arguments, flags (marking the ones the in-UI assistant may set and the ones that repeat), exit codes and output shape. | |
