# continuo

LLM-friendly command-line client for the Continuo platform. Calls the
`run-controller` service over gRPC, and the `catalog` service for a node's code
history and failure precedents, and emits machine-readable JSON by default.

## Status

Multiple commands implemented:

```
continuo schedule trigger <schedule-name>
continuo schedule list
continuo schedule graph <schedule-name>
continuo schedule set-cron <name> <cron> <timezone> [description]
continuo schedule remove-cron <name>
continuo schedule pause <name>
continuo schedule resume <name>
continuo node list [--search <table>] [--service <name>] [--operation run|test|build] [--limit N] [--offset N]
```

More verbs (status, cancel, logs) will follow the same output and
exit-code contract documented below.

## Build

```bash
# From the cli/ directory
make build            # → bin/continuo  (static, CGO_ENABLED=0)
make install          # go install to $GOBIN (~/go/bin fallback)
```

The Dockerfile in this directory produces a minimal alpine image with
the binary at `/usr/local/bin/continuo`:

```bash
docker build -t continuo-cli:dev .
```

## Test

```bash
make test              # unit tests only (fast, default for sibling services)
make test-integration  # spins up an in-process gRPC stub and exercises the binary end-to-end
```

## Usage

```
continuo [global-flags] <command> [args...]
```

### Global flags

| Flag         | Env                    | Default           | Purpose                                            |
|--------------|------------------------|-------------------|----------------------------------------------------|
| `--endpoint` | `CONTINUO_RUN_CONTROLLER_ADDR` | `localhost:50051` | gRPC address of the run-controller service |
| `--catalog-endpoint` | `CONTINUO_CATALOG_ADDR` | `localhost:50052` | gRPC address of the catalog service (`node versions`, `node diff`, `node upstream-changes`, `node code-units`, `precedents`) |
| `--dead-letter-endpoint` | `CONTINUO_DEAD_LETTER_ADDR` | `localhost:50055` | gRPC address of dead-letter-controller |
| `--timeout`  | `CONTINUO_TIMEOUT`     | `10s`             | Per-call deadline (any `time.ParseDuration` value) |
| `--human`    | —                      | `false`           | Emit a human one-liner on **stderr** instead of JSON on stdout |
| `--json`     | —                      | `true`            | Forward-compat no-op; JSON is already the default  |

Flag > env > default. An unparsable `--timeout` / `CONTINUO_TIMEOUT`
silently falls back to the default.

### `schedule trigger <schedule-name>`

Manually trigger a new run of a named schedule. Success prints a
single JSON object to stdout:

```json
{"run_id":"<uuid>","schedule_name":"<name>","triggered_at":"<RFC3339 UTC>"}
```

`triggered_at` is the CLI's clock at the moment the request returned;
the run-controller service does not echo a server timestamp today.

Every command that starts a run or cancels a schedule's active run
(`schedule trigger|test|build|cancel`, `node trigger|test|build|full-refresh`,
`run rerun` and `run rebase`) sends one request id per invocation:
`CONTINUO_REQUEST_ID` when it is set, otherwise a fresh UUID. The run-controller service
answers a repeat of the same id and arguments with the run the first call made
and writes nothing. The client retries such a command, with the same id, up to
three tries in all (100 ms, then 400 ms apart) when the run-controller service answers
UNAVAILABLE, or ABORTED because concurrent calls of the same id kept rolling
back. It does not retry a refusal for maintenance mode, and it never retries a
read. A schedule edit (`set-cron`, `remove-cron`, `pause`, `resume`) sends no
request id and is retried the same way on UNAVAILABLE only: its ABORTED is a
stale version, which sending the same request again cannot settle (a first
cron, sent with no version, is ABORTED only when other edits saved first on
three tries in a row). `set-cron` answers that ABORTED itself, by reading the
schedule again and saving once more; `pause` and `resume` exit 4 (see below).

With `--human`, stdout is empty and a single line is written to stderr:

```
Triggered run <uuid> for schedule '<name>'
```

### `schedule list`

Lists all schedules: every name the live release has and every name that has
a cron. Calls RunService.ListSchedules. Success prints a JSON object to
stdout:

```json
{"schedules":[{"schedule_name":"daily","cron":"0 1 * * *","cron_words":"At 01:00","has_cron":true,"paused":false,"version":3,"next_fire_at":"2026-10-28T00:00:00Z","description":"Runs daily at 1am","timezone":"Europe/Paris","model_count":41,"has_active_run":false,"last_run_id":"…","last_run_status":"succeeded","last_run_at":"2026-10-27T00:00:00Z","updated_by":"alice@example.com","updated_at":"2026-10-21T12:02:00Z"}]}
```

A schedule is scheduled (`has_cron` and not `paused`), paused, or manual
(`has_cron` false: it runs only when triggered). `description`, `cron_words`,
`next_fire_at`, `last_run_at`, `updated_by` and `updated_at` are omitted when
empty. `cron` and `version` are the fields `set-cron`, `pause` and `resume`
print for the schedule they saved; `version` counts the edits of the cron, from
1, and is 0 while the schedule has no cron. `model_count` is how many models of
the live release carry the schedule's tag.

### `schedule set-cron <name> <cron> <timezone> [description]`

Sets when a schedule runs on its own. Pass the five fields of the cron as one
argument (in a shell, quote it). The command re-sends the schedule's current
paused flag and description along with the cron, so a pause is kept; without
`[description]` the current description is kept, and a blank one is a usage
error. For a schedule that has a cron it sends the version it read as the expected
version, so a pause saved since is not overwritten (a `[description]` you pass
replaces the stored one): on a conflict it reads the schedule again and saves
once more, and a second conflict exits 4 with `changed by <who> while this ran;
run it again`. A first cron is saved with no version, so it overwrites an edit
saved between the read and the save. The cron itself is last-write-wins. Calls RunService.UpsertSchedule, then PreviewSchedule for the words.
Success prints:

```json
{"schedule_name":"daily","cron":"0 1 * * *","cron_words":"At 01:00","timezone":"Europe/Paris","description":"Runs daily at 1am","paused":false,"version":4,"next_fire_times":["2026-10-28T00:00:00Z","2026-10-29T00:00:00Z","2026-10-30T00:00:00Z"]}
```

A name no model of the live release carries, with no cron yet, is
`not_found` (exit 3), with a `did you mean` when a listed name differs only by
case; a cron or timezone the parser refuses is `usage` (exit 2) with the
parser's message.

### `schedule pause <name>` and `schedule resume <name>`

Read the schedule and save it back with `paused` flipped, its cron, timezone
and description kept, and its version as the expected version. Another edit
in between exits 4 with `changed by <who> while this ran; run it again`.
A schedule already in the requested state succeeds with `"changed":false` and
nothing is saved; a schedule with no cron has nothing to pause or resume
(`not_found`, exit 3). Success prints:

```json
{"schedule_name":"hourly","paused":false,"changed":true,"cron":"0 * * * *","timezone":"Europe/Paris","version":6,"next_fire_at":"2026-10-28T09:00:00Z"}
```

`pause` prints the same without `next_fire_at`.

### `schedule remove-cron <name>`

Removes the cron of a schedule, which then runs only when triggered. Calls
RunService.DeleteSchedule. Removing the cron of a schedule that has none
succeeds. Success prints `{"schedule_name":"daily","has_cron":false}`.

### `schedule graph <schedule-name>`

Returns the dependency graph of a schedule in the live release. Calls
RunService.GetScheduleGraph. Success prints a JSON object to stdout:

```json
{"schedule_name":"...","nodes":[{"table_name":"...","schema_name":"...","service_name":"...","node_type":"..."}],"edges":[{"from_node_id":"...","to_node_id":"..."}]}
```

### `node list`

Lists the nodes with per-node run statistics, one page at a time.
Calls RunService.ListNodes. This is the command that turns a bare table
name into the `<service> <schema> <table>` triple every other `node`
subcommand takes. Success prints a JSON object to stdout:

```json
{"total_count":7,"nodes":[{"service_name":"finance","schema_name":"analytics","table_name":"orders","task_count":12,"success_rate_pct":92,"avg_duration_sec":34,"p95_duration_sec":80,"flaky_rate_pct":8,"last_status":"succeeded","last_run_at":"2026-09-18T10:00:00Z","operation":"run","timeout_seconds":7200,"timeout_declared":true}]}
```

`timeout_seconds` is the task timeout the node's tasks get: its declared
timeout in the live release when `timeout_declared` is `true`, else the
install default (`runs.taskTimeoutHours`).

| Flag          | Default | Meaning                                                                 |
|---------------|---------|-------------------------------------------------------------------------|
| `--search`    | `""`    | Table name; case-insensitive **exact** match, not a substring           |
| `--service`   | `""`    | Exact service name filter                                               |
| `--operation` | `run`   | Which operation's runs feed the statistics: `run`, `test`, `build`      |
| `--limit`     | `50`    | Page size, 1..200; outside that range is a usage error (exit 2)         |
| `--offset`    | `0`     | Rows to skip; `total_count` tells the caller when another page exists   |

`success_rate_pct`, `avg_duration_sec` and `p95_duration_sec` are omitted
when the run-controller service has nothing to measure for that node; `last_run_at`
is omitted for a node that never ran. With `--human`, stderr gets one line
per node and a trailing `showing <first>-<last> of <total>` line.

## Exit codes

The CLI maps gRPC status codes onto a small, stable vocabulary. Exit
codes are part of the contract — scripts can branch on them.

| Exit | `code` (JSON)  | gRPC source                                       | Retryable | Meaning                                          |
|-----:|----------------|---------------------------------------------------|:---------:|--------------------------------------------------|
| 0    | —              | OK                                                | —         | Success                                          |
| 2    | `usage`        | `InvalidArgument`, or bad CLI args/flags          | no        | Caller error; fix the command and retry          |
| 3    | `not_found`    | `NotFound`                                        | no        | The named resource does not exist                |
| 4    | `conflict`     | `FailedPrecondition`, `AlreadyExists`, `Aborted`  | yes       | Transient business-rule violation (e.g. already running) |
| 5    | `unavailable`  | `Unavailable`, `DeadlineExceeded`                 | yes       | Network/service problem; safe to retry           |
| 6    | `internal`     | `Internal`, `Unknown`, non-gRPC errors            | no        | Bug or unclassified server failure               |
| 1    | —              | —                                                 | —         | Unclassified cobra error (should not happen)     |

JSON error envelope (stdout; stderr stays empty):

```json
{"error":{"code":"conflict","message":"…","retryable":true}}
```

In `--human` mode the same error is rendered on stderr as:

```
Error [conflict]: schedule "daily_ingest" already has an active run
```

## Examples

```bash
# Happy path
$ continuo schedule trigger daily_ingest
{"run_id":"e61d…","schedule_name":"daily_ingest","triggered_at":"2026-04-20T20:25:02Z"}
$ echo $?
0

# Re-trigger while a run is still pending/running
$ continuo schedule trigger daily_ingest
{"error":{"code":"conflict","message":"schedule \"daily_ingest\" already has an active run","retryable":true}}
$ echo $?
4

# run-controller service unreachable
$ continuo --endpoint localhost:1 schedule trigger daily_ingest
{"error":{"code":"unavailable","message":"…connection refused","retryable":true}}
$ echo $?
5

# Human mode
$ continuo --human schedule trigger daily_ingest 2>/tmp/msg; cat /tmp/msg
Triggered run cd4423ae-9161-4773-aa43-3ec087f24a0a for schedule 'daily_ingest'
```

## Design notes

- **JSON by default, human on demand.** The CLI is aimed at agents and
  scripts first, humans second. That is why JSON goes to stdout and the
  opt-in human text goes to stderr — a pipeline that always reads stdout
  never has to care which mode the caller picked.
- **Thin client, server owns the rules.** Pre-conditions (schedule
  exists, no concurrent run, …) are enforced by the run-controller service. The
  CLI only translates gRPC status codes into exit codes and JSON shapes.
- **Bounded retries, the caller owns the rest.** The client repeats only
  what is safe to repeat: a command that starts a run or cancels a
  schedule's active run (same request id; up to three tries, 100 ms then
  400 ms apart, on UNAVAILABLE or ABORTED) and a schedule edit (on
  UNAVAILABLE only). `set-cron` also reads
  the schedule again and saves once more after a stale version. Any other
  error, and any error left once those tries are spent, exits at once. The
  `retryable` bit is advice for the caller (agent, shell loop, CI step), who
  owns any further retry policy.
