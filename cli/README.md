# continuo

LLM-friendly command-line client for the Continuo platform. Calls the
`state` service over gRPC and emits machine-readable JSON by default.

## Status

Multiple commands implemented:

```
continuo schedule trigger <schedule-name>
continuo schedule list
continuo schedule graph <schedule-name>
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
| `--endpoint` | `CONTINUO_STATE_ADDR`  | `localhost:50051` | gRPC address of the state service                  |
| `--orchestrator-endpoint` | `CONTINUO_ORCHESTRATOR_ADDR` | `localhost:50052` | gRPC address of the orchestrator service |
| `--timeout`  | `CONTINUO_TIMEOUT`     | `10s`             | Per-call deadline (any `time.ParseDuration` value) |
| `--human`    | —                      | `false`           | Emit a human one-liner on **stderr** instead of JSON on stdout |
| `--json`     | —                      | `true`            | Forward-compat no-op; JSON is already the default  |

Flag > env > default. An unparsable `--timeout` / `CONTINUO_TIMEOUT`
silently falls back to the default.

### `schedule trigger <schedule-name>`

Manually trigger a new run of a named schedule. Success prints a
single JSON object to stdout:

```json
{"schedule_id":"<uuid>","schedule_name":"<name>","triggered_at":"<RFC3339 UTC>"}
```

`triggered_at` is the CLI's clock at the moment the request returned;
the state service does not echo a server timestamp today.

With `--human`, stdout is empty and a single line is written to stderr:

```
Triggered run <uuid> for schedule '<name>'
```

### `schedule list`

Lists all schedules. Calls StateService.ListAllSchedules. Success prints a
JSON object to stdout:

```json
{"schedules":[{"schedule_name":"...","cron_expression":"..."}]}
```

### `schedule graph <schedule-name>`

Returns the dependency graph for a schedule. Calls OrchestratorQuery.GetScheduleGraph.
Success prints a JSON object to stdout:

```json
{"schedule_name":"...","nodes":[...],"edges":[...]}
```

Requires `--orchestrator-endpoint` or `CONTINUO_ORCHESTRATOR_ADDR` to be set correctly.

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
{"schedule_id":"e61d…","schedule_name":"daily_ingest","triggered_at":"2026-04-20T20:25:02Z"}
$ echo $?
0

# Re-trigger while a run is still pending/running
$ continuo schedule trigger daily_ingest
{"error":{"code":"conflict","message":"schedule \"daily_ingest\" already has an active run","retryable":true}}
$ echo $?
4

# State service unreachable
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
  exists, no concurrent run, …) are enforced by the state service. The
  CLI only translates gRPC status codes into exit codes and JSON shapes.
- **No retries, no backoff.** The `retryable` bit is advice for the
  caller. The CLI exits immediately so the caller (agent, shell loop,
  CI step) owns the retry policy.
