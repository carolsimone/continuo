# continuo CLI

## Purpose

`continuo` is a standalone command-line client of the system, intended primarily for an LLM chat agent to call (humans use it too). It exposes a small set of schedule and model-node operations and a self-description command, composing the public gRPC surfaces of `state` and `orchestrator`.

It provides:
- `schedule list` — every schedule and its last-run status
- `schedule trigger <name>` — start a new run of a schedule now
- `schedule cancel <name> <reason>` — stop the active run of a schedule, recording why
- `schedule status <name>` — the per-node status of a schedule's latest run
- `schedule graph <name>` — the dependency graph (nodes and edges) of a schedule
- `node history <service> <schema> <table>` — the recent run history of one model node
- `node trigger <service> <schema> <table>` — run one model node now using its latest metadata
- `describe` — a machine-readable catalog of every command, for LLM discovery

It owns no storage, constructs no Redis client, and runs no server. It is invoked on demand and exits.

**Runtime**: Go (separate module `github.com/carolsimone/continuo/cli`, deliberately outside the parent `go.work`).

## Pure gRPC client

The CLI consumes services only through their public gRPC interfaces. It imports no backend internal implementation — no `state/*`, `orchestrator/*`, `pkg/*`, or any domain/service/adapter package. Its only dependencies are cobra, grpc, protobuf, and the generated stubs of the public `.proto` contracts.

Those contracts are vendored under `cli/proto/`, and the stubs are regenerated (`make proto`) from the vendored copy, never from a service's source tree. New behavior composes public RPCs client-side rather than reaching into service internals.

## Owned Storage

None.

## Configuration

| Flag | Environment variable | Purpose |
|---|---|---|
| `--endpoint` | `CONTINUO_STATE_ADDR` | gRPC address of the `state` service |
| `--orchestrator-endpoint` | `CONTINUO_ORCHESTRATOR_ADDR` | gRPC address of the `orchestrator` service |
| `--timeout` | `CONTINUO_TIMEOUT` | gRPC deadline applied to each call |
| `--human` | — | emit human text on stderr instead of JSON on stdout |
| `--json` | — | forward-compatibility no-op; JSON is the default output |
| — | `CONTINUO_ACTOR` | initiating identity forwarded to `state` — recorded as `cancelled_by` on `schedule cancel`, and as the run initiator on `node trigger` (via the `x-continuo-user-id` gRPC metadata header); empty selects the `state` service's own system identity |

## Commands and the RPC each consumes

| Command | Service | Public RPC(s) |
|---|---|---|
| `schedule list` | `state` | `ListAllSchedules` |
| `schedule trigger <name>` | `state` | `TriggerSchedule` |
| `schedule cancel <name> <reason>` | `state` | `CancelSchedule` |
| `schedule status <name>` | `state` | `ListAllSchedules` + `ListTasks` (composed client-side) |
| `schedule graph <name>` | `orchestrator` | `GetScheduleGraph` |
| `node history <service> <schema> <table>` | `state` | `ListNodeRuns` |
| `node trigger <service> <schema> <table>` | `state` | `TriggerSingleNodeRun` |
| `describe` | — | none (pure introspection of the cobra tree) |

`schedule status` has no dedicated server RPC. It resolves the schedule name to its latest `run_id` via `ListAllSchedules`, then pages through `ListTasks` (page size 200) for that run, collecting every node's status. The composition is entirely client-side; the services expose no combined endpoint.

`schedule cancel <name> <reason>` requires a non-empty `reason` positional describing why the run is being stopped; a blank reason is rejected before the RPC is sent. The cancelling identity (`cancelled_by`) is sourced from the `CONTINUO_ACTOR` environment variable rather than a command argument — when it is unset or empty, the `state` service records its own system identity as the canceller.

`node history` and `node trigger` both address a single model node by its `<service> <schema> <table>` identity triple. `node history` requests the newest 50 runs of that node (the page size is fixed; `state` clamps it) and passes every `NodeRun` field through to JSON, so the caller sees the image and manifest version each run used alongside its status and any error. An unknown node yields `{"runs":[]}`, not an error.

`node trigger` starts a fresh run of the node using its latest topology metadata; it deliberately exposes only the "latest" mode of `TriggerSingleNodeRun`, not the snapshot-of-a-previous-run mode. The initiating identity is forwarded from `CONTINUO_ACTOR` as the `x-continuo-user-id` gRPC metadata header (the CLI cannot import `state`'s identity package, so the header name is mirrored as a local constant). The command reports acceptance, not completion: `state` durably records the new run and its outbox event synchronously, but if the node is absent from the topology that failure is surfaced asynchronously downstream, not by this command.

## Output contract

The CLI is LLM-first: on success it emits a single JSON object to **stdout**. Under `--human` it emits a compact human-readable summary to **stderr** instead, leaving stdout empty.

Errors are emitted as a structured JSON envelope (to stdout, or to stderr under `--human`) and mapped to a fixed exit code by error code:

| Error code | Exit code | Meaning |
|---|---|---|
| `usage` | 2 | argument/flag problem detected before the RPC, or `InvalidArgument` from the server |
| `not_found` | 3 | named schedule does not exist |
| `conflict` | 4 | a run is already active for the schedule (trigger), or there is no active run to cancel / it already finished (cancel) (`FailedPrecondition` / `AlreadyExists` / `Aborted`) |
| `unavailable` | 5 | the target service is unreachable (`Unavailable` / `DeadlineExceeded`) |
| `internal` | 6 | unexpected server error (`Internal` / `Unknown`) |

A successful command exits `0`. The error envelope carries `code`, `message`, and a `retryable` flag; `conflict` and `unavailable` are marked retryable. gRPC status codes are translated to this vocabulary in one place so no command imports `google.golang.org/grpc/codes` directly.

## Self-description

`continuo describe` serializes the cobra command tree into a machine-readable catalog so an LLM can discover the entire surface in one call. For each runnable command it emits the path, `short`, `long`, positional `args`, `flags` (name, shorthand, usage, default — including inherited global flags), `examples`, and the `output_schema` / `exit_codes` annotations.

The catalog is derived from the command tree, not hand-maintained: a new command appears automatically. Cobra's auto-generated `help` and `completion` commands and any hidden commands are excluded. A test enforces that every first-class command carries a non-empty `Long` and at least one `Example`, and that each declared `output_schema` matches the JSON the command actually emits.

## Inbound Interfaces

None. The CLI runs no server and consumes no Redis streams.

## Outbound gRPC calls

| Service | Methods used |
|---|---|
| `state` | `ListAllSchedules`, `ListTasks`, `TriggerSchedule`, `CancelSchedule`, `ListNodeRuns`, `TriggerSingleNodeRun` |
| `orchestrator` | `GetScheduleGraph` |
