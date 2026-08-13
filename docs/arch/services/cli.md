# continuo CLI

## Purpose

`continuo` is a standalone command-line client of the system, intended primarily for an LLM chat agent to call (humans use it too). It exposes a small set of schedule and model-node operations and a self-description command, composing the public gRPC surfaces of `state` and `orchestrator`.

It provides:
- `schedule list` — every schedule and its last-run status
- `schedule trigger <name>` — start a new run of a schedule now
- `schedule test <name>` — run dbt tests for every tested model in a schedule now
- `schedule build <name>` — dependency-ordered `dbt build` (run + test) of a schedule now, cascade-skipping descendants of a failed node
- `schedule cancel <name> <reason>` — stop the active run of a schedule, recording why
- `schedule status <name>` — the per-node status of a schedule's latest run
- `schedule graph <name>` — the dependency graph (nodes and edges) of a schedule
- `node history <service> <schema> <table>` — the recent run history of one model node, enriched with the `content_hash` each run executed (in `--human` mode, rendered as an `EXECUTED_HASH` column)
- `node trigger <service> <schema> <table>` — run one model node now using its latest metadata
- `node test <service> <schema> <table>` — run one model node's dbt tests now using its latest metadata
- `node build <service> <schema> <table>` — run and test one model node now (`dbt build`) using its latest metadata
- `node versions <service> <schema> <table>` — the node's recorded code-version history, newest first; `--include-code` (default off) opts into fetching `raw_code`/`compiled_code`
- `node diff <service> <schema> <table> --from <seq> --to <seq>` — a server-rendered diff between two recorded versions of the node
- `node upstream-changes <service> <schema> <table>` — the node's ancestors' most recent code changes, most-recently-changed first
- `node code-units [<unit-id>] [--service <s> --schema <s> --table <t>]` — a shared-code unit's version chain, or a node's units' chains concatenated
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
| `--orchestrator-endpoint` | `CONTINUO_ORCHESTRATOR_ADDR` | gRPC address of the `orchestrator` service. The orchestrator client dials with `grpc.MaxCallRecvMsgSize(32 MiB)`, sized to match the server's largest legitimate response (`GetNodeVersions` with `include_code=true` at the default page size), well above the gRPC library's 4 MiB default. |
| `--timeout` | `CONTINUO_TIMEOUT` | gRPC deadline applied to each call |
| `--human` | — | emit human text on stderr instead of JSON on stdout |
| `--json` | — | forward-compatibility no-op; JSON is the default output |
| — | `CONTINUO_ACTOR` | initiating identity forwarded to `state` on every mutating command — recorded as `cancelled_by` on `schedule cancel`, and as the run initiator on `schedule trigger`/`test`/`build` and `node trigger`/`test`/`build` (via the `x-continuo-user-id` gRPC metadata header; `schedule cancel` carries it in the request's `cancelled_by` field); empty selects the `state` service's own system identity |

## Commands and the RPC each consumes

| Command | Service | Public RPC(s) |
|---|---|---|
| `schedule list` | `state` | `ListAllSchedules` |
| `schedule trigger <name>` | `state` | `TriggerSchedule` |
| `schedule test <name>` | `state` | `TriggerSchedule` (`operation=test`) |
| `schedule build <name>` | `state` | `TriggerSchedule` (`operation=build`) |
| `schedule cancel <name> <reason>` | `state` | `CancelSchedule` |
| `schedule status <name>` | `state` | `ListAllSchedules` + `ListTasks` (composed client-side) |
| `schedule graph <name>` | `orchestrator` | `GetScheduleGraph` |
| `node history <service> <schema> <table>` | `state` + `orchestrator` | `ListNodeRuns`, then `GetNodeRunHistory` (composed client-side) |
| `node trigger <service> <schema> <table>` | `state` | `TriggerSingleNodeRun` |
| `node test <service> <schema> <table>` | `state` | `TriggerSingleNodeRun` (`operation=test`) |
| `node build <service> <schema> <table>` | `state` | `TriggerSingleNodeRun` (`operation=build`) |
| `node versions <service> <schema> <table>` | `orchestrator` | `GetNodeVersions` |
| `node diff <service> <schema> <table>` | `orchestrator` | `GetNodeVersionDiff` |
| `node upstream-changes <service> <schema> <table>` | `orchestrator` | `GetUpstreamChanges` |
| `node code-units [<unit-id>]` | `orchestrator` | `GetCodeUnitVersions` |
| `describe` | — | none (pure introspection of the cobra tree) |

`schedule status` has no dedicated server RPC. It resolves the schedule name to its latest `run_id` via `ListAllSchedules`, then pages through `ListTasks` (page size 200) for that run, collecting every node's status. The composition is entirely client-side; the services expose no combined endpoint.

`schedule cancel <name> <reason>` requires a non-empty `reason` positional describing why the run is being stopped; a blank reason is rejected before the RPC is sent. The cancelling identity (`cancelled_by`) is sourced from the `CONTINUO_ACTOR` environment variable rather than a command argument — when it is unset or empty, the `state` service records its own system identity as the canceller.

`node history` and `node trigger` both address a single model node by its `<service> <schema> <table>` identity triple. `node history` requests the newest 50 runs of that node (the page size is fixed; `state` clamps it) and passes every `NodeRun` field through to JSON, so the caller sees the image and manifest version each run used alongside its status and any error. An unknown node yields `{"runs":[]}`, not an error.

Each run's JSON is additionally enriched with `content_hash` — the code that run executed — joined client-side against `orchestrator`'s `GetNodeRunHistory` by run id, filtered server-side to the same `--operation` the state query used (so newer executions of a different operation cannot fill the orchestrator's page and starve the join for the rows `state` actually returned). `state` remains the primary source of truth for run history: if the join to `orchestrator` fails for any reason (the service is unreachable, the node has no recorded version history, or any other error), `node history` still returns `state`'s rows in full, with every `content_hash` simply omitted rather than failing the command. `content_hash` is also omitted per-run for any run that predates the stamp on the `:EXECUTES` edge. Under `--human`, the joined hash is rendered too — each line ends with an `EXECUTED_HASH` column (the first 12 characters of `content_hash`, or `-` when unavailable) — so human mode surfaces the same join JSON mode does rather than fetching and discarding it.

`node versions` lists a node's recorded code-version history, newest first (ordered by when each version's code was *first* promoted — a revert does not move a version's position in this list, only which row's `is_current` is set), via `GetNodeVersions`; `--limit` defaults to 20 and is clamped server-side to 200. `--include-code` (default off) requests `raw_code`/`compiled_code`; left off, the orchestrator never fetches those columns from Neo4j, keeping the default response well under the CLI's 32 MiB receive limit even though a single version's `compiled_code` can run to 256 KiB. `node diff --from <seq> --to <seq>` renders a server-side diff between two of a node's recorded versions via `GetNodeVersionDiff`; `--from`/`--to` address a `version_seq` — a stable per-node handle, not a chronological position, so `--from` need not be the older version. `node upstream-changes` lists a node's ancestors' most recent code changes, most-recently-changed first — ranked by each ancestor's *effective* last-change time, so a revert reports the actual before/after of the revert rather than the two versions' original creation order — via `GetUpstreamChanges`; results are capped server-side at the 5 most-recently-changed ancestors (enforced before any version body leaves Neo4j) with each diff independently capped at 8 KiB (contract, not a client option), `--depth` defaults to 3 hops and is rejected above 10, and `--since` (RFC3339) excludes ancestors whose effective last-change time predates it. `node code-units` lists a shared-code unit's version chain via `GetCodeUnitVersions`, addressed by exactly one of a positional `<unit-id>` or `--service`/`--schema`/`--table` (which resolves the named node's current units first and, in one batched orchestrator round trip, concatenates each of their chains); supplying both or neither selector is a usage error.

All four code-version commands return `not_found` (exit 3) for an unknown node/unit/version and, for a known node/unit with no recorded history, an empty list rather than an error — the same degrade-to-empty contract `orchestrator` applies server-side.

`node trigger` starts a fresh run of the node using its latest topology metadata; it deliberately exposes only the "latest" mode of `TriggerSingleNodeRun`, not the snapshot-of-a-previous-run mode. The initiating identity is forwarded from `CONTINUO_ACTOR` as the `x-continuo-user-id` gRPC metadata header (the CLI cannot import `state`'s identity package, so the header name is mirrored as a local constant). The command reports acceptance, not completion: `state` durably records the new run and its outbox event synchronously, but if the node is absent from the topology that failure is surfaced asynchronously downstream, not by this command.

`node test` is identical to `node trigger` except it calls `TriggerSingleNodeRun` with `operation=test`, so the target runs its dbt tests (`dbt test --select <node>`) rather than being built. It shares the "latest" mode restriction, the `CONTINUO_ACTOR` identity forwarding, and the acceptance-not-completion contract with `node trigger`: if the node has no dbt tests defined (`test_count == 0`), that failure also surfaces asynchronously, not by this command.

`schedule test` is identical to `schedule trigger` except it calls `TriggerSchedule` with `operation=test`: every node in the schedule with dbt tests defined runs `dbt test` independently (a flat fan-out, not the schedule's normal dependency order); nodes with no tests defined are silently skipped rather than failed.

`node build` is identical to `node trigger` except it calls `TriggerSingleNodeRun` with `operation=build`, so the target runs `dbt build` (materialize and test in one invocation) rather than only `dbt run` or only `dbt test`. It shares the "latest" mode restriction, the `CONTINUO_ACTOR` identity forwarding, and the acceptance-not-completion contract with `node trigger` and `node test`. Unlike `node test`, a node with no dbt tests defined is still built; there is no `no_tests` skip for build.

`schedule build` is identical to `schedule trigger` except it calls `TriggerSchedule` with `operation=build`: the whole-DAG run uses `dbt build` in the schedule's normal dependency order (unlike `schedule test`'s flat fan-out), and cascade-skips the descendants of any node that fails, rather than fanning out independently. As with `node build`, a node with no tests defined is still built.

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
| `state` | `ListAllSchedules`, `ListTasks`, `TriggerSchedule`, `CancelSchedule`, `ListNodeRuns`, `TriggerSingleNodeRun` (`operation` in `{"", "test", "build"}`) |
| `orchestrator` | `GetScheduleGraph`, `GetNodeVersions`, `GetNodeVersionDiff`, `GetUpstreamChanges`, `GetCodeUnitVersions`, `GetNodeRunHistory` |
