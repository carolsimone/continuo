# Run-lifecycle benchmark harness

## Purpose

The harness measures continuo's run lifecycle — timing, Redis message volume, and per-service CPU and memory — with identical settings before and after each phase of the run-lifecycle consolidation, so the phases are compared on the same scenarios and the same metrics.

Every node of a synthetic DAG is a real Kubernetes Job with the production dbt pod shape. The Job runs a stand-in `dbt` (`image/`) that prints a line and exits at once (after a 30 s sleep in the `slow30` image), fails on purpose for tables named `fail_*`, and fails a table named `once_*` on its first attempt only (a retry's Job name ends in `-r<attempt>`). Run time is therefore almost entirely the platform's own overhead.

## Targets

`BENCH_TARGET` selects where the harness runs:

| Target | Environment | Used for |
|---|---|---|
| `compose` (default) | The local compose stack with its kind cluster | Harness smoke, `dag-500`, the dependency-outage scenarios, the whole-DAG test fan-out (`test-2000`), the width sweep, fail-once and the two start failures |
| `k8s` | A Helm install reached through `BENCH_KUBECONFIG` | The synthetic scale scenarios, the width sweep and fail-once |

Environment knobs:

| Variable | Default | Meaning |
|---|---|---|
| `BENCH_TARGET` | `compose` | `compose` or `k8s` |
| `BENCH_KUBECONFIG` | — | kubeconfig of the install (`k8s`, required) |
| `BENCH_K8S_NAMESPACE` | `continuo` on `k8s`, `default` on `compose` | Namespace of the services (`k8s`) or of the task Jobs (`compose`) |
| `BENCH_KUBE_CONTEXT` | `kind-continuo` | kubectl context of the kind cluster (`compose`) |
| `BENCH_KIND_CLUSTER` | `continuo` | kind cluster that receives the images (`compose`) |
| `BENCH_REDIS_POD` | — | Redis pod of the install (`k8s`, required) |
| `BENCH_REDIS_CONTAINER_NAME` | `redis` | Container inside `BENCH_REDIS_POD` |
| `BENCH_PG_POD` | — | Postgres pod that holds release-controller's, state's and execution-controller's databases (`k8s`, required by the export, the release check and every rep's hand-off breakdown) |
| `BENCH_SSH_HOST` | — | ssh destination on the k3s node; `build_image.sh` imports the images into its containerd (`k8s`, required) |
| `CONTINUO_CLI` | `cli/bin/continuo` | CLI binary (`compose`); on `k8s` the harness runs the CLI inside `deploy/agent-chat` |
| `BENCH_SCENARIOS` | all | Scenario names to run, comma- or space-separated (for example `cascade-2000,cancel-500`) |
| `BENCH_QUIET_WINDOWS_UTC` | `2215-2345` | UTC ranges in which no rep starts on `k8s` (the install's scheduled runs); a range may cross midnight |
| `BENCH_IDLE_S` | `300` | Idle window before rep 1 of a scenario |
| `BENCH_SETTLE_S` | `15` | Pause before each later rep and after each run |
| `BENCH_RUN_TIMEOUT_S` | `3600` (`2700` in `outage.sh`) | Longest wait for one run; a run still live then is cancelled and waited for before anything else starts |
| `BENCH_SECOND_RUN_S` | `90` | How long the cancel scenario's second run lives before it is cancelled |
| `BENCH_CANCEL_WAIT_S` | `300` | How long an interrupted scenario waits for its cancelled run to stop before it reports the run as still live |
| `BENCH_START_TIMEOUT_S` | `120` | Start timeout `start_local_services.sh` gives execution-controller (`compose`), in seconds; a version without a start timeout ignores it |

`discover.sh` prints what the harness reads from a `k8s` install and lists the candidate datastore pods; secrets are reported only as resolved or missing.

## Safety on a shared install

The harness announces every topology through release-controller's `announce-topology` command, run inside the install (`kubectl exec deploy/release-controller` on `k8s`, the `release-controller` container on `compose`): it writes the topology artifact, takes the next promotion seq and queues `release.promoted:v2`, without moving `current_prod`. A later real promotion therefore always takes a larger seq and wins over a benchmark topology. An announcement swaps the orchestrator's whole topology: nodes missing from it are retired, and retired nodes that no run used in the last seven days are deleted with their code-version and failure-history links. The harness therefore never announces a topology that omits a live node:

1. `export_topology.sh` reads `current_prod`'s release id and the nodes of the artifact it points at (`announce-topology --print-current`) and writes `restore.json`: the live release's nodes, with dbt test nodes left out, as the orchestrator leaves them out of its graph.
2. `preflight.sh` records the schedule list and refuses to continue unless every scheduled export node is in the live schedule graphs, every node of those graphs is in the export, and the export carries no test node.
3. Every bench payload is `topology_io.py union` of `restore.json` and the bench DAG; the union refuses a bench node or schedule that a live node already uses.
4. Before each injection, `run_baseline_dev.sh` checks that `current_prod` still names the exported release and stops otherwise.
5. On every exit after the first injection, a signal or a failed step included, `run_baseline_dev.sh` stops the running scenario, cancels its live bench run and runs `restore.sh`. `restore.sh` re-announces `current_prod`'s own artifact under a fresh promotion seq (`announce-topology --reannounce-current`), waits until the schedule list equals the recorded one, and compares the live graphs with the export again; when a release was promoted meanwhile, it re-exports that release for the comparison and compares the graphs only.

If the machine running the harness dies mid-run, no exit handler runs. Recover by cancelling the live bench run (`continuo schedule cancel <bench schedule> <reason>`) and running `restore.sh OUT_DIR` with the same environment. Start long runs with `nohup` or in `tmux` so a closed terminal does not end them.

No scenario rep starts inside a quiet window around the install's own scheduled runs: `BENCH_QUIET_WINDOWS_UTC`, comma-separated `HHMM-HHMM` ranges in UTC (default `2215-2345`, around a 23:00 UTC daily run). The dependency-outage scenarios cut a datastore off the network and run only on the local compose stack.

A benchmark leaves these traces on a shared install: its runs stay in state's run history; its retired bench nodes stay in Neo4j until the seven-day run sweep and disappear at a later promotion; its Job logs stay in the logs bucket under service `bench`.

## Prerequisites

The machine that runs the harness stays awake for the whole run: a sleeping host pauses the local stack and the sampler, and on a `k8s` install it pauses the polling and the tunnel. Wrap long runs in `caffeinate -is` on macOS and keep the lid open.


- Both targets: `python3` 3.9 or newer, `kubectl`, Docker with BuildKit.
- `compose`: the stack from `bash scripts/setup.sh` (in a fresh worktree, `bash scripts/ensure-dev-env.sh` first) and the CLI from `make -C cli build`. `run_baseline_kind.sh` starts state, orchestrator, release-controller and execution-controller itself through `start_local_services.sh` (release-controller's outbox publisher is what publishes the announcement `announce-topology` queues); execution-controller runs in compose and reaches MinIO through the Docker bridge, which the task pods in kind can also reach. The release-controller image must be built from the branch under test (`docker compose build release-controller`), since `inject.sh` runs the `announce-topology` that image carries.
- `k8s`: a kubeconfig for the install, ssh access to its k3s node for the image import, and `docker buildx` for the `linux/amd64` build. Obtaining cluster credentials is managed outside this repository.

## Usage

```bash
# Local stack: harness smoke, dag-500, the two outage scenarios, test-2000, the width sweep, fail-once
# and the two start failures -> OUT_DIR/report.md
BENCH_TARGET=compose scripts/bench/run_baseline_kind.sh [OUT_DIR]

# k8s install: every synthetic scenario on the union topology, restore on exit -> OUT_DIR/report.md
BENCH_TARGET=k8s BENCH_KUBECONFIG=... BENCH_REDIS_POD=... BENCH_PG_POD=... BENCH_SSH_HOST=... \
  scripts/bench/run_baseline_dev.sh [OUT_DIR]

# A subset, for example to repeat two scenarios
BENCH_SCENARIOS=cascade-2000,cancel-500 BENCH_TARGET=k8s ... scripts/bench/run_baseline_dev.sh [OUT_DIR]

# One scenario on a topology that is already published
scripts/bench/run_scenario.sh NAME PAYLOAD SCHEDULE run|test REPS OUT_DIR [CANCEL_AFTER_S]

# Two result folders side by side, scenario by scenario -> markdown on stdout
python3 scripts/bench/compare.py BEFORE_DIR AFTER_DIR [--labels A,B] > comparison.md

```

`make bench-test` runs the unit tests and shellcheck; `make guards` includes it.

## Scenarios

| Scenario | Shape | Measures |
|---|---|---|
| `smoke` | 3 nodes in a chain | Harness check; its rep-1 idle window is the idle footprint |
| `chain-500` | 50 levels × 10 nodes, fan-in 1 | Hand-off cost between levels |
| `dag-500` | 10 levels × 50 nodes, fan-in 2 | Per-level overhead and throughput |
| `dag-2000` | 20 levels × 100 nodes, fan-in 2 | The same at four times the size |
| `test-2000` | `dag-2000` run as a whole-DAG test, on the local stack | Flat fan-out and admission; 2,000 near-simultaneous Jobs can saturate a small cluster's API server, so it never runs on a shared install |
| `cascade-2000` | `dag-2000` under a single failing root | Retries of the root, then 1,999 skips |
| `cancel-500` | `dag-500` with 30 s nodes | Cancel after 60 s, immediate re-trigger, second run cancelled after 90 s; overlap of the two runs' Jobs |
| `outage-postgres`, `outage-neo4j` | `dag-500` | The datastore container is disconnected from its networks for 10 minutes during a run and reconnected with its aliases, so its data survives (local stack only); final status and dropped messages |
| `sweep-10`, `sweep-50`, `sweep-100`, `sweep-200` | 5 levels of 10, 50, 100 or 200 nodes, fan-in 2 | Hand-off against level width; the report fits its growth per task of width |
| `fail-once` | 5 levels × 50 nodes, fan-in 2; every node fails its first attempt and succeeds on its retry | Retry turnaround |
| `start-image` | 10 nodes on an image tag that does not exist (local stack only) | A pod that cannot pull its image: attempts, overlapping attempts, how the run ends |
| `start-quota` | 10 nodes while a `pods: 0` ResourceQuota (`k8s/no-pods-quota.yaml`) holds the Job namespace (local stack only) | A Job that never gets a pod: whether the start timeout ends it, and how the run ends |

However a start failure ends, normally or on INT, TERM or HUP, `bench_start_failure` cancels its live run, stops its runner, deletes its Jobs and then removes the start-quota scenario's quota; further INT, TERM and HUP are ignored until it has, and a step that fails is reported without skipping the next. If the machine running the harness dies during start-quota, remove the Jobs and the quota by hand: `kubectl --context kind-continuo -n default delete jobs -l schedule=bench-start-quota`, then `kubectl --context kind-continuo -n default delete resourcequota bench-no-pods`; until then the `pods: 0` quota blocks every Job of the local stack.

## Metric definitions

| Metric | Definition |
|---|---|
| wall | Trigger to the last Job's finish; empty when no Job of the run finished |
| busy | Union of the Jobs' lifetimes (creation to finish) |
| idle | wall − busy |
| first start | Trigger to the first Job's creation |
| finalize | Last Job's finish to the CLI reporting the run as not running (1 s poll) |
| hand-off | A node's first Job creation minus its last upstream Job's finish |
| messages per task | Sum of the streams' `entries-added` deltas ÷ executed tasks |
| messages per attempt | The same sum ÷ the run's Jobs |
| retries | Jobs created after each task's first Job, whether or not the earlier Job finished |
| retry turnaround | A failed Job's finish to the creation of the next Job of the same task |
| overlapping attempts | Jobs created while an earlier Job of the same task had not finished; a Job that never finished counts as unfinished, and a Job gone from the capture is not counted (the capture holds no lifetime for it) |
| observe | An upstream's last Job finishing to execution-controller recording its result (`deployments.outcome_at`) |
| round trip | That result recorded to the node's first deployment queued (`deployments.created_at`), both on execution-controller's clock |
| to dispatch, to queue | The round trip split at state's first dispatch of the node (`tasks.dispatched_at`) |
| admit | The node's first deployment queued to its Job created (`deployments.deployed_at`) |
| CPU-seconds per 1,000 tasks | CPU integrated over the run (5 s samples) ÷ executed tasks × 1,000 |
| peak memory | Highest per-service memory during the run, replicas summed per instant |
| idle footprint | Mean CPU and memory over the smoke scenario's idle window |
| max sample gap | Longest interval between two sampling instants during the run; the report lists reps whose gap exceeds 60 s, since their timings span a host sleep or stall |

A retried node counts from its first Job's creation to its last Job's finish. A failed Job ends at its `Failed` condition's transition time.

The hand-off steps take, for each node, the upstream whose last Job finished last, that upstream's last deployment and the node's first. Each rep reads them with `run_rows.py` from execution-controller's and state's databases (`deployments-rep<N>.json`, `tasks-rep<N>.json`); a read that fails leaves the steps empty and keeps its error output beside them (`deployments-rep<N>.err`, `tasks-rep<N>.err`). Observe, to dispatch and to queue compare two services' clocks, which share a host on the local stack.

## Run-lifecycle targets

`report.py` ends with the run-lifecycle targets and the measured value of each; `compare.py` shows both sides. They are reported, never gated.

| Measure | Target | Read from |
|---|---|---|
| Hand-off p50 at width 100 | ≤ 5 s | `sweep-100` |
| Growth of the hand-off per task of level width | < 0.1 s | least-squares slope of the hand-off p50 over the `sweep-*` widths |
| Retry turnaround p50 | ≤ 5 s | `fail-once` |
| Messages per attempt | 4 | `sweep-100` |
| Overlapping attempts | 0 | the largest of any scenario |

## Run history

`sql/run_history.sql` computes wall, busy, idle, first-start and finalize percentiles from state's database (`continuo_run_controller`), read-only:

```bash
psql -v kinds=cron,trigger -v days=30 -f scripts/bench/sql/run_history.sql
```

## Results

Each run writes raw captures, per-rep `rep<N>.json` files and `report.md` under `.bench/` at the repository root, which is gitignored. Results are summarised in the run-lifecycle design notes; raw output is never committed.

`compare.py` reads two such folders, for example the same target measured before and after a change. For every scenario found in both it prints each side's p50 and the change (`after − before`, and the relative change against the size of `before` when that is non-zero), for the timing metrics, each service's CPU-seconds per 1,000 tasks and peak memory, and the idle footprint. Both sides leave out the reps whose sampling paused for over 60 s, and the output names them. When the two sides' final statuses differ, it says so, since their timings then cover different work. A scenario found on one side only is listed as such.
