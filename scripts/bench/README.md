# Run-lifecycle benchmark harness

## Purpose

The harness measures continuo's run lifecycle — timing, Redis message volume, and per-service CPU and memory — with identical settings before and after each phase of the run-lifecycle consolidation, so the phases are compared on the same scenarios and the same metrics.

Every node of a synthetic DAG is a real Kubernetes Job with the production dbt pod shape. The Job runs a stand-in `dbt` (`image/`) that prints a line and exits at once (after a 30 s sleep in the `slow30` image), and fails on purpose for tables named `fail_*`. Run time is therefore almost entirely the platform's own overhead.

## Targets

`BENCH_TARGET` selects where the harness runs:

| Target | Environment | Used for |
|---|---|---|
| `compose` (default) | The local compose stack with its kind cluster | Harness smoke, `dag-500`, the dependency-outage scenarios, the whole-DAG test fan-out (`test-2000`) |
| `k8s` | A Helm install reached through `BENCH_KUBECONFIG` | The synthetic scale scenarios |

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
| `BENCH_PG_POD` | — | Postgres pod that holds release-controller's database (`k8s`, required by the export) |
| `BENCH_SSH_HOST` | — | ssh destination on the k3s node; `build_image.sh` imports the images into its containerd (`k8s`, required) |
| `CONTINUO_CLI` | `cli/bin/continuo` | CLI binary (`compose`); on `k8s` the harness runs the CLI inside `deploy/agent-chat` |
| `BENCH_SCENARIOS` | all | Scenario names to run, comma- or space-separated (for example `cascade-2000,cancel-500`) |
| `BENCH_QUIET_WINDOWS_UTC` | `2215-2345` | UTC ranges in which no rep starts on `k8s` (the install's scheduled runs); a range may cross midnight |
| `BENCH_IDLE_S` | `300` | Idle window before rep 1 of a scenario |
| `BENCH_SETTLE_S` | `15` | Pause before each later rep and after each run |
| `BENCH_RUN_TIMEOUT_S` | `3600` (`2700` in `outage.sh`) | Longest wait for one run; a run still live then is cancelled and waited for before anything else starts |
| `BENCH_SECOND_RUN_S` | `90` | How long the cancel scenario's second run lives before it is cancelled |

`discover.sh` prints what the harness reads from a `k8s` install and lists the candidate datastore pods; secrets are reported only as resolved or missing.

## Safety on a shared install

Publishing a topology to `release.promoted:v1` swaps the orchestrator's whole topology: nodes missing from the payload are retired, and retired nodes that no run used in the last seven days are deleted with their code-version and failure-history links. The harness therefore never publishes a payload that omits a live node:

1. `export_topology.sh` reads release-controller's `current_prod` and writes `restore.json`, the payload that re-announces the live release unchanged: every node marked unchanged, `secret_ref` kept only when set, and dbt test nodes left out, as a promotion leaves them out (`current_prod` keeps them for validation only).
2. `preflight.sh` records the schedule list and refuses to continue unless every scheduled export node is in the live schedule graphs, every node of those graphs is in the export, and the export carries no test node.
3. Every bench payload is `topology_io.py union` of `restore.json` and the bench DAG; the union refuses a bench node or schedule that a live node already uses.
4. Before each injection, `run_baseline_dev.sh` checks that `current_prod` still names the exported release and stops otherwise.
5. On every exit after the first injection, a signal or a failed step included, `run_baseline_dev.sh` stops the running scenario, cancels its live bench run and runs `restore.sh`. `restore.sh` re-announces `restore.json`, waits until the schedule list equals the recorded one, and compares the live graphs with the export again; when a release was promoted meanwhile, it re-exports and re-announces that release and compares the graphs only.

If the machine running the harness dies mid-run, no exit handler runs. Recover by cancelling the live bench run (`continuo schedule cancel <bench schedule> <reason>`) and running `restore.sh OUT_DIR` with the same environment. Start long runs with `nohup` or in `tmux` so a closed terminal does not end them.

No scenario rep starts inside a quiet window around the install's own scheduled runs: `BENCH_QUIET_WINDOWS_UTC`, comma-separated `HHMM-HHMM` ranges in UTC (default `2215-2345`, around a 23:00 UTC daily run). The dependency-outage scenarios cut a datastore off the network and run only on the local compose stack.

A benchmark leaves these traces on a shared install: its runs stay in state's run history; its retired bench nodes stay in Neo4j until the seven-day run sweep and disappear at a later promotion; its Job logs stay in the logs bucket under service `bench`.

## Prerequisites

The machine that runs the harness stays awake for the whole run: a sleeping host pauses the local stack and the sampler, and on a `k8s` install it pauses the polling and the tunnel. Wrap long runs in `caffeinate -is` on macOS and keep the lid open.


- Both targets: `python3` 3.9 or newer, `kubectl`, Docker with BuildKit.
- `compose`: the stack from `bash scripts/setup.sh` (in a fresh worktree, `bash scripts/ensure-dev-env.sh` first) and the CLI from `make -C cli build`. `run_baseline_kind.sh` starts state, orchestrator and execution-controller itself through `start_local_services.sh`; execution-controller runs in compose and reaches MinIO through the Docker bridge, which the task pods in kind can also reach.
- `k8s`: a kubeconfig for the install, ssh access to its k3s node for the image import, and `docker buildx` for the `linux/amd64` build. Obtaining cluster credentials is managed outside this repository.

## Usage

```bash
# Local stack: harness smoke, dag-500 and the two outage scenarios -> OUT_DIR/report.md
BENCH_TARGET=compose scripts/bench/run_baseline_kind.sh [OUT_DIR]

# k8s install: every synthetic scenario on the union topology, restore on exit -> OUT_DIR/report.md
BENCH_TARGET=k8s BENCH_KUBECONFIG=... BENCH_REDIS_POD=... BENCH_PG_POD=... BENCH_SSH_HOST=... \
  scripts/bench/run_baseline_dev.sh [OUT_DIR]

# A subset, for example to repeat two scenarios
BENCH_SCENARIOS=cascade-2000,cancel-500 BENCH_TARGET=k8s ... scripts/bench/run_baseline_dev.sh [OUT_DIR]

# One scenario on a topology that is already published
scripts/bench/run_scenario.sh NAME PAYLOAD SCHEDULE run|test REPS OUT_DIR [CANCEL_AFTER_S]

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

## Metric definitions

| Metric | Definition |
|---|---|
| wall | Trigger to the last Job's finish |
| busy | Union of the Jobs' lifetimes (creation to finish) |
| idle | wall − busy |
| first start | Trigger to the first Job's creation |
| finalize | Last Job's finish to the CLI reporting the run as not running (1 s poll) |
| hand-off | A node's first Job creation minus its last upstream Job's finish |
| messages per task | Sum of the streams' `entries-added` deltas ÷ executed tasks |
| CPU-seconds per 1,000 tasks | CPU integrated over the run (5 s samples) ÷ executed tasks × 1,000 |
| peak memory | Highest per-service memory during the run, replicas summed per instant |
| idle footprint | Mean CPU and memory over the smoke scenario's idle window |
| max sample gap | Longest interval between two sampling instants during the run; the report lists reps whose gap exceeds 60 s, since their timings span a host sleep or stall |

A retried node counts from its first Job's creation to its last Job's finish. A failed Job ends at its `Failed` condition's transition time.

## Run history

`sql/run_history.sql` computes wall, busy, idle, first-start and finalize percentiles from state's database (`continuo_state`), read-only:

```bash
psql -v kinds=cron,trigger -v days=30 -f scripts/bench/sql/run_history.sql
```

## Results

Each run writes raw captures, per-rep `rep<N>.json` files and `report.md` under `.bench/` at the repository root, which is gitignored. Results are summarised in the run-lifecycle design notes; raw output is never committed.
