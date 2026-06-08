# Topology

## Static System View

```mermaid
flowchart LR
  subgraph ControlPlane
    ST[state]
    OR[orchestrator]
    EC[executor-controller]
    KC[k8s-controller]
    MC[manifest-controller]
    RC[release-controller]
    UI[ui-service]
  end

  subgraph Storage
    STDB[(Postgres: state)]
    ORPG[(Postgres: orchestrator)]
    GRDB[(Neo4j: graph)]
    ECPG[(Postgres: executor_deployments/executor_outbox/message_processing/cancelled_schedules)]
    KCPG[(Postgres: k8s_outbox/message_processing)]
    S3[(S3/LocalStack)]
    K8S[(Kubernetes API)]
    R[(Redis Streams)]
  end

  ST --> STDB
  OR --> GRDB
  OR --> ORPG
  EC --> ECPG
  KC --> KCPG

  ST <--> R
  OR <--> R
  EC <--> R
  KC <--> R
  MC <--> R
  RC <--> R

  OR --> ST
  EC --> ST
  KC --> ST
  UI --> ST
  UI --> OR

  EC --> K8S
  KC --> K8S
  KC --> S3
  MC --> S3
  RC --> S3
```

## Redis Topology

```mermaid
flowchart TD
  RR[release.requested:v1]
  MLC[manifest.loaded.candidate:v1]
  RP[release.promoted:v1]
  SL[schedules.loaded:v1]
  SS[scheduler.started:v1]
  RED[run.entries.dispatched:v1]
  TRR[trigger.rerun:v1]
  TRB[trigger.rebase:v1]
  TSN[trigger.single_node_run:v1]
  QM[query.model:v1]
  ED[node.deployed:v1]
  KCV[check.k8s:v1]
  TR[retry.task:v1]
  TF[task.failed:v1]
  UT[node.updated:v1]

  RC[release-controller] --> RR
  RR --> MC[manifest-controller]
  MC --> MLC
  MLC --> RC
  RC --> RP
  RP --> OR[orchestrator]
  OR --> SL
  SL --> ST[state]

  ST --> SS
  SS --> OR
  OR --> RED
  RED --> ST

  ST --> TRR
  TRR --> OR

  ST --> TRB
  TRB --> OR

  ST --> TSN
  TSN --> OR

  OR --> QM

  QM --> EC[executor-controller]
  TR --> EC

  EC --> ED
  ED --> KC[k8s-controller]

  KC --> KCV
  KCV --> KC
  KC --> TR
  KC --> TF
  KC --> UT

  UT --> OR

  SC_EV[schedule.cancelled:v1]
  ST --> SC_EV
  SC_EV --> OR
  SC_EV --> EC
  SC_EV --> KC
```

## Ownership Boundaries

| Domain concern | Owning service | Primary store |
|---|---|---|
| Scheduler/task/task-execution truth | `state` | Postgres |
| Dependency topology and run projection | `orchestrator` | Neo4j |
| Node completion, downstream unlock, run finalization | `orchestrator` | Postgres outbox + Neo4j |
| Schedule/bootstrap dispatch intents | `orchestrator` | Postgres outbox |
| Deployment intents / inbound dedup | `executor-controller` | Postgres (`executor_deployments`, `executor_outbox`, `message_processing`) |
| Runtime status / retry orchestration | `k8s-controller` | Postgres (`k8s_outbox`, `message_processing`) |
| Cancelled schedule guard (local copy) | `orchestrator`, `executor-controller`, `k8s-controller` | Postgres (`cancelled_schedules`) |
| Candidate manifest parsing and dependency resolution | `manifest-controller` | Redis + S3 |
| UI/API facade | `ui-service` | none (gRPC reads/writes to `state` and `orchestrator`) |
| `topology_generation` counter and run-isolation snapshot | `orchestrator` | Postgres (`topology_state`) + Neo4j (`:TopologyRoot`, `Run`, `EXECUTES`) |

## Key Architectural Rules

- `state` owns task and scheduler status; other services must mutate that state through gRPC.
- `orchestrator` owns table topology (Neo4j) and run-time `EXECUTES` status projection; it also handles node completion events and downstream unlocking.
- `release.promoted:v1` carries a full topology snapshot. `orchestrator` reconciles Neo4j against it by retiring missing `Table` nodes from the active graph while preserving historical `Run` snapshots.
- The dedicated Flyway migration image artifact runs the shared `db/migration/` trees sequentially for `continuo_state`, `continuo_executor`, `continuo_orchestrator`, and `continuo_k8s`.
- Redis carries orchestration events between services. Redis requires password authentication in all environments (local docker-compose: `--requirepass continuo`; production: injected via Kubernetes secret as `REDIS_PASSWORD`). All services must supply `REDIS_PASSWORD` or the process will refuse to start (see `pkg/config.Validator`).
- The controller services use local Postgres outbox and dedup tables to make cross-service messaging reliable.
- The `deploy/infra` Helm chart provisions the shared infrastructure stack (`Postgres`, `Redis`, `Neo4j`) as cluster-internal defaults and initializes the service databases in one Postgres instance. Local docker-compose uses `POSTGRES_PASSWORD=continuo` (superuser) and `REDIS_PASSWORD=continuo`.
- `manifest-controller` parses candidate manifests and resolves dependencies; it does not orchestrate execution.
- Topology enters production exclusively through releases: `POST /releases` on `release-controller` emits `release.requested:v1`, `manifest-controller` parses the candidate manifests and publishes `manifest.loaded.candidate:v1`, and after validation `release-controller` promotes via `release.promoted:v1`.
- `ui-service` is read-only apart from the run-trigger write endpoints (`TriggerRerun`, `TriggerRebase`, `TriggerSingleNodeRun`, `TriggerSchedule`), which it issues as gRPC calls to `state`. It constructs no Redis client.
- `schedule.cancelled:v1` is published by `state` via the outbox processor and consumed independently by `orchestrator`, `executor-controller`, and `k8s-controller` (each with its own consumer group). Each consumer maintains a local `cancelled_schedules` Postgres table populated from this stream and uses it as a hot-path guard to suppress further processing for cancelled runs. Rows are swept after a configurable TTL (default 24h).

## Topology Versioning

### Generation Counter

Each `release.promoted:v1` that changes the current release atomically increments `topology_state.topology_generation` (a monotonic `BIGINT` in orchestrator's Postgres). The counter is stamped on:

- The `:TopologyRoot {id:'singleton'}` Neo4j node — holds the current generation and the full `service_metadata` JSON map (`{svc: {manifest_version, image_tag}}`).
- Each `Table` node in Neo4j (`image_tag`, `node_type`, `topology_generation` properties).
- Each `Run` node in Neo4j (`topology_generation`, `service_metadata` properties) — **copied from `:TopologyRoot` at `SnapshotGraph` time**.
- Each `EXECUTES` edge in Neo4j (`image_tag`, `manifest_version` properties) — **stamped from the Table at `SnapshotGraph` time**.

### Run Isolation (Lazy Generation Switch)

`SnapshotGraph` is the atomic switch point. When a new `release.promoted:v1` arrives mid-run:

1. The generation counter increments in Postgres and `:TopologyRoot` is updated.
2. The in-flight `Run` node already has its `topology_generation` stamped — it is **not** updated.
3. All `EXECUTES` edges for the in-flight run carry the `image_tag` from the snapshot — they are **not** updated.
4. The next `SnapshotGraph` call (for the next run) copies the new generation and `service_metadata` from `:TopologyRoot`.

This guarantees that every K8s Pod in a run uses the exact image tag that was current when the run was started.

### Content-Addressed Image Tags

Image tags reach the topology through the release path. Each team's CI sends its own service's image tag in the single-service `POST /releases` request body; `release-controller` records it and, when the release activates, assembles the full per-service tag map from the changed service's tag plus every other service's `service_prod` pointer. `manifest-controller` parses the candidate manifests and leaves `image_tag` empty; `release-controller` joins the assembled per-service tags onto the candidate topology before validation and carries them through `release.promoted:v1`. The orchestrator stamps those tags onto every `:Table` node and `EXECUTES` edge.

`executor-controller` reads `image_tag` from `query.model:v1` stream fields and refuses to construct a K8s Pod if the tag is empty. There is no fallback to `"latest"`.
