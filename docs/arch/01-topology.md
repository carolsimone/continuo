# Topology

## Static System View

```mermaid
flowchart LR
  subgraph ControlPlane
    ST[state]
    GR[graph]
    SC[startup-controller]
    DC[dependency-controller]
    EC[executor-controller]
    KC[k8s-controller]
    MC[manifest-controller]
    UI[ui-service]
  end

  subgraph Storage
    STDB[(Postgres: state)]
    GRDB[(Neo4j: graph)]
    SCPG[(Postgres: startup_outbox)]
    DCPG[(Postgres: message_processing/outbox/published_messages)]
    ECPG[(Postgres: deployment_outbox/processed_events)]
    KCPG[(Postgres: k8s_status_outbox/processed_events)]
    S3[(S3/LocalStack)]
    K8S[(Kubernetes API)]
    R[(Redis Streams)]
  end

  ST --> STDB
  GR --> GRDB
  SC --> SCPG
  DC --> DCPG
  EC --> ECPG
  KC --> KCPG

  ST <--> R
  SC <--> R
  DC <--> R
  EC <--> R
  KC <--> R
  MC <--> R

  SC --> ST
  SC --> GR
  DC --> ST
  DC --> GR
  EC --> ST
  KC --> ST
  MC --> GR
  UI --> ST
  UI --> GR

  EC --> K8S
  KC --> K8S
  KC --> S3
  MC --> S3
```

## Redis Topology

```mermaid
flowchart TD
  UG[update.graph:v1]
  SL[schedules.loaded:v1]
  SS[scheduler.started:v1]
  CR[command.rerun:v1]
  QM[query.model:v1]
  ED[executor.deployed:v1]
  KCV[k8s.check:v1]
  TR[task.retry:v1]
  TF[task.failed:v1]
  UT[update.table:v1]

  UG --> MC[manifest-controller]
  MC --> SL
  SL --> ST[state]

  ST --> SS
  ST --> CR

  SS --> SC[startup-controller]
  CR --> SC

  SC --> QM
  DC[dependency-controller] --> QM

  QM --> EC[executor-controller]
  TR --> EC

  EC --> ED
  ED --> KC[k8s-controller]

  KC --> KCV
  KCV --> KC
  KC --> TR
  KC --> TF
  KC --> UT

  UT --> DC
```

## Ownership Boundaries

| Domain concern | Owning service | Primary store |
|---|---|---|
| Scheduler/task/task-execution truth | `state` | Postgres |
| Dependency topology and run projection | `graph` | Neo4j |
| Schedule/bootstrap dispatch intents | `startup-controller` | Postgres outbox |
| Downstream unlock / inbound dedup | `dependency-controller` | Postgres |
| Deployment intents / inbound dedup | `executor-controller` | Postgres |
| Runtime status / retry orchestration | `k8s-controller` | Postgres |
| Manifest ingestion | `manifest-controller` | Redis + graph gRPC + filesystem/S3 |
| Read-only UI/API facade | `ui-service` | none |

## Key Architectural Rules

- `state` owns task and scheduler status; other services must mutate that state through gRPC.
- `graph` owns table topology and run-time `EXECUTES` status projection; other services mutate it through graph gRPC.
- The dedicated Flyway migration image artifact runs the shared `db/migration/` trees sequentially for `continuo_state`, `continuo_startup`, `continuo_executor`, `continuo_dependency`, and `continuo_k8s`.
- Redis carries orchestration events between services.
- The controller services use local Postgres outbox and dedup tables to make cross-service messaging reliable.
- The `deploy/infra` Helm chart provisions the shared infrastructure stack (`Postgres`, `Redis`, `Neo4j`) as cluster-internal defaults and initializes the service databases in one Postgres instance.
- `manifest-controller` is topology ingest, not execution orchestration.
- `ui-service` should remain read-only.
