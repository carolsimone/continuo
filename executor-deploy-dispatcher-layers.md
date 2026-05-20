# executor-controller deploy dispatcher — layer interaction & DDD assessment

A high-level map of the code touched by the #68 refactor, organised by Clean Architecture / DDD layer, so the layering and dependency directions can be judged at a glance. The repo's working agreement (see `CLAUDE.md`): *domain stays independent of infrastructure; databases/Redis/K8s/serialization live in adapters; application/use-case code depends on ports, not concrete infrastructure; handlers are thin and delegate.*

---

## 1. Layers and where the touched code lives

```
┌──────────────────────────────────────────────────────────────────────────┐
│ COMPOSITION ROOT (wiring only — no business logic)                          │
│   executor-controller/main.go          (M)  builds + starts processor +     │
│                                              dispatcher goroutines           │
│   executor-controller/config/config.go (M)  + MaxConcurrentJobs              │
└──────────────────────────────────────────────────────────────────────────┘
        │ constructs & injects ports/adapters downward
        ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ APPLICATION / USE-CASE LAYER  (executor-controller/service/)                │
│                                                                            │
│  Handlers (thin orchestration)                                             │
│    handlers/query_model_handler.go   (M)  ─┐                               │
│    handlers/retry_task_handler.go    (M)  ─┤ call                          │
│    handlers/create_deployment.go     (M, renamed from deploy_outbox.go)    │
│        └─ writes a pending Deployment via the UoW port                     │
│                                                                            │
│  Unit of Work (transaction boundary port + impl)                           │
│    service/uow/uow.go   (M)  + DeploymentsRepo() accessor                   │
│    service/uow/fake.go  (M)  in-memory double for tests                     │
│                                                                            │
│  Deploy command-queue use case  (service/deployer/)                        │
│    deployer/dispatcher.go   (N)  use-case service: drain queue → deploy →  │
│                                  write announcements; owns retry/cap policy │
│    deployer/repository.go   (N)  PORT (interface) over executor_deployments │
│    deployer/deploy_job.go   (N)  queued payload value object               │
│    deployer/deployment.go   (N)  queue row model                           │
│    deployer/postgres.go     (N)  ADAPTER: Postgres impl of the port *      │
│        (* colocated with the port — see Deviation D1)                       │
└──────────────────────────────────────────────────────────────────────────┘
        │ depends on ▼ (interfaces only)            ▲ implemented by
        ▼                                           │
┌──────────────────────────────────────────────────────────────────────────┐
│ DOMAIN LAYER  (executor-controller/domain/)                                 │
│    domain/event/event.go  (M)  NodeUpdated (N), JobDeployed — typed events  │
│    domain/event/deploy_task.go  (REMOVED)                                   │
└──────────────────────────────────────────────────────────────────────────┘
        ▲ implemented by
        │
┌──────────────────────────────────────────────────────────────────────────┐
│ ADAPTER / INFRASTRUCTURE LAYER  (executor-controller/adapters/)             │
│    adapters/publisher/outbox_publisher.go (M)  implements pkg/outbox.       │
│        Publisher → marshal payload + Redis XADD (no K8s, no fanout)         │
│    adapters/k8s/client.go (M)  + CountActiveJobs; clientset widened to      │
│        kubernetes.Interface (testability)                                   │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│ SHARED KERNEL  (pkg/) — unchanged by this PR, consumed as ports/contracts   │
│    pkg/outbox    Publisher, Repository, Processor, Entry, Executor          │
│    pkg/events    TaskStatusUpdated (+ shared payloads)                       │
│    pkg/streams   versioned stream-name constants                            │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│ PERSISTENCE / DEPLOY SURFACES                                               │
│    db/migration/executor/V13__init_executor_deployments.sql (N)            │
│    docker-compose.yml, deploy/app/values.yaml,                              │
│    tests/e2e/k8s/executor-controller-deployment.yaml  (M: MAX_CONCURRENT…)  │
└──────────────────────────────────────────────────────────────────────────┘

(N) = new file, (M) = modified, REMOVED = deleted
```

**Dependency rule check:** every arrow points *inward* (toward domain) or *toward an interface*. The application layer (`dispatcher`, handlers) names only ports (`deployer.Repository`, `outbox.Repository`, `uow.UnitOfWork`) and the `K8sDeployer` / `outbox.Publisher` interfaces — never a concrete `*sqlx.DB`, `*goredis.Client`, or `*k8s.K8sClient` directly in its logic. Concrete infrastructure is injected at the composition root (`main.go`).

---

## 2. Runtime interaction (one task, across layers)

**Write path (inbound command → queued row):**
```
Redis query.model:v1 / retry.task:v1
  → adapters/redis binding (infra)            unchanged
      → service/handlers/*Handler (application, thin)
          → uow.UnitOfWork.DeploymentsRepo().Create(pending Deployment)   [Postgres tx]
      commit  — a PURE database write; no K8s, no Redis side effect in the tx
```

**Drain path (queued command → side effect → events):**
```
service/deployer/Dispatcher.ProcessBatch (application use case)
  → K8sDeployer.CountActiveJobs(...)          [adapters/k8s]   — concurrency cap
  → deployer.Repository.GetDueBatch(...)       [Postgres, FOR UPDATE SKIP LOCKED]
  → K8sDeployer.CreateQueryJob(...)            [adapters/k8s]   — the command effect
  on success, in ONE tx:
      → outbox.Repository.Create(task_status_updated=RUNNING)  [domain event → outbox]
      → outbox.Repository.Create(node_deployed)
      → deployer.Repository.MarkDeployed(...)
  on exhaustion/permanent error, in ONE tx:
      → outbox.Repository.Create(task_status_updated=FAILED)
      → outbox.Repository.Create(node_updated=FAILED)
      → deployer.Repository.MarkFailed(...)
```

**Publish path (outbox → wire):**
```
pkg/outbox.Processor (shared)               unchanged
  → adapters/publisher.OutboxPublisher.Publish(entry)   [infra]
      → toValues(entry) maps event_type → domain event struct → ToMap()
      → Redis XADD to entry.StreamName, inject outbox_entry_id
```

The key structural change: the K8s deploy moved from *inside the publisher* (an infrastructure adapter doing a domain-ish workflow) to *its own application use-case service* (`Dispatcher`) backed by a command queue. The publisher is now a pure infrastructure translation (struct → map → XADD), identical in shape to state/orchestrator/k8s-controller.

---

## 3. DDD / Clean Architecture assessment

### Follows the pattern
- **Ports & adapters honoured.** Handlers and the Dispatcher depend on interfaces (`uow.UnitOfWork`, `deployer.Repository`, `outbox.Repository`, `outbox.Publisher`, `K8sDeployer`). Concrete `sqlx`/`go-redis`/`client-go` types appear only in adapters and `main.go`. The Dispatcher is unit-testable with a fake `K8sDeployer` + real Postgres, exactly because it codes to the interface.
- **Thin handlers.** `query_model_handler.go` / `retry_task_handler.go` only orchestrate (cancelled-schedule guard + one repo write); no SQL, Redis, or JSON in them. `create_deployment.go` is a single-purpose helper.
- **Command/effect vs event separation (CQRS-flavoured).** The K8s deploy is now modelled as a *command* on `executor_deployments` (a write-side operation queue) and is cleanly distinct from the *event* outbox (`executor_outbox`). This restores the transactional-outbox invariant: the inbound handler's transaction is a pure DB write, so a K8s call can never race a DB commit.
- **Single responsibility per unit.** `deployer` splits into payload / row / port / postgres-adapter / dispatcher, each small and independently testable.
- **Composition root.** All concrete wiring (DB, Redis, K8s client, namespace, cap, tick) is assembled in `main.go` and injected; nothing self-constructs its dependencies.
- **Uniformity.** Removing the executor-only `TerminalFailureHook` and the deploy-fanout means every service's `Publisher` now has the same contract — the interface no longer "lies" about what publishing does.

### Honest deviations / things to eyeball
- **D1 — adapter colocated with its port.** `service/deployer/postgres.go` is an infrastructure adapter (raw SQL, `sqlx`) living *inside* the application package next to `repository.go`, rather than under `adapters/`. Strict Clean Architecture would separate them. This is a deliberate repo-wide convention — `pkg/outbox` does exactly the same (`repository.go` + `postgres.go` together) — so it is consistent, but it is the main spot where the layer boundary is physical-package-fuzzy. The dependency direction is still correct (the port doesn't import the adapter).
- **D2 — serialization on domain events.** `domain/event` types (`NodeUpdated`, `JobDeployed`) carry `ToMap()` (a Redis-wire concern) and JSON tags. That is a small infrastructure leak into the domain layer. Pre-existing pattern across the codebase, unchanged by this PR — flagged only for completeness.
- **D3 — application payload references an adapter type.** `deployer.DeployJob.ToJobParams()` returns `k8s.JobParams` (an adapter type), so the application package imports the k8s adapter package. This is an outward (application → adapter) reference. It is narrow (one value-object conversion) and inherited from the old `DeployTask`, but a purist would invert it (define the params in the application layer and map in the adapter).
- **D4 — row model doubles as scan struct.** `deployer.Deployment` carries `db:` tags and is used both as the model passed across the port and as the `sqlx` scan target. Pragmatic and common in this repo; a stricter design would keep a persistence DTO separate from the domain/application model.

### Net
The change moves executor-controller *toward* the standard DDD/ports-and-adapters shape the rest of the system uses: it turns an infrastructure adapter that was secretly running a multi-step workflow into a first-class application use case behind ports, and demotes the publisher to pure translation. The deviations (D1–D4) are all pre-existing repo conventions rather than new debt introduced here.
