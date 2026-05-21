# Design: Port ownership & dependency-inversion cleanup

**Issue:** #72 — `Fix dependency-inversion in k8s-controller: relocate adapter-owned ports + PostgresUnitOfWork`
**Surfaced by:** DDD layer-flow review of PR #71 (k8s-controller #58 standardization)
**Scope:** repo-wide consistency pass across `k8s-controller`, `orchestrator`, `state`
**Status:** Proposed

---

## 1. Problem

The inbound event flow in `k8s-controller` is clean, but two structural wrinkles invert the Clean Architecture dependency direction. Both are shared with `orchestrator` (and partly `state`), so the fix is a repo-wide convention, not a one-service patch.

### 1.1 Ports declared inside outbound-adapter packages

`k8s-controller/service/handlers/check_status_handler.go` imports adapter packages purely because the interfaces it depends on live there:

| Port | Declared in (adapter) | Consumed by |
|---|---|---|
| `CancelledSchedulesRepository` | `k8s-controller/adapters/postgres/cancelled_schedules_repository.go:11-15` | `check_status_handler.go:42,51,81` |
| `LogUploader` | `k8s-controller/adapters/s3/client.go:13-16` | `check_status_handler.go:40,49,167` |

The handler's import block:

```go
postgresadapter "github.com/carolsimone/continuo/k8s-controller/adapters/postgres" // line 10
s3adapter       "github.com/carolsimone/continuo/k8s-controller/adapters/s3"        // line 11
```

So the package dependency arrow points **application → adapter** — the exact inversion of `uow.UnitOfWork`, which is correctly owned by the application layer. The interface still gives runtime decoupling, but the *compile-time* arrow is backwards: an adapter is supposed to depend on a port, never declare one.

### 1.2 Concrete `PostgresUnitOfWork` lives in the application layer

In all three services the concrete `PostgresUnitOfWork` (imports `sqlx`, binds the outbox table) sits in `service/uow/uow.go` next to the `UnitOfWork` *interface*:

| Service | Interface | Concrete impl | Outbox table |
|---|---|---|---|
| k8s-controller | `service/uow/uow.go:16-22` | `service/uow/uow.go:25-97` | `k8s_outbox` |
| orchestrator | `service/uow/uow.go:13-20` | `service/uow/uow.go:22-114` | `orchestrator_outbox` |
| state | `service/uow/uow.go:29-49` | `service/uow/uow.go:52-163` | (via injected repos) |

The application layer thus mixes a port with an infrastructure implementation (`sqlx`, table binding).

## 2. The standard (the rule we commit to)

A **port is owned by the innermost layer whose vocabulary it speaks; adapters only implement ports, never declare them.** This is the negative claim all three reference schools agree on (Cockburn: a port is by definition application-owned; Martin: boundary interfaces are owned by the inner layer; Evans: repository interfaces belong to the domain). The only fork — *which* inner layer — is decided by whether the port carries domain meaning.

| Port kind | Example | Owner layer | Package |
|---|---|---|---|
| Repository over a domain notion | `CancelledSchedulesRepository`, `RunRepository` | domain | `domain/repository` |
| Technical / orchestration collaborator | `LogUploader`, `OutboxPublisher`, `Clock`, `UnitOfWork` | application | `service/ports` (UoW interface stays in `service/uow`) |
| Concrete implementation | `PostgresUnitOfWork`, S3 uploader | adapter | `adapters/*` |

Discriminator examples:
- `CancelledSchedulesRepository` — a repository over a domain notion (cancelled schedules) → **domain** (`domain/repository`).
- `LogUploader` — uploading run logs to S3 is not in the ubiquitous language; it is a technical collaborator the use case happens to need → **application** (`service/ports`).
- `UnitOfWork` — a transaction-orchestration boundary, not a domain concept → **application**; interface stays in `service/uow`, only the concrete impl moves to the adapter.

This rule maps onto packages the repo already uses: `orchestrator` already has `domain/repository/`; all three already keep the `UnitOfWork` interface in `service/uow`.

## 3. Current-state inventory

| Service | UoW interface loc | Concrete UoW loc | Adapter-owned ports consumed by app | Existing ports home |
|---|---|---|---|---|
| **k8s-controller** | `service/uow` | `service/uow` ❌ | `CancelledSchedulesRepository` (postgres), `LogUploader` (s3) ❌ | none ❌ |
| **orchestrator** | `service/uow` | `service/uow` ❌ | none — `Neo4jClient`, `ScheduleAndRunListReader`, `DriftAwareRunReader` are adapter-internal, consumed only by adapters ✅ | `domain/repository/`; `SnapshotService` port co-located in `service/handlers/deps.go` ✅ |
| **state** | `service/uow` | `service/uow` ❌ | `task_execution_recorded_handler.go` imports `adapters/postgres` for the `TaskExecution` row type ❌ | top-level `ports/` (`RunRepository`, `OutboxPublisher`, `Clock`) — diverges from `domain/repository` |

## 4. Changes per service

### 4.1 k8s-controller (primary target)

1. **`CancelledSchedulesRepository` interface → `k8s-controller/domain/repository/`.** `adapters/postgres` keeps the concrete repo and now *implements* the domain port.
2. **`LogUploader` interface → new `k8s-controller/service/ports/`.** `adapters/s3` implements it.
3. **`PostgresUnitOfWork` → `k8s-controller/adapters/postgres`.** The `UnitOfWork` interface stays in `service/uow`. Update the wiring in `main.go` / lifecycle to construct it from the adapter package.
4. **Result:** `check_status_handler.go` imports `domain/repository` + `service/ports` + `service/uow` and **zero `adapters/*` packages**.

### 4.2 orchestrator

1. **`PostgresUnitOfWork` → `orchestrator/adapters/postgres`.** Interface stays in `service/uow`.
2. **Handlers are already clean.** The `Neo4jClient`, `ScheduleAndRunListReader`, and `DriftAwareRunReader` interfaces are declared in adapter packages but are consumed *only by adapters* (gRPC/neo4j wiring), not by `service/handlers`. That is a legitimate adapter-internal seam, not an application→adapter inversion, so they **stay put**. Document this distinction so it is not "fixed" by mistake later.
3. Keep `SnapshotService` where it is (`service/handlers/deps.go`) — a technical/application port co-located with its consumer is consistent with §2 (it could equally live in `service/ports`; co-location with a single consumer is an accepted equivalent and not worth the churn).

### 4.3 state (full reconciliation)

1. **`PostgresUnitOfWork` → `state/adapters/postgres`.** Interface stays in `service/uow`.
2. **Fold the top-level `ports/` package into the standard:**
   - `RunRepository` (domain repository) → `state/domain/repository/` (new package, mirroring orchestrator).
   - `OutboxPublisher`, `Clock`, `ScheduleCatalogRepository` — classify each: `OutboxPublisher` and `Clock` are technical/orchestration ports → `state/service/ports/`. `ScheduleCatalogRepository`, being a repository over a domain notion, → `state/domain/repository/`.
   - Delete the top-level `state/ports/` package once empty.
3. **Fix `task_execution_recorded_handler.go:9`.** It imports `adapters/postgres` to use the `postgres.TaskExecution` row struct (`:40`). Introduce a port-level input type (a plain struct owned by the application/domain port, e.g. on the `TaskCollection`/relevant repository port) so the handler constructs that instead, and the adapter maps it to its row type. The handler then imports no `adapters/*` package.
4. **Low-level tracker repos exposed through the UoW** (`SchedulerTrackerRepository`, `TaskTrackerRepository`, `TaskExecutionRepository`, `NodeRunRepository`, declared in `adapters/postgres`): these are consumed by `PostgresUnitOfWork`. Once `PostgresUnitOfWork` itself lives in `adapters/postgres`, an adapter declaring and consuming its own collaborator interfaces is adapter-internal and **does not** violate §2. They stay in `adapters/postgres`. The `UnitOfWork` *interface* (in `service/uow`) must expose only ports the application is allowed to see; audit its accessor signatures so no accessor returns an adapter-declared type to application code.

## 5. Mechanics & risks

- **Pure relocation, no behavioural change.** Interfaces and the concrete UoW move packages; method sets are unchanged. The risk is import cycles and wiring breakage, not logic.
- **Import-cycle check.** Moving the concrete `PostgresUnitOfWork` into `adapters/postgres` is safe because the adapter already depends on `sqlx`, `pkgoutbox`, and `messageprocessing`; it now also imports `service/uow` for the interface it satisfies. Confirm `service/uow` does not import `adapters/postgres` after the move (it must not — that would re-invert the arrow). The interface package should import only `pkgoutbox` / `messageprocessing` / `sqlx.Tx`-level types as today.
- **Constructor placement.** `NewPostgresUnitOfWork` moves with the struct into `adapters/postgres`. Every call site (service `main.go` / `internal/lifecycle`) updates its import; the AST wiring detector in `pkg/streams/wiring_test.go` is unaffected (no stream literals involved).
- **state's `TaskExecution` port type.** Define the application-facing input type in the port's package (domain/application), not in the adapter. The adapter owns the *row* mapping; the port owns the *intent* type. This is the only change that adds a tiny mapping function rather than a move.

## 6. Tests

The change is structural, so the primary guarantee is **the build and existing suites stay green** after relocation. Add targeted guards for the invariants this design establishes:

- **Compile-time guard (per service):** an `_ = port.X(adapterImpl)` style assignment (or existing wiring) proving the adapter type still satisfies the relocated port. Already implicit in wiring; make it explicit where a test double previously leaned on the adapter package.
- **Import-direction guard:** extend the existing architecture/wiring test approach (cf. `pkg/streams/wiring_test.go`) with an AST check that `service/handlers/*.go` in all three services import **no** `adapters/*` package. This is the durable regression guard for §1.1 — it makes the acceptance criterion machine-checked, so the inversion cannot silently return.
- **state handler test:** `task_execution_recorded_handler` test constructs the new port-level input type (not `postgres.TaskExecution`), proving the handler no longer depends on the adapter type.
- Run each service's full suite under the repo's integration conventions (`TESTCONTAINERS_RYUK_DISABLED=true`, `-tags integration`) plus the e2e suite per `tests/e2e/README.md` before merge.

## 7. Acceptance criteria mapping

| Criterion (issue #72) | Covered by |
|---|---|
| `service/handlers` imports no `adapters/*`; collaborators reached through app/domain-owned ports | §4.1.4, §4.3.3, §6 import-direction guard |
| Concrete `*UnitOfWork` lives under `adapters/` | §4.1.3, §4.2.1, §4.3.1 |
| Convention applied consistently across k8s-controller, orchestrator, state | §2 standard + §4 per-service |
| `docs/arch/` updated to describe port ownership | §8 |

## 8. docs/arch reconciliation

Per the repository working agreement, before the task is complete update `docs/arch/*` to describe the port-ownership convention as the **current** state (no "previously/relocated in PR" phrasing — arch docs read as a fresh snapshot):

- Add/replace the layering description so it states: domain repository ports in `domain/repository`, technical/application ports in `service/ports` (UoW interface in `service/uow`), concrete implementations in `adapters/*`, with the dependency arrow always adapter → port.
- Reconcile any k8s-controller / orchestrator / state structure diagrams that currently show `PostgresUnitOfWork` or the relocated interfaces in their old packages.

## 9. Files touched (indicative)

**k8s-controller**
- `domain/repository/` (new) — `CancelledSchedulesRepository` interface.
- `service/ports/` (new) — `LogUploader` interface.
- `adapters/postgres/` — implement `CancelledSchedulesRepository`; receive `PostgresUnitOfWork` (moved from `service/uow`).
- `adapters/s3/client.go` — drop the local `LogUploader` decl; implement the `service/ports` one.
- `service/uow/uow.go` — keep interface only; remove concrete impl.
- `service/handlers/check_status_handler.go` — swap imports to ports.
- `main.go` / `internal/lifecycle` — UoW construction import.

**orchestrator**
- `adapters/postgres/` — receive `PostgresUnitOfWork`.
- `service/uow/uow.go` — interface only.
- wiring import update.

**state**
- `domain/repository/` (new) — `RunRepository`, `ScheduleCatalogRepository`.
- `service/ports/` — `OutboxPublisher`, `Clock` (+ task-execution input type).
- `ports/` (top-level) — deleted once empty.
- `adapters/postgres/` — receive `PostgresUnitOfWork`; map the port-level task-execution type.
- `service/uow/uow.go` — interface only; audit accessor signatures.
- `service/handlers/task_execution_recorded_handler.go` — use port type, drop adapter import.

**cross-cutting**
- `pkg/streams/wiring_test.go` (or a sibling arch test) — import-direction guard for `service/handlers → adapters/*`.
- `docs/arch/*` — §8.
