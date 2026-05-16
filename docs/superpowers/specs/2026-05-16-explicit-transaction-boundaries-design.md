# Explicit Transaction Boundaries — Design

**Status:** Approved
**Date:** 2026-05-16
**Branch:** `stream-consumer-std`

## Problem

The split-consumer refactor now runs multiple Redis consumers concurrently inside both `executor-controller` and `k8s-controller`.

Each service still wires those parallel consumers through one handler instance backed by one mutable `PostgresUnitOfWork`:

- `executor-controller`: `query.model:v1` and `retry.task:v1` both call the same `DeployHandler`
- `k8s-controller`: `node.deployed:v1` and `check.k8s:v1` both call the same `CheckStatusHandler`

Today each UOW stores a single mutable transaction slot (`tx *sqlx.Tx` plus `inTx bool`) and exposes implicit transaction state through `Begin()`, `Commit()`, `Rollback()`, and repository getters. That makes the UOW unsafe to share across concurrent handler calls: the second `Begin()` sees `inTx == true`, returns `transaction already in progress`, and the failed Redis message remains pending until it is reclaimed later.

PostgreSQL itself supports parallel transactions. The bug is in the service-side abstraction: one shared Go object models exactly one current transaction.

## Goals

- Allow parallel consumers inside a single service process to handle messages concurrently without sharing mutable transaction state.
- Make transaction scope explicit at the call site instead of hidden inside a long-lived UOW instance.
- Ensure repositories used inside a transaction are actually transaction-bound.
- Preserve current service behavior, outbox writes, dedup semantics, and existing Redis flow.
- Add regression coverage for concurrent handler calls so this class of bug does not reappear.

## Non-goals

- Reworking Redis pending-entry recovery behavior.
- Broadly refactoring every UOW in the monorepo.
- Changing external contracts, stream names, payloads, or database schema.
- Adding worker pools or increasing per-stream concurrency beyond the existing split-consumer behavior.

## Considered Approaches

### 1. Give each consumer its own handler and UOW

This is the smallest repair: keep the current mutable UOW API, but instantiate separate handler/UOW pairs for the parallel consumers.

**Pros**
- Very small patch.
- Matches the workaround already used in `orchestrator/main.go`.

**Cons**
- Keeps the footgun in place.
- Only fixes current wiring, not future concurrent handler reuse.
- Leaves transaction ownership implicit and easy to misuse.

### 2. Create a fresh UOW per message

Inject a factory into handlers and allocate a new mutable UOW for every `Handle()` call.

**Pros**
- Supports concurrency safely.
- Smaller change than redesigning the UOW API.

**Cons**
- Still preserves the hidden “current transaction” model.
- Requires every future caller to remember to allocate a fresh UOW at the right lifetime.
- Does not make the transaction boundary visible in handler code.

### 3. Make transaction boundaries explicit (**chosen**)

Replace the mutable `Begin/Commit/Rollback` API with a callback-style boundary:

```go
err := h.txRunner.WithinTransaction(ctx, func(tx uow.Transaction) error {
    return tx.OutboxRepo().Create(ctx, entry)
})
```

Each invocation creates its own database transaction and passes transaction-scoped repositories into the callback. Handlers no longer rely on hidden shared state.

**Pros**
- Solves the current bug and removes the underlying footgun.
- Makes transaction ownership obvious in the handler.
- Maps naturally to PostgreSQL concurrency: one handler call → one DB transaction.
- Ensures tx-bound repositories are used where atomicity matters.

**Cons**
- Touches more code than the narrower fixes.
- Helper methods in `k8s-controller` need a transaction parameter threaded through them.

## Chosen Architecture

### `executor-controller`

`service/uow` will expose:

- `Transaction` — provides `OutboxRepo()`
- `TransactionRunner` — provides `WithinTransaction(ctx, fn)`
- `PostgresTransactionRunner` — opens a fresh `*sqlx.Tx` per call, constructs tx-backed repositories, commits on success, rolls back on error

`DeployHandler.Handle()` will build the outbox entry, then call `WithinTransaction()` and insert through the transaction-scoped outbox repository.

The executor outbox repository will support both DB-backed and tx-backed executors. This fixes the existing mismatch where `DeployHandler` begins a transaction but `OutboxRepo()` currently writes via the shared DB handle instead of the active transaction.

### `k8s-controller`

`service/uow` will expose:

- `Transaction` — provides `OutboxRepo()` and `ProcessedEventsRepo()`
- `TransactionRunner` — provides `WithinTransaction(ctx, fn)`
- `PostgresTransactionRunner` — opens a fresh `*sqlx.Tx` per call and passes tx-bound repos into the callback

`CheckStatusHandler.Handle()` will keep the K8s status read outside the SQL transaction, then perform dedup, cancellation guard handling, outbox writes, and commit inside `WithinTransaction()`.

The helper methods that create outbox entries (`handleSucceeded`, `handleFailedPermanent`, `handleFailedWithRetry`, `handleRunning`, `handleUnknown`) will accept the transaction object explicitly instead of reaching back into handler-owned mutable state.

### Main wiring

Both services can keep a single handler instance shared by multiple consumers because the handler itself will no longer contain mutable transaction state. The handler owns a concurrency-safe transaction runner backed by `*sqlx.DB`, and each `Handle()` call gets its own short-lived transaction.

## Data Flow

### Executor message handling

1. Consumer parses Redis message.
2. `DeployHandler` builds the deployment outbox entry.
3. `WithinTransaction()` opens a fresh transaction.
4. Transaction-scoped outbox repository inserts the row.
5. Callback returns nil; runner commits.
6. Consumer records processed event and acknowledges the Redis message as before.

### K8s status handling

1. Consumer parses Redis message.
2. Handler reads K8s status outside Postgres.
3. `WithinTransaction()` opens a fresh transaction.
4. Transaction-scoped processed-events repository claims the inbound message.
5. If duplicate, callback returns nil and no outbox rows are added.
6. Otherwise transaction-scoped outbox repository writes the outcome rows.
7. Callback returns nil; runner commits all writes atomically.
8. Consumer acknowledges the Redis message as before.

## Error Handling

- `WithinTransaction()` returns begin failures directly.
- If the callback returns an error, the runner rolls back and returns that callback error.
- If commit fails, the runner returns the commit error.
- Rollback is best-effort after callback failure; the original callback error remains the primary failure.
- K8s status lookups still fail before opening a transaction, preserving current behavior.
- Duplicate k8s messages still short-circuit safely after the transaction-scoped dedup insert.

## Testing Strategy

### Unit tests

- Add focused tests for the new transaction runner behavior:
  - commit on successful callback
  - rollback on callback error
  - transaction-scoped repositories are available inside the callback
- Update handler fakes to the new explicit transaction API.
- Preserve existing handler behavior tests.

### Regression tests

- Add concurrent handler tests that run two `Handle()` calls against the same handler instance and prove both succeed.
- Cover both:
  - executor deploy handling
  - k8s status handling

These tests should fail against the current shared mutable UOW design because the second call would hit `transaction already in progress`.

### Verification

- Run targeted package tests for both services.
- Run formatting.
- Rebuild graph metadata after code changes, per repo policy.
- Update the relevant architecture docs because service transaction behavior changes.

## Expected Consequences

- Parallel stream consumers become safe without serializing work.
- The handler/UOW boundary becomes harder to misuse in future refactors.
- Executor writes finally become genuinely transactional instead of only appearing to be.
- The refactor remains internal: no external API, stream contract, or deployment surface changes are required.
