# Rerun HTTP → gRPC Migration

**Date:** 2026-04-10
**Status:** Approved
**Scope:** Replace the `POST /schedules/{id}/rerun` HTTP endpoint on the state service with a `TriggerRerun` gRPC method, keeping the transactional outbox → `command.rerun:v1` → startup-controller flow unchanged.

---

## Problem

The state service exposes two protocols: gRPC (port 50051) for all entity operations, and HTTP (port 8082) for the rerun trigger. The HTTP endpoint is a protocol island — it gets no gRPC interceptors, its contract is invisible to `state.proto`, and operators need to know about two ports. Everything else in the system is gRPC.

The original rationale for HTTP was `202 Accepted` semantics (fire-and-forget). This is weak: the same contract can be expressed as an empty gRPC response with no error.

---

## Design

### 1. Proto changes (`state.proto` and `ui-service/proto/state.proto`)

Add to the existing `StateService`:

```protobuf
rpc TriggerRerun(TriggerRerunRequest) returns (TriggerRerunResponse);

message TriggerRerunRequest {
  string schedule_id  = 1;
  string schema       = 2;
  string table_name   = 3;
  string service_name = 4;
}

message TriggerRerunResponse {}
```

The field names map 1:1 to the current HTTP request body and path param. No semantic change.

The same proto copy lives in `ui-service/proto/state.proto` — it receives the identical addition.

### 2. State service — new gRPC handler, HTTP cleanup

**New handler:** `state/internal/grpc/handlers/rerun_handler.go`

Move the logic from `state/adapters/http/rerun_handler.go:ServeHTTP()` into a new gRPC handler implementing `TriggerRerun`. Pre-flight guards and the atomic 3-table transaction (UPDATE `scheduler_tracker`, UPDATE `task_tracker`, INSERT `state_outbox`) are preserved exactly. Returns `TriggerRerunResponse{}` on success.

**HTTP cleanup:**
- Delete `state/adapters/http/rerun_handler.go` and its test file
- Remove the `rerunHandler` parameter from `http.NewServer()` — it becomes a no-arg, health-only server
- Drop the `/schedules/{schedule_id}/rerun` route registration from `state/adapters/http/server.go`

Port 8082 remains alive for `/health` only.

**Wire-up in `state/main.go`:**
- Instantiate `rerunHandler := handlers.NewRerunHandler(db, schedulerRepo, taskRepo, outboxRepo, logger)` (same deps as today)
- Pass it to `grpcserver.NewServer()` as a new parameter
- Remove the existing HTTP rerun wiring (lines 180–185)

**Wire-up in `state/internal/grpc/server.go`:**
- Add `rerunHandler *handlers.RerunHandler` field to `Server` struct
- Add `rerunHandler` parameter to `NewServer()`
- Add delegation method: `func (s *Server) TriggerRerun(...) { return s.rerunHandler.TriggerRerun(...) }`

**Deleted files:**
- `state/adapters/http/rerun_handler.go`
- `state/adapters/http/rerun_handler_test.go`

### 3. BFF (ui-service) changes

**`grpc-client.ts`** — add to the `GrpcClient` interface:

```typescript
triggerRerun: (request: any, callback: (err: any) => void) => void;
```

**`routes/schedulers.ts`** — add to the existing schedulers router:

```typescript
router.post('/:id/rerun', (req, res) => {
  client.triggerRerun(
    {
      schedule_id:  req.params.id,
      schema:       req.body.schema,
      table_name:   req.body.table_name,
      service_name: req.body.service_name,
    },
    (err: any) => {
      if (err) return res.status(500).json({ error: err.message });
      res.sendStatus(200);
    }
  );
});
```

`app.ts` requires no changes — the schedulers router is already mounted at `/api/schedulers`.

### 4. e2e test update (`e2e/rerun_test.go`)

`callRerunEndpoint()` currently calls `http://state:8082/schedules/{id}/rerun` directly. Update it to call the BFF instead:

```
POST http://ui:8090/api/schedulers/{id}/rerun
```

The assertion (`202 Accepted` → `200 OK`) and all downstream polling logic remain unchanged.

---

## What does not change

- The transactional outbox pattern inside the state service
- The `command.rerun:v1` Redis stream and its payload shape
- The `startup-controller` `RerunConsumer` and `RerunHandler`
- Any other caller or consumer of the state gRPC service

---

## Response semantics

`TriggerRerunResponse` is intentionally empty. The rerun has been accepted and queued (atomic write + outbox entry committed) but not yet processed — startup-controller will handle it asynchronously. There is no meaningful data to return at call time. The caller confirms success by the absence of a gRPC error, not by inspecting a response body.

---

## Callers after migration

| Caller | Before | After |
|---|---|---|
| e2e test | `POST http://state:8082/schedules/{id}/rerun` | `POST http://ui:8090/api/schedulers/{id}/rerun` |
| Future UI | not wired up | `POST /api/schedulers/{id}/rerun` via BFF |
