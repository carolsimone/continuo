# Graph Service Methods Reference

This is a compact reference for the `graph` gRPC surface because several services depend on a subset of it.

| Method | Typical callers | Primary effect |
|---|---|---|
| `CreateNode` | `manifest-controller` | Upsert topology node and `DEPENDS_ON` edges |
| `GetScheduleGraph` | `ui-service` | Return topology graph for a schedule |
| `GetScheduleInitNodes` | `startup-controller` | Return all nodes, roots, and seed roots for a run |
| `UpdateNodeStatus` | `startup-controller`, `dependency-controller` | Update `EXECUTES.status` projection |
| `GetReadyDownstream` | `dependency-controller` | Find now-runnable downstream nodes |
| `CheckScheduleCompletion` | `dependency-controller` | Decide whether a run is drained |
| `SnapshotGraph` | `startup-controller` | Create `Run` node and `EXECUTES` edges |
| `FinalizeRun` | `dependency-controller` | Mark run terminal metadata |
| `ListRuns` | `ui-service` | Historical run listing |
| `GetRunGraph` | `ui-service` | Historical graph with statuses |
| `GetTransitiveDownstream` | `startup-controller` | Rerun reset scope |
