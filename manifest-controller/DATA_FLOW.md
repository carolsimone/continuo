# manifest-controller — Data Flow

## Overview

The manifest-controller is an event-driven consumer that sits between the dbt manifest files and the rest of the continuo platform. It is triggered by a Redis Stream event, performs a three-phase manifest loading pipeline, and produces an outbound event that allows the state service to reconcile its schedule catalog.

```
  ┌──────────────────────────────────────────────────────────────────────────┐
  │                       manifest-controller                                 │
  │                                                                           │
  │  update.graph:v1 ──▶ Consumer ──▶ ManifestHandler ──▶ GraphService       │
  │       (inbound)                                             (gRPC)        │
  │                                        │                                  │
  │                                        ▼                                  │
  │                               SchedulesLoadedPublisher                    │
  │                                        │                                  │
  │                                        ▼                                  │
  │                            schedules.loaded:v1 ──▶ state service          │
  │                                    (outbound)                             │
  └──────────────────────────────────────────────────────────────────────────┘
```

---

## Inbound Event: `update.graph:v1`

**Producer:** external trigger (CI pipeline, manual, dbt deploy hook)

**Payload:**
```json
{ "source": "local" | "s3" }
```

**Consumer group:** `manifest-controller`

**Startup behaviour:**
- Creates consumer group with `XGROUP CREATE … $ MKSTREAM` (idempotent; ignores `BUSYGROUP`)
- Starts from ID `0` so unacknowledged messages from a prior crash are replayed before new ones

---

## Processing Pipeline

```
┌────────────────────────────────────────────────────────────────────────────┐
│                          MANIFEST LOAD PIPELINE                             │
└────────────────────────────────────────────────────────────────────────────┘

TRIGGER: update.graph:v1 message consumed
  source = "local" | "s3"
       │
       ▼
┌──────────────────────────────────┐
│  Source resolution               │
│  "local" → filesystem adapter   │
│  "s3"    → S3 adapter           │
└───────────┬──────────────────────┘
            │
            ▼
┌──────────────────────────────────────────────────────┐
│  Phase 1 — Parse                                      │
│  Read all manifest.json files from source             │
│  Extract per-node metadata:                           │
│    node_id, service_name, schema_name, table_name,    │
│    resource_type (model/seed/snapshot),               │
│    schedule_name, owner, SQL definition               │
└───────────┬──────────────────────────────────────────┘
            │
            ▼
┌──────────────────────────────────────────────────────┐
│  Phase 2 — Registry                                   │
│  Build NodeRegistry from all parsed entries           │
│  Persist to CSV at REGISTRY_PATH (/data/registry.csv) │
└───────────┬──────────────────────────────────────────┘
            │
            ▼
┌──────────────────────────────────────────────────────┐
│  Phase 3 — Load                                       │
│  For each node:                                       │
│    resolve_upstream_deps() via sqlglot                │
│    map resource_type → NodeType:                      │
│      "model"    → "dbt-model"                        │
│      "seed"     → "dbt-seed"                         │
│      "snapshot" → "dbt-snapshot"                     │
│    GraphService.CreateNode(node_type, upstream_deps)  │
│  Returns: list of distinct schedule_names found       │
└───────────┬──────────────────────────────────────────┘
            │
            ▼
┌──────────────────────────────────────────────────────┐
│  Publish schedules.loaded:v1                          │
│  Payload:                                             │
│    {                                                  │
│      "event_id": "<new uuid>",                        │
│      "schedule_names": ["daily", "hourly", …]         │
│    }                                                  │
│  Stream: schedules.loaded:v1 (XADD)                   │
└───────────┬──────────────────────────────────────────┘
            │
            ▼
┌──────────────────────────────────────────────────────┐
│  Cleanup: source.cleanup()                            │
│  (removes temp files, local download artifacts, etc.) │
└───────────┬──────────────────────────────────────────┘
            │
            ▼
        XACK input message
```

**ACK gate:** the input message is only acknowledged after both the graph load and the `schedules.loaded:v1` publish succeed. A failure in either step leaves the message pending for retry.

---

## Outbound Event: `schedules.loaded:v1`

**Consumer:** state service (consumer group `state-schedule-catalog`)

**Payload:**
```json
{
  "event_id": "a3f1c2d4-...",
  "schedule_names": ["daily", "hourly"]
}
```

**Purpose:** lets the state service reconcile its `schedule_catalog` table — upserting new schedules and soft-deleting any that are no longer present in the manifests.

**Idempotency:** the state service deduplicates on `event_id`, so duplicate deliveries are safe.

---

## Error Handling

| Scenario | Behaviour |
|---|---|
| `UnqualifiedTableReferenceError` during dep resolution | Node is rejected; error logged; processing continues for other nodes |
| Graph gRPC call fails for a node | Failure counted; error logged; processing continues |
| `schedules.loaded:v1` publish fails | Exception propagates; input message is **not** ACKed; will be retried |
| Transient Redis read error | Sleep 3 s and retry |
| `NOGROUP` error on read | Recreate consumer group and retry |
| Any unhandled exception in handler | Input message is **not** ACKed; stays pending for reclaim on next startup |

---

## Component Interaction

```
                ┌─────────────────────────────────────┐
                │           main.py                    │
                └──────────────────┬──────────────────┘
                                   │
               ┌───────────────────┼────────────────────┐
               │                   │                    │
               ▼                   ▼                    ▼
    ┌──────────────────┐  ┌────────────────┐  ┌─────────────────────┐
    │  Consumer        │  │ ManifestHandler│  │SchedulesLoaded      │
    │  (update.graph)  │─▶│  (3-phase      │─▶│Publisher            │
    │                  │  │   pipeline)    │  │(schedules.loaded:v1)│
    └──────────────────┘  └───────┬────────┘  └─────────────────────┘
                                  │
                    ┌─────────────┼──────────────┐
                    │             │              │
                    ▼             ▼              ▼
           ┌──────────────┐ ┌──────────┐ ┌──────────────┐
           │  Source      │ │ Registry │ │ GraphClient  │
           │ (local / s3) │ │  (CSV)   │ │  (gRPC)      │
           └──────────────┘ └──────────┘ └──────────────┘
```

---

## Redis Stream Summary

| Stream | Direction | Group | Purpose |
|---|---|---|---|
| `update.graph:v1` | consumed | `manifest-controller` | Triggers manifest load |
| `schedules.loaded:v1` | produced | — | Notifies state of discovered schedules |
