# Streaming Platform Services

Two services extend the orchestration platform to cover real-time Flink pipelines alongside batch jobs.

---

## streaming_monitor

### Why

Flink jobs run continuously — they never "finish" like batch tasks do. The orchestration platform needs visibility into their health and data freshness without owning their lifecycle. `streaming_monitor` is the dedicated observer: it provides the platform with a continuous, up-to-date picture of every streaming pipeline.

### What it does

- Polls Flink REST API for job status (`RUNNING`, `FAILED`, `RESTARTING`)
- Polls Kafka consumer group lag per registered streaming job
- Tracks watermark progression to detect stalls
- Writes all observations to `state_service`

`streaming_monitor` does **nothing else**. It does not restart jobs, trigger graph nodes, or make decisions. It detects and reports.

### System interactions

```
streaming_monitor
  ─── reads ──→  Flink REST API       (job status, watermark metrics)
  ─── reads ──→  Kafka API            (consumer group lag)
  ─── writes ──→ state_service        (job health, lag, watermark snapshots)
```

`state_service` is the only output. All downstream reactions (remediation, graph unblocking) are driven by other services reading that state.

---

## flink_manager

### Why

When a Flink job needs to be deployed, upgraded, or recovered, something has to execute that operation reliably and report the outcome back to the platform. `flink_manager` is the single actor authorised to mutate the state of Flink deployments. This keeps all Helm and Flink REST write operations in one place, with a clean interface for the rest of the platform.

### What it does

- Deploys and upgrades Flink jobs via Helm (`helm upgrade --install`)
- Polls Flink REST until the job reaches `RUNNING` state
- Executes recovery on instruction from `graph_service` (restart, restart from savepoint)
- Writes deployment outcomes and job state transitions back to `state_service`

`flink_manager` acts only when commanded. It does not monitor continuously and does not decide when to act.

### System interactions

```
graph_service
  ─── commands ──→ flink_manager

flink_manager
  ─── reads/writes ──→ Flink REST API   (job status, savepoint triggers)
  ─── executes ──→     Helm             (deploy, upgrade)
  ─── writes ──→       state_service    (deployment outcome, new job state)
```

---

## How they fit together

```
streaming_monitor  ──→  state_service  ←──  flink_manager
                              │
                        graph_service
                              │
                     (unblock downstream
                      batch nodes or
                      dispatch recovery
                      to flink_manager)
```

`streaming_monitor` feeds state in continuously. `graph_service` reads that state to make decisions — unblocking downstream batch nodes when lag is within threshold, or dispatching a recovery command to `flink_manager` when a job has failed. `flink_manager` executes and reports back.

Neither `streaming_monitor` nor `flink_manager` talk to each other directly.

---

## Job registry

Both services operate on a registered set of streaming jobs. Each entry defines what to monitor and how to handle it:

```json
{
  "job_name": "clickstream-enrichment",
  "kafka_consumer_group": "flink-clickstream-cg",
  "flink_rest_endpoint": "http://clickstream-enrichment-rest:8081",
  "helm_chart": "charts/flink-job",
  "max_lag": 1000,
  "on_failure": "restart_from_savepoint",
  "max_restart_attempts": 3
}
```

`flink_manager` reads this on deploy/recover. `streaming_monitor` reads this to know what to poll and against which thresholds to evaluate.
