# Observability

continuo ships no monitoring stack, dashboards or alert rules. It exposes metrics, labels on its Jobs and logs, so you can use the tools you already run.

| What | Where it comes from | What you do |
|---|---|---|
| Service metrics | `/metrics` on every Go service and on topology-controller | Point your scraper at it and allow it through the NetworkPolicy |
| Failed Jobs, with their table | Labels on each Kubernetes Job | Export the labels through kube-state-metrics or your agent's equivalent |
| Alerts | Your monitoring tool | Write your own, or start from the [example rules](#example-alert-rules) |
| Logs | Pod output, and the bucket for attempt logs. The Go services and topology-controller log one JSON object per line | Collect pod output with your log agent |

continuo emits no traces.

## Service metrics

Every Go service and topology-controller serve Prometheus metrics at `/metrics` on `metrics.port` (default 9464), on a listener of its own. `ui` doesn't serve metrics.

| Value | Default | Effect |
|---|---|---|
| `metrics.port` | `9464` | The port the services serve metrics on |
| `metrics.podAnnotations` | `true` | Adds `prometheus.io/scrape`, `prometheus.io/port` and `prometheus.io/path` to those pods, for scrapers that find pods by annotation |
| `metrics.scrapeNamespaceSelector` | `{}` | The namespaces allowed to reach the port |

**With `networkPolicy.enabled`, no scraper can reach the port until you set `metrics.scrapeNamespaceSelector`:**

```yaml
metrics:
  scrapeNamespaceSelector:
    matchLabels:
      kubernetes.io/metadata.name: monitoring
```

With the Prometheus Operator, this PodMonitor scrapes the services. Replace `<release>` and `<namespace>` with your own, and make sure your Prometheus selects it:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: continuo
spec:
  selector:
    matchLabels:
      app.kubernetes.io/instance: <release>
  namespaceSelector:
    matchNames: [<namespace>]
  podMetricsEndpoints:
    - port: metrics
      path: /metrics
```

### Metrics

Every series also carries a `service` label.

| Metric | Type | Labels | Served by |
|---|---|---|---|
| `continuo_consumer_handler_duration_seconds` | histogram | `stream`, `group`, `result` | services with stream consumers |
| `continuo_consumer_dead_letters_total` | counter | `stream`, `group`, `kind` | services with stream consumers |
| `continuo_consumer_infra_pauses_total` | counter | `stream`, `group` | services with stream consumers |
| `continuo_consumer_lag` | gauge | `stream`, `group` | Go services with stream consumers |
| `continuo_consumer_pending` | gauge | `stream`, `group` | Go services with stream consumers |
| `continuo_outbox_published_total` | counter | `table` | services with an outbox |
| `continuo_outbox_publish_failures_total` | counter | `table`, `kind` | services with an outbox |
| `continuo_outbox_backlog` | gauge | `table` | services with an outbox |
| `continuo_outbox_oldest_open_age_seconds` | gauge | `table` | services with an outbox |
| `continuo_outbox_dead_letters` | gauge | `table` | services with an outbox |
| `continuo_dead_letters_total` | counter | `source`, `stream`, `group`, `kind` | `dead-letter-controller` |
| `continuo_dead_letters_redriven_total` | counter | `source`, `stream` | `dead-letter-controller` |
| `continuo_dead_letters_expired_total` | counter | `source` | `dead-letter-controller` |
| `continuo_dead_letter_backlog` | gauge | `source`, `stream`, `kind` | `dead-letter-controller` |
| `continuo_dead_letter_oldest_open_age_seconds` | gauge | `source` | `dead-letter-controller` |
| `continuo_stream_trimmed_entries_total` | counter | `stream` | `dead-letter-controller` |
| `continuo_stream_quarantined_total` | counter | `stream`, `group` | `dead-letter-controller` |
| `continuo_stream_length` | gauge | `stream` | `dead-letter-controller` |
| `continuo_redis_used_memory_bytes` | gauge | none beyond `service` | `dead-letter-controller` |
| `continuo_redis_maxmemory_bytes` | gauge | none beyond `service` | `dead-letter-controller` |
| `continuo_stream_trim_last_success_timestamp_seconds` | gauge | none beyond `service` | `dead-letter-controller` |
| `continuo_egress_requests_total` | counter | `decision` (`allowed`, `denied`), `reason` | `egress-proxy` |
| `continuo_egress_bytes_total` | counter | `direction` (`up` to the destination, `down` to the agent) | `egress-proxy` |
| `continuo_egress_active_tunnels` | gauge | none beyond `service` | `egress-proxy` |
| `continuo_attempts_awaiting_pod_stop` | gauge | none beyond `service` | `execution-controller` |
| `continuo_attempts_stopped_total` | counter | none beyond `service` | `execution-controller` |
| `continuo_fenced_jobs_deleted_total` | counter | `reason` (`fenced_run`, `ended_undeployed`) | `execution-controller` |
| `continuo_runs_stalled_total` | counter | none beyond `service` | `run-controller` |
| `continuo_runs_cancelling` | gauge | none beyond `service` | `run-controller` |
| `continuo_runs_cancelling_overdue` | gauge | none beyond `service` | `run-controller` |
| `continuo_run_cancel_seconds` | histogram | none beyond `service` | `run-controller` |
| `continuo_node_lease_waiters` | gauge | none beyond `service` | `run-controller` |
| `continuo_node_lease_wait_seconds` | histogram | none beyond `service` | `run-controller` |
| `continuo_node_leases_repaired_total` | counter | none beyond `service` | `run-controller` |
| `continuo_command_requests_total` | counter | `command`, `outcome` | `run-controller` |
| `continuo_cron_fires_total` | counter | `outcome` | `run-controller` |
| `continuo_schedules` | gauge | `state` | `run-controller` |
| `continuo_seed_reloads_total` | counter | `action` (`load`, `full_refresh`), `outcome` (`loaded`, `stopped`, `refused`, `failed`) | `run-controller` |
| `continuo_seed_reload_stops_total` | counter | `reason` | `run-controller` |
| `continuo_views_missing` | gauge | `state` (`missing`, `stale`) | `run-controller` |
| `continuo_view_outage_seconds` | histogram | none beyond `service` | `run-controller` |
| `continuo_catalog_exports_total` | counter | `outcome` (`success`, `failure`, `skipped`) | `catalog`, while `CATALOG_BACKUP_ENABLED` is true |
| `continuo_catalog_export_duration_seconds` | histogram | none beyond `service` | `catalog`, while `CATALOG_BACKUP_ENABLED` is true |
| `continuo_catalog_export_bytes` | gauge | none beyond `service` | `catalog`, while `CATALOG_BACKUP_ENABLED` is true |
| `continuo_catalog_export_records` | gauge | `kind` (`node`, `relationship`) | `catalog`, while `CATALOG_BACKUP_ENABLED` is true |
| `continuo_catalog_export_last_success_timestamp_seconds` | gauge | none beyond `service` | `catalog`, while `CATALOG_BACKUP_ENABLED` is true |
| `continuo_topology_stage_duration_seconds` | histogram | `stage` | `topology-controller` |
| `continuo_topology_releases_total` | counter | `outcome`, `failure_kind` | `topology-controller` |
| `continuo_topology_release_nodes` | gauge | `family` | `topology-controller` |
| `continuo_topology_release_columns` | gauge | `family` | `topology-controller` |
| `continuo_topology_release_skipped_nodes` | gauge | `reason` | `topology-controller` |
| `continuo_topology_release_peak_rss_bytes` | gauge | none beyond `service` | `topology-controller` |
| `continuo_releases_total` | counter | `outcome` (`promoted`, `rejected`), `reject_reason` (`none` or a reject reason) | `release-controller` |
| `continuo_release_stage_duration_seconds` | histogram | `stage` (`compiling`, `parsing`, `seed_building`, `validating`), `outcome` (`ok`, `failed`) | `release-controller` |
| `go_sql_*` (open, in-use, idle connections, wait count and duration, closed-by-limit counters) | gauge / counter | `db_name` | every service that keeps a Postgres database (not `catalog` or `egress-proxy`) |
| `go_*`, `process_*` | Go runtime and process collectors | none beyond `service` | every Go service |

`continuo_seed_reloads_total` counts the seed loads that drop the seed's table, when they end, and the seed loads the planner refused before they started. A seed load that drops nothing is not counted.

## Job labels

continuo runs each task and each release check as a Kubernetes Job, and these labels say what the Job is for:

| Label | Task | Validation | Seed-build | Compile |
|---|---|---|---|---|
| `app` (always `dbt-job`) | yes | yes | yes | yes |
| `service_name` | yes | yes | yes | yes |
| `schema_name` | yes | yes | yes | |
| `table_name` | yes | yes | yes | |
| `run-id` | yes | | | |
| `task-id` | yes | | | |
| `attempt` | yes | | | |
| `schedule` | yes | | | |
| `runtime` (Python Jobs only) | yes | | | |
| `mode` | | yes | yes | yes |
| `release-id` | | yes | yes | yes |
| `node-id` | | yes | yes | yes |

Kubernetes limits a label value to 63 characters, so continuo shortens longer `table_name`, `release-id` and `node-id` values. Validation Jobs for dbt tests have a `schema_name` such as `analytics_dbt_test__audit`, so `node-id` identifies them better.

### Export the labels to Prometheus

A Job's pods carry the same labels. kube-state-metrics skips these labels unless you list them, and without a list it emits no `kube_job_labels` or `kube_pod_labels` series at all. Start it with:

```text
--metric-labels-allowlist=jobs=[app,service_name,schema_name,table_name,run-id,task-id,attempt,schedule,runtime,mode,release-id,node-id],pods=[app,service_name,schema_name,table_name,run-id,task-id,attempt,schedule,runtime,mode,release-id,node-id]
```

With the `kube-prometheus-stack` chart, set the same lists in its values:

```yaml
kube-state-metrics:
  metricLabelsAllowlist:
    - jobs=[app,service_name,schema_name,table_name,run-id,task-id,attempt,schedule,runtime,mode,release-id,node-id]
    - pods=[app,service_name,schema_name,table_name,run-id,task-id,attempt,schedule,runtime,mode,release-id,node-id]
```

Dashes become underscores in Prometheus, so `run-id` becomes `label_run_id`.

### Failed Jobs that are expected

Validation, seed-build and compile Jobs are all named `validate-*`. When one fails, it usually means the release was rejected, which continuo handles itself, so leave these Jobs out of your alerts.

A failed Job stays on the cluster for `execution.jobTTLMinutes` (default 30). An alert on it can only fire during that time, so its wait must be shorter than this value.

### CPU and memory per table

The kubelet reports each container's CPU and memory by pod name. Join that with `kube_pod_labels` to see the table. This assumes you scrape the kubelet's cAdvisor endpoint, as `kube-prometheus-stack` does, and these queries are for Prometheus.

Memory of each running Job, right now:

```promql
container_memory_working_set_bytes{container="dbt-job"}
  * on (namespace, pod) group_left (label_schema_name, label_table_name)
  kube_pod_labels{label_table_name!=""}
```

CPU cores of each running Job, right now:

```promql
rate(container_cpu_usage_seconds_total{container="dbt-job"}[1m])
  * on (namespace, pod) group_left (label_schema_name, label_table_name)
  kube_pod_labels{label_table_name!=""}
```

Highest memory seen per table over the last day. The range on `kube_pod_labels` is needed, because that series ends once the Job is deleted:

```promql
max by (label_schema_name, label_table_name) (
  max by (namespace, pod) (max_over_time(container_memory_working_set_bytes{container="dbt-job"}[1d]))
    * on (namespace, pod) group_left (label_schema_name, label_table_name)
    max by (namespace, pod, label_schema_name, label_table_name) (max_over_time(kube_pod_labels{label_table_name!=""}[1d]))
)
```

For the CPU used by a finished Job, replace the memory metric in that query with `container_cpu_usage_seconds_total`. The result is CPU-seconds.

These numbers are samples, not a record of each Job:

- **Short Jobs can be missed:** a Job that ends between two scrapes has no samples at all. On a kubelet scraped every 10 seconds, Jobs of a few seconds often have none or one.
- **Peaks can be missed:** the real peak can fall between samples, so don't size memory limits from a short Job's numbers.
- **`rate` reads low on short Jobs:** a Job shorter than the `[1m]` range shows fewer cores than it used. Use CPU-seconds for those.

## Example alert rules

These are Prometheus rules. Adjust the thresholds to your install, and the `job="kube-state-metrics"` selector if your scrape job has another name.

```yaml
groups:
  - name: continuo
    rules:
      - alert: ContinuoJobFailed
        expr: |
          (kube_job_failed{job="kube-state-metrics", job_name!~"validate-.*"} > 0)
            * on (namespace, job_name) group_left (label_service_name, label_schema_name, label_table_name, label_schedule, label_run_id, label_attempt)
            kube_job_labels
          or on (namespace, job_name)
          (kube_job_failed{job="kube-state-metrics", job_name!~"validate-.*"} > 0)
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "{{ if $labels.label_table_name }}{{ $labels.label_schema_name }}.{{ $labels.label_table_name }} failed (service {{ $labels.label_service_name }}{{ if $labels.label_schedule }}, schedule {{ $labels.label_schedule }}{{ end }}{{ if $labels.label_run_id }}, run {{ $labels.label_run_id }}, attempt {{ $labels.label_attempt }}{{ end }}){{ else }}Job {{ $labels.namespace }}/{{ $labels.job_name }} failed{{ end }}"
      - alert: OutboxStuck
        expr: continuo_outbox_oldest_open_age_seconds > 300
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "The oldest open row of outbox {{ $labels.table }} is {{ $value | humanizeDuration }} old"
      - alert: NewDeadLetters
        expr: increase(continuo_dead_letters_total[15m]) > 0
        labels:
          severity: warning
        annotations:
          summary: "New dead letters from {{ $labels.source }} on {{ $labels.stream }} ({{ $labels.kind }})"
      - alert: RunCancellingOverdue
        expr: continuo_runs_cancelling_overdue > 0
        labels:
          severity: warning
        annotations:
          summary: "{{ $value }} cancelling runs are overdue and still wait for attempts to stop"
      - alert: CatalogExportStale
        expr: time() - continuo_catalog_export_last_success_timestamp_seconds > 26 * 3600
        labels:
          severity: warning
        annotations:
          summary: "The last stored catalog export is older than 26 hours"
      - alert: TopologyStageNearLivenessBudget
        expr: |
          sum by (stage) (increase(continuo_topology_stage_duration_seconds_bucket{le="+Inf", stage!~"resolve|rewrite|candidate_upload"}[30m]))
            - sum by (stage) (increase(continuo_topology_stage_duration_seconds_bucket{le="30.0", stage!~"resolve|rewrite|candidate_upload"}[30m]))
            > 0
        labels:
          severity: warning
        annotations:
          summary: "topology-controller's {{ $labels.stage }} stage took over 30 s; liveness restarts the pod about 80 s after its last heartbeat"
```

The `or` in `ContinuoJobFailed` keeps the alert firing with the Job's name if the labels aren't exported. `CatalogExportStale` applies only with `catalog.backup.enabled`. `TopologyStageNearLivenessBudget` leaves out `resolve`, `rewrite` and `candidate_upload`: the per-node stages refresh the heartbeat after every node, so their summed time says nothing about it. `dbt_parse` and `python_parse` refresh the heartbeat once per manifest, so the rule is a coarse near-budget heuristic over a stage's total time in one release. Python bucket labels are written `30.0`, not `30`.

## Datadog

**We haven't tested these settings against continuo.** They come from Datadog's documentation.

| Data | How Datadog gets it |
|---|---|
| `continuo_*` metrics | The Agent reads the `prometheus.io/*` pod annotations once `datadog.prometheusScrape.enabled` is true |
| Failed Jobs | Its Kubernetes State Core integration reports `kubernetes_state.job.completion.failed`, with no Prometheus involved |
| Job labels | Map them to tags under `datadog.kubeStateMetricsCore.labelsAsTags` |

```yaml
datadog:
  prometheusScrape:
    enabled: true
  kubeStateMetricsCore:
    labelsAsTags:
      job:
        schema_name: schema_name
        table_name: table_name
```

- **Network access:** add the Agent's namespace to `metrics.scrapeNamespaceSelector`. If the Agent uses host networking, a namespace selector may not admit it, so check that scrapes succeed.
- **Cost:** Datadog bills scraped metrics as custom metrics. Limit the check to the metrics you need.
- **Tags:** the key format of `labelsAsTags` depends on the chart version, so check that the tags appear on the metric.
- **Alerts:** Datadog doesn't read the rules above, so rebuild them as monitors.

## Logs

- **Service logs:** services write to their pod's output. `global.logLevel` sets the level for the Go services and `topology-controller`.
- **Attempt logs:** `execution-controller` uploads each attempt's log to the bucket when the attempt ends. You can read them in the UI.
