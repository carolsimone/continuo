# Release notes

Every continuo release is one published Helm chart
(`oci://ghcr.io/carolsimone/charts/continuo`). A single `vX.Y.Z` tag stamps the
chart `version` and its `appVersion`, so a release pins every service image to
the same tag. A version is the whole platform at that point.

Highlights are below, newest first. For the rest:

- [Chart changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md): values-contract detail such as schema changes, upgrade steps, and what a breaking change does to an existing `values.yaml`.
- [GitHub releases](https://github.com/carolsimone/continuo/releases): the full commit and PR list per version, including the dependency and CI churn left out here.

The `MAJOR` / `MINOR` / `PATCH` tag is the chart's semver against the values
contract, not the size of the release. `MAJOR` means an unmodified `values.yaml`
may not survive the upgrade, so read the changelog first. A release with large
app changes can still be a `PATCH`.

---

## 0.15.0 · 2026-10-26 · `MINOR`

- **The two LLM agents reach the outside only through an egress proxy.** agent-chat and agent-remediation send their outbound traffic through `egress-proxy`, which admits a connection only to a host and port on its allowlist. The chart builds the allowlist from your settings (`llm.provider` and `llm.baseUrl`, agent-remediation's `GITHUB_BASE_URL`, `s3.endpointUrl` with an external bucket, `egress.allowedDomains`, `egress.privateDomains`), and with `networkPolicy.enabled` a NetworkPolicy drops every other outbound connection from the agents. It is on by default.
- **Addresses the agents reach directly.** An IP address the agents are given (a URL host, a gRPC address, a Postgres or Redis on a public address) is a direct destination, and `egress.directDestinations` lists more.
- **Breaking, read the changelog first.** An install whose agents reach a host that is not on the allowlist, or a Postgres or Redis on a public address, needs `egress.allowedDomains`, `egress.privateDomains` or `egress.directDestinations` before it upgrades, or `egress.enabled: false` to keep open egress. The render fails, naming the key, when an agent's `services` entry sets a proxy variable, `LLM_BASE_URL`, `S3_ENDPOINT_URL` or a literal `LLM_PROVIDER`, or when a URL host is no valid host name.
- The proxy's refusals are logged: `kubectl -n <ns> logs -l app.kubernetes.io/name=egress-proxy --tail=-1 | grep "egress denied"`. See [Outbound traffic](https://github.com/carolsimone/continuo/blob/main/deploy/README.md#outbound-traffic).

[Changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#0150---2026-10-26) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.15.0)

## 0.14.0 · 2026-10-24 · `MINOR`

- **A cancel is asynchronous.** A cancelled run goes to `cancelling` and its tasks in flight to `stopping`; execution-controller deletes each attempt's Job and reports it `stopped`, and the run then ends `cancelled`. While a run is `cancelling` it holds its schedule, so no cron fire or manual trigger creates a second run. The run page and `continuo schedule status` show the new states.
- The `schedule.cancelled:v1` stream is replaced by `run.cancelled:v1`, which names the attempts state still waits on and is sent again for them.
- Upgrade with no run active: runs in flight when this release deploys are not carried over. Cancel every pending or running run once it has deployed.
- The chart also deploys `egress-proxy`, a forward proxy with an allowlist; the agents start using it in 0.15.0.

[Changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#0140---2026-10-24) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.14.0)

## 0.13.0 · 2026-10-23 · `MINOR`

- **Retries are decided by failure class.** The state service decides every attempt of every task and retries a failed one at once while the task has retries left (`runs.taskMaxRetries`, default 2); a failure by timeout, or a dispatch that cannot run, is never retried. A task shows `dispatched` and `queued` before it runs, and a run that fails without a failed task says why, for example `stalled` when nothing was in flight for 5 minutes.
- **Every task has a timeout.** Domain teams declare a node's timeout (`meta.timeout_minutes` on a dbt node, `timeout_minutes` on a Python contract entry, 1 to 10,080 minutes); a node that declares none gets `runs.taskTimeoutHours` (default 24). Kubernetes stops the Job at its timeout. A Python domain repository declares it only once both its CLI and its image base are continuo-python-runtime 0.10.0 or later, and the default `validation.imageTag` moves to `v0.10.0`. `execution.startTimeoutMinutes` (default 15) bounds how long a pod may take to start a container.
- **Attempts are visible.** The run page shows every attempt of every task, `continuo node history` carries each attempt's failure class and reason, and the node page and `continuo node list` show a node's task timeout and whether the node declares it.
- A pod that cannot start (an image it cannot pull, a missing Secret or ConfigMap) has its Job deleted and its attempt fails as a start failure, so a retry never runs beside it.
- The orchestrator's dispatch watchdog is gone: the state service fails a stalled run itself, and a healthy run waiting for a slot is no longer cancelled.
- Upgrade with no run active: runs in flight when this release deploys are not carried over, because their tasks were dispatched on a stream nothing reads any more. Turn maintenance mode on and wait for running work to finish first.

[Changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#0130---2026-10-23) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.13.0)

## 0.12.0 · 2026-10-16 · `MINOR`

- **Rerun failed nodes → Latest snapshot** (a rebase) restarts a failed run from its failure on the live release. It re-runs what did not succeed, the nodes the release added and everything below them and, by default, the changed nodes upstream of what it re-runs, so a fix to the node that caused the failure is picked up. A change to a node's image alone does not count. The dialog previews the selection before you confirm: how many nodes run and why, and each changed upstream node with its last run time.
- The state service plans every run itself and starts its ready tasks in the same transaction. A trigger that cannot make a valid run is refused at once and creates no run.
- `llm.baseUrl` points the agents at an LLM server for every provider. A values file that sets `LLM_BASE_URL` through `services[].env` or `secretEnv` now fails the render: move the value to `llm.baseUrl`.
- Upgrade with no run active: on its first start, the state service cancels every pending or running run it did not plan. Turn maintenance mode on and wait for running work to finish first; `helm upgrade` prints the same note.

[Changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#0120---2026-10-16) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.12.0)

## 0.11.0 · 2026-10-13 · `MINOR`

- release-controller's database drops the columns that held topologies inline. Upgrade to 0.10.0 first and let release-controller start once, then upgrade to 0.11.0 with no release in flight; the migration refuses to run otherwise and changes nothing. No values change.

[Changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#0110---2026-10-13) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.11.0)

## 0.10.0 · 2026-10-13 · `MINOR`

- Messages are no longer dropped: a new service, dead-letter-controller, stores every message a consumer or an outbox gives up on, and `continuo dlq list`, `show` and `redrive` read and replay them. It also trims the Redis streams, so the `stream-reaper` CronJob is gone.
- `maintenance.enabled` puts continuo in maintenance mode: no new work starts and work already running finishes.
- Every Go service serves Prometheus metrics on `metrics.port` (default 9464). `execution.maxConcurrentJobs` caps the Jobs that run at once, and `database.pool.*` bounds each service's Postgres pool.
- Promotions travel by reference to a topology object in the bucket. With an external bucket, orchestrator and state need read access to the `tenants/` prefix.

[Changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#0100---2026-10-13) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.10.0)

## 0.9.1 · 2026-10-02 · `PATCH`

- A fresh install comes up in dependency order with no restarts: every service that uses Redis waits for it, in an init container with the bundled Redis and in code everywhere else (BYO, docker-compose). Before, the Go services waited only for their database migrations and could crash-loop if Redis started later.
- Each Go service answers its liveness probe while it waits for Redis and stays unready until startup completes.
- The datastore gates no longer fill the Postgres and MinIO bucket-init logs with errors while they wait.

[Changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#091---2026-10-02) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.9.1)

## 0.9.0 · 2026-10-02 · `MINOR`

- CD pipelines release through continuo's public URL: `POST /api/v1/releases` on the ui, authenticated with the workflow's own GitHub Actions OIDC token. Bind each service to its repository in `ciAuth.bindings`. A pipeline needs no kubeconfig, SSH key or stored secret, and polls `GET /api/v1/releases/{id}` until the release is final. See [Releasing from CI](../deploy/README.md#releasing-from-ci-github-actions).
- An operator can call the same API with a 1-hour token from the bundled Dex; the tutorial no longer port-forwards to release-controller.
- Re-submitting a release id with different content is refused with `409` instead of being silently ignored.
- release-controller and remediation receive their HTTP port under the variable they read.

[Changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#090---2026-10-02) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.9.0)

## 0.8.1 · 2026-10-01 · `PATCH`

- A fresh install no longer restarts `orchestrator` while Neo4j is still starting: it waits for Neo4j instead of exiting and relying on a pod restart.
- The state database drops the unused `task_tracker.manifest_version` column. Upgrade through 0.8.0 first; an install already on 0.8.0 is unaffected.
- Node services are listed in a stable order regardless of the database's locale.

[Changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#081---2026-10-01) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.8.1)

## 0.8.0 · 2026-10-01 · `MAJOR`

- Full refresh of a single dbt node from the node page: its table is dropped and rebuilt from scratch. Seeds always full-refresh. `dbt-commands.yaml` gains two optional keys, `full_refresh` and `seed_full_refresh`.
- New `python-api` node kind: a python script node that can read an operator-created Secret named in its contract (`secret_ref`). The chart now reserves the `continuo-api-` prefix, so a release name, `fullnameOverride` or Secret name inside it fails `helm upgrade`; read the changelog before upgrading.
- The python script node kind is `python-node`; contracts written for `python-model` keep working.
- The node run history no longer shows a Manifest column, and `continuo node history` no longer returns `manifest_version`; it was always empty. Use `image_tag`.

[Changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#080---2026-10-01) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.8.0)

## 0.7.0 · 2026-09-24 · `MINOR`

- Remediation can target your own repositories. The new `serviceRepos` value maps each service to its project root; set it and it replaces the shipped demo map, now marked example-only.
- `agent-chat` caches its prompt prefix and stops dropping tool calls when the turn budget runs low.
- CLI: `node list`, plus an optional source run id on node `trigger`, `test`, and `build`.

[Changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#070---2026-09-24) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.7.0)

## 0.6.2 · 2026-09-17 · `MINOR`

- The Run tab shows status directly. Fast tasks now register as `running` instead of jumping straight from `pending` to `succeeded`, nodes are coloured by status with a legend, and graphs draw in execution order (seeds on the left, arrows from a table to its readers).

[Changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#062---2026-09-17) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.6.2)

## 0.6.1 · 2026-09-16 · `PATCH`

- Dashboard cleanup before go-live: dropped the misleading "topology version unknown" strip and the Flaky node column, and tidied the Runs and Nodes tabs and the schedule page.

[Changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#061---2026-09-16) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.6.1)

## 0.6.0 · 2026-09-15 · `MAJOR`

- `executor-controller` and `k8s-controller` become one `execution-controller` that settles retries in-process. Nothing in flight carries over, so upgrade only when nothing is running.
- Schedule detail redesigned: a status-first Run tab and a Topology tab, plus a grouped snapshot picker.
- Remediation now handles parse-stage failures, not just validation ones.

[Changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#060---2026-09-15) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.6.0)

## 0.5.0 · 2026-09-09 · `MAJOR`

- Remediation got a big pass. It retries a rejected release in rounds, opens one batched PR per release, and has a rebuilt tab: proposals grouped by release and round, nested attempts, status pills, a service filter, and the diff shown first. A proposal with no repository fails closed.
- Validation bind-checks dbt tests. A test that reads a column the change removed rejects the release.
- Verification runs got their own page (`/verifications/:id`) and `passed`/`failed` states, split out from releases. (Two `agentRemediation` env keys renamed to `VERIFICATION_TIMEOUT` and `VERIFICATION_POLL_INTERVAL`.)
- UI: header branding, service tiles and a pipeline timeline on release pages, an Author column on Releases, schedule Active/Inactive badges with cron and timezone, and a liveness badge.

[Changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#050---2026-09-09) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.5.0)

## 0.4.1 · 2026-08-25 · `PATCH`

- The `continuo-python-runtime-<engine>` image installs its runtime and adapters from PyPI instead of building from source. It is a drop-in change.
- The local stack moves from LocalStack to MinIO for S3, and a new walkthrough runs real dbt projects on Continuo locally. Demo repo renamed to `continuo-demo`.

[Changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#041---2026-08-25) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.4.1)

## 0.4.0 · 2026-08-23 · `MAJOR`

- Service renames: `ui-service` to `ui`, `agent-runner` to `agent-chat`, `remediation-agent` to `agent-remediation`, `manifest-controller` to `topology-controller`. Databases and images renamed to match, migrated in place (see the chart NOTES).
- New python-csv node kind across validation, remediation, the executor, and the chart. The UI shows node types with icons, a header chip, and the csv source URI.
- Network policy lets cert-manager's ACME HTTP-01 solver through when ingress and TLS are on.

[Changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#040---2026-08-23) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.4.0)

## 0.3.0 · 2026-08-21 · `MAJOR`

- Python-node remediation with shadow-release verification: proposals carry file edits, the fix prompt reads the graph and the release code bundle, and past fixes inform new ones.
- Each release's node code versions are recorded in the graph and queryable.
- A release is rejected when two models write the same warehouse table.
- The validation runner ships from the merged `continuo-python-runtime-<engine>` image. `/livez` restarts a pod whose Redis consumer has wedged, and a configurable grace period bounds shutdown.

[Changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#030---2026-08-21) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.3.0)

## 0.2.0 · 2026-08-10 · `MINOR`

- Validation renders candidate SQL in the target warehouse's dialect, so Trino stops getting Postgres SQL. `helm upgrade` restarts only the pods whose config changed.
- Security: non-root containers with tighter pod security contexts, rate-limited expensive routes, redacted startup logs, and hardened GitHub App key handling.
- The validation stack moved to the `continuo-validation` repo, pinned by the new `validation.imageTag`. Early python-node contract work landed.

[Changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#020---2026-08-10) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.2.0)

## 0.1.1 · 2026-07-29 · `PATCH`

- Multi-arch images (linux/arm64 as well as amd64).
- Trino validation engine, and an engine-agnostic dbt connection: pods read the warehouse Secret instead of assuming Postgres.
- Parse-free dbt Jobs reuse a release-proven partial-parse cache.
- Chart hardening: typed `values.schema.json` checked on lint, install, and upgrade; post-install `NOTES.txt`; this changelog and its CI gate; and a security-scanning baseline.

[Changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#011---2026-07-29) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.1.1)

## 0.1.0 · 2026-07-17

- First published chart release. It predates these notes; see `git log v0.1.0 -- .` for what it contained.

[Release](https://github.com/carolsimone/continuo/releases/tag/v0.1.0)
