# Release notes

What shipped in each Continuo release, newest first. A release is one published
Helm chart (`oci://ghcr.io/carolsimone/charts/continuo`) whose `version` and
`appVersion` are stamped by the same `vX.Y.Z` tag, so every service image is
pinned to that tag — a version is the whole platform at that point.

The highlights below are the user-visible changes. Two companion sources carry
the rest:

- **[Chart changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md)** — the values-contract detail: schema changes, upgrade steps, and exactly what a breaking change means for an existing `values.yaml`.
- **[GitHub releases](https://github.com/carolsimone/continuo/releases)** — the full commit and pull-request list for each version.

Impact tags below (`MAJOR` / `MINOR` / `PATCH`) are the chart's semver against the values contract: `MAJOR` can break an unmodified existing `values.yaml` on upgrade — read the chart changelog before taking one.

---

## 0.7.0 — 2026-09-24 · `MINOR`

Agentic remediation can be pointed at your own repositories. The new
`serviceRepos` chart value maps each service to its project root within its repo;
when set it **replaces** the shipped demo map, which is now labelled
example-only. The default map also gained the `continuo-core-finance-demo`
services so that walkthrough works out of the box.

[Chart changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#070---2026-09-24) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.7.0)

## 0.6.2 — 2026-09-17 · `MINOR`

Fast tasks are now observed as `running` instead of skipping straight from
`pending` to `succeeded`, and the Run tab graph reads status at a glance: nodes
coloured by status with a legend, dependencies drawn in execution order (seeds
on the left, arrows from a table to the tables that read it).

[Chart changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#062---2026-09-17) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.6.2)

## 0.6.1 — 2026-09-16 · `PATCH`

Operator-dashboard polish before go-live: cleaner Runs and Nodes tabs and
schedule detail, a width-capped schedule page, and no more spurious "topology
version unknown" strip on runs that predate topology tracking.

[Chart changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#061---2026-09-16) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.6.1)

## 0.6.0 — 2026-09-15 · `MAJOR`

`executor-controller` and `k8s-controller` are merged into a single
`execution-controller` (one service on port 8084, one database, one outbox).
Nothing in flight is carried over — **upgrade only when nothing is running**:
stop the old services and let every run, release, and queued retry finish first.

[Chart changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#060---2026-09-15) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.6.0)

## 0.5.0 — 2026-09-09 · `MAJOR`

Validation now bind-checks every dbt test of a changed model: a test whose SQL
names a column or relation the release removed rejects the release. Fix-
verification runs became first-class — they leave the Releases tab for their own
`/verifications/:id` page and end in `passed`/`failed`. (Two `agentRemediation`
env keys were renamed to `VERIFICATION_TIMEOUT` / `VERIFICATION_POLL_INTERVAL`.)

[Chart changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#050---2026-09-09) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.5.0)

## 0.4.1 — 2026-08-25 · `PATCH`

The `continuo-python-runtime-<engine>` image now installs its runtime and engine
adapters from PyPI instead of building from source — a drop-in change. The demo
repository was renamed to `continuo-demo`.

[Chart changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#041---2026-08-25) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.4.1)

## 0.4.0 — 2026-08-23 · `MAJOR`

Platform-wide service renames: `ui-service` → `ui`, `agent-runner` →
`agent-chat`, `remediation-agent` → `agent-remediation`, `manifest-controller` →
`topology-controller` (databases and images renamed to match, migrated in place
— see the chart NOTES). Network policy now lets cert-manager's ACME HTTP-01
solver through when ingress and TLS are on.

[Chart changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#040---2026-08-23) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.4.0)

## 0.3.0 — 2026-08-21 · `MAJOR`

The validation image is consolidated into `continuo-python-runtime-<engine>`
(runtime and validation runner from one repository). Graceful shutdown is now
bounded by a configurable `global.terminationGracePeriodSeconds`, and the Go
services move to a `/livez` liveness probe so a wedged Redis-stream consumer
restarts its own pod instead of serving stale work.

[Chart changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#030---2026-08-21) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.3.0)

## 0.2.0 — 2026-08-10 · `MINOR`

`helm upgrade` now rolls only the pods whose configuration actually changed (a
`checksum/config` annotation over the shared ConfigMap), so a changed
`REDIS_HOST` or `LOG_LEVEL` takes effect instead of silently persisting.
Candidate SQL is re-rendered in the target warehouse's own dialect (Trino no
longer receives Postgres SQL), and `validation.imageTag` lets you pin the
external validation image independently of `appVersion`.

[Chart changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#020---2026-08-10) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.2.0)

## 0.1.1 — 2026-07-29 · `PATCH`

Multi-arch images. Introduced `values.schema.json` (typed validation of
`values.yaml` on `helm lint`/`install`/`upgrade`), post-install `NOTES.txt`
access instructions, and the chart changelog itself with a CI gate that requires
it to be updated whenever the values or templates change.

[Chart changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#011---2026-07-29) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.1.1)

## 0.1.0 — 2026-07-17

First published chart release (`oci://ghcr.io/carolsimone/charts/continuo`). It
predates the chart changelog — see `git log v0.1.0 -- deploy/continuo` for what
shipped.

[Release](https://github.com/carolsimone/continuo/releases/tag/v0.1.0)
