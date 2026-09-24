# Release notes

Every Continuo release is one published Helm chart
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
