# Release notes

What shipped in each Continuo release, newest first. A release is one published
Helm chart (`oci://ghcr.io/carolsimone/charts/continuo`) whose `version` and
`appVersion` are stamped by the same `vX.Y.Z` tag, so every service image is
pinned to that tag — a version is the whole platform at that point.

The highlights below are the user-visible changes, drawn from the pull requests
merged in each version and the chart changelog. Two companion sources carry the
rest:

- **[Chart changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md)** — the values-contract detail: schema changes, upgrade steps, and exactly what a breaking change means for an existing `values.yaml`.
- **[GitHub releases](https://github.com/carolsimone/continuo/releases)** — the complete commit and pull-request list for each version, including dependency and CI changes omitted here.

Impact tags (`MAJOR` / `MINOR` / `PATCH`) are the **chart's** semver against the values contract, not a measure of how much shipped: `MAJOR` can break an unmodified existing `values.yaml` on upgrade — read the chart changelog before taking one. A release can carry large app changes under a `PATCH` chart tag.

---

## 0.7.0 — 2026-09-24 · `MINOR`

- **Point agentic remediation at your own repos.** The new `serviceRepos` chart value maps each service to its project root; when set it replaces the shipped demo map, now labelled example-only.
- **`agent-chat`** caches the prompt prefix and no longer drops tool calls when the turn budget is tight.
- **CLI:** `node list`, and an optional source run id on node `trigger` / `test` / `build`.

[Chart changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#070---2026-09-24) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.7.0)

## 0.6.2 — 2026-09-17 · `MINOR`

- **Run tab reads status at a glance.** Fast tasks are now observed as `running` instead of skipping `pending → succeeded`, nodes are coloured by status with a legend, and dependency graphs are drawn in execution order (seeds on the left, arrows from a table to the tables that read it).

[Chart changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#062---2026-09-17) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.6.2)

## 0.6.1 — 2026-09-16 · `PATCH`

- **Operator-dashboard polish** before go-live: dropped the spurious "topology version unknown" strip and the redundant Flaky node column, cleaned up the Runs/Nodes tabs and the schedule detail page.

[Chart changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#061---2026-09-16) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.6.1)

## 0.6.0 — 2026-09-15 · `MAJOR`

- **One execution service.** `executor-controller` and `k8s-controller` are merged into a single `execution-controller` that settles outcomes and retries in-process (one service, database, and outbox). Nothing in flight is carried over — **upgrade only when nothing is running.**
- **Schedule detail redesigned:** a status-first Run tab and a Topology tab, plus a grouped old-snapshot picker.
- **Remediation reaches parse-stage failures** — it can now repair a python-model node rejected at the parse leg, not only at validation.

[Chart changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#060---2026-09-15) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.6.0)

## 0.5.0 — 2026-09-09 · `MAJOR`

- **Remediation, end to end.** Retry rounds ("try again" on a rejected release), one batched pull request per rejected release, and a redesigned Remediation tab — proposals grouped by release and round, nested attempts, status pills, a per-service filter, and a diff-first rationale composed from facts. Proposals without a repository fail closed.
- **Validation bind-checks dbt tests.** A test whose SQL names a column or relation the changed model removed now rejects the release.
- **Verification runs are first-class.** A fix's verification run leaves the Releases tab for its own `/verifications/:id` page and ends in `passed`/`failed`, separate from releases. (Two `agentRemediation` env keys renamed to `VERIFICATION_TIMEOUT` / `VERIFICATION_POLL_INTERVAL`.)
- **UI:** branding across page headers, release-page service tiles and a pipeline timeline, an Author column on Releases, Active/Inactive schedule badges with cron + timezone, and a liveness badge.

[Chart changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#050---2026-09-09) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.5.0)

## 0.4.1 — 2026-08-25 · `PATCH`

- **Python runtime from PyPI.** The `continuo-python-runtime-<engine>` image installs its runtime and engine adapters from PyPI instead of building from source — a drop-in change.
- **Local dev on MinIO.** The dev stack replaces LocalStack with MinIO for local S3, and a new walkthrough puts real dbt projects on Continuo locally. Demo repo renamed to `continuo-demo`.

[Chart changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#041---2026-08-25) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.4.1)

## 0.4.0 — 2026-08-23 · `MAJOR`

- **Platform-wide service renames:** `ui-service` → `ui`, `agent-runner` → `agent-chat`, `remediation-agent` → `agent-remediation`, `manifest-controller` → `topology-controller` (databases and images renamed to match, migrated in place — see the chart NOTES).
- **New python-csv node kind** with control-plane support across validation, the remediation lane, the executor, and the chart; the UI shows node types (family icons, a detail-header chip, the csv source URI).
- **Ingress:** network policy admits cert-manager's ACME HTTP-01 solver when ingress and TLS are on.

[Chart changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#040---2026-08-23) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.4.0)

## 0.3.0 — 2026-08-21 · `MAJOR`

- **Python-node remediation** with shadow-release-verified contract fixes: proposals carry file edits, the fix prompt reads the dependency graph and the release code bundle, and a failure-precedent case base informs the fix.
- **Code-version history in the graph** — each release's node code versions are recorded and queryable.
- **Duplicate-table rejection** — a release is rejected when two models claim the same warehouse table.
- **Runtime consolidation:** the validation runner ships from the merged `continuo-python-runtime-<engine>` image. `/livez` liveness restarts a pod whose Redis-stream consumer has wedged, and shutdown is bounded by a configurable grace period.

[Chart changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#030---2026-08-21) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.3.0)

## 0.2.0 — 2026-08-10 · `MINOR`

- **Engine-aware validation.** Candidate SQL is re-rendered in the target warehouse's own dialect (Trino no longer receives Postgres SQL), and `helm upgrade` rolls only the pods whose configuration actually changed.
- **Security hardening:** containers run non-root with tightened pod security contexts, expensive routes are rate-limited with redacted startup logging, and GitHub App private-key handling is hardened.
- **Validation stack extracted** to the standalone `continuo-validation` repository, pinned via the new `validation.imageTag`; early python-node contract work lands (kind dispatch, contract schema v1, the code-bundle contract).

[Chart changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#020---2026-08-10) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.2.0)

## 0.1.1 — 2026-07-29 · `PATCH`

- **Multi-arch images** (linux/arm64 alongside amd64).
- **Trino validation engine** and an engine-agnostic dbt connection (pods read the warehouse Secret rather than assuming Postgres).
- **Parse-free dbt Jobs** — per-node runs reuse a release-proven partial-parse cache.
- **Chart hardening:** typed `values.schema.json` validated on `helm lint`/`install`/`upgrade`, post-install `NOTES.txt`, the chart changelog with its CI gate, and a security/secret-scanning baseline.

[Chart changelog](https://github.com/carolsimone/continuo/blob/main/deploy/continuo/CHANGELOG.md#011---2026-07-29) · [Release](https://github.com/carolsimone/continuo/releases/tag/v0.1.1)

## 0.1.0 — 2026-07-17

First published chart release (`oci://ghcr.io/carolsimone/charts/continuo`). It
predates the chart changelog and the per-release notes above — see
`git log v0.1.0 -- .` for the full initial contents.

[Release](https://github.com/carolsimone/continuo/releases/tag/v0.1.0)
