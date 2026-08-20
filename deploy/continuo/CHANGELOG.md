# Changelog

All notable changes to the `continuo` Helm chart (`deploy/continuo/`) are
documented here. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Chart `version` follows semver against the values contract — see the "Helm
chart versioning" section of the repository's `CLAUDE.md` for the exact rule
and the process for updating this file.

This changelog starts at the point it was introduced. `v0.1.0-rc.1` and
`v0.1.0` predate it — see `git log v0.1.0 -- deploy/continuo` for what
shipped in those.

## [Unreleased]

### Added
- `global.terminationGracePeriodSeconds` (default `30`) — sets every service
  Deployment's `terminationGracePeriodSeconds` explicitly instead of relying
  on Kubernetes' implicit 30s default. Each service bounds its own graceful
  shutdown sequence via the `SHUTDOWN_GRACE` env var (default 15s, 10s for
  `agent-runner`); the pod-level value must stay comfortably above whatever
  `SHUTDOWN_GRACE` is configured to, or Kubernetes can SIGKILL the container
  mid-teardown. The default of `30` matches today's implicit behavior, so an
  unmodified existing values file renders the same Deployment as before.
- `remediation-agent.env.RELEASE_CONTROLLER_URL` (default
  `http://release-controller:8088`), `remediation-agent.env.SHADOW_VERIFY_TIMEOUT`
  (default `"20m"`) and `remediation-agent.env.SHADOW_VERIFY_POLL_INTERVAL`
  (default `"15s"`) — release-controller's HTTP address, how long a python-node
  fix proposal waits for its shadow verification release to reach a verdict
  before the attempt is recorded as failed, and how often the remediation-agent
  reads those releases. The timeout is spent only while the shadow release is
  actually running: time it spends queued behind another release does not count
  against it. All three sit in the same free-form `env` map as the
  existing `REMEDIATION_PR_POLL_INTERVAL`/`REMEDIATION_PR_OPENING_GRACE_PERIOD`
  keys, so no schema change is required; an unmodified existing values file
  already gets these defaults via the chart's own `env` defaults.

### Changed
- `remediation-agent` now refuses to start when one of its optional duration
  settings — `LLM_CACHE_TTL`, `REMEDIATION_PR_POLL_INTERVAL`,
  `REMEDIATION_PR_OPENING_GRACE_PERIOD`, `SHADOW_VERIFY_TIMEOUT`,
  `SHADOW_VERIFY_POLL_INTERVAL` — is set to something that is not a Go duration
  (e.g. `"20 minutes"`), naming the offending key in the boot log. Previously
  such a value was silently replaced by the default, so an install looked
  configured while every process ran the built-in value. Leaving a key unset is
  unchanged and still runs its documented default, so an unmodified values file
  is unaffected.
- The `orchestrator` service now requires object storage to be reachable at
  start-up: it reads each release's code-bundle document to record node
  code-version history in the graph. Endpoint, bucket and region already reach
  every pod through the shared ConfigMap.
- S3 credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) are now injected
  by the deployment template for every storage-reading service, instead of being
  listed in each service's `secretEnv`. `services` is a list and Helm replaces
  lists wholesale, so an operator upgrading with their own older copy of
  `values.yaml` would otherwise carry an entry lacking the new refs and start
  orchestrator with no credentials. An unmodified existing values file now keeps
  working, and an entry that still lists the refs is de-duplicated rather than
  emitted twice. Installs authenticating by IAM role or workload identity leave
  the secret empty and are unaffected.
- `orchestrator` is now allowed through the bundled-MinIO NetworkPolicy. Without
  it, a default install with `networkPolicy.enabled=true` blocked every
  code-bundle read.
- `files/service_repos.yaml` header comments describe the map accurately: it
  covers every service that ships data jobs, not dbt services only, and each
  team's repository is named by the remediation trigger rather than assumed to
  be one shared checkout. Comments only — the file's keys, the ConfigMap it
  populates, and the values contract are unchanged, so an existing
  `values.yaml` or override needs no edit.
- `state`, `orchestrator`, and `agent-runner` now set `livenessPath: /livez`
  instead of falling back to the always-200 `/health`. For `state` and
  `orchestrator` this means a dead or wedged Redis stream consumer restarts
  the pod, since `/livez` reports workers and heartbeats only, deliberately
  excluding dependency probes, so a transient Redis/Postgres outage no longer
  restarts a pod whose consumers are already retrying. `agent-runner` runs no
  stream consumers, so this change gives it no new restart behavior — it now
  answers `/livez` so every Go service exposes the same liveness probe
  contract. Readiness (`readinessPath: /ready`) is unchanged for all three,
  and is not uniform across the chart: `state`, `orchestrator`,
  `executor-controller`, `k8s-controller`, and `agent-runner` use `/ready`,
  while `release-controller`, `remediation`, and `remediation-agent` use
  `/healthz`. `services` is a list and Helm replaces lists wholesale, so an
  operator who overrides it in their own values file keeps their old entries
  as-is on upgrade; they must add `livenessPath: /livez` to their own service
  entries themselves, or those pods keep probing the always-200 `/health`.
  Implies a MINOR version bump.

## [0.2.0] - 2026-08-10

### Added
- `remediation-agent.env.REMEDIATION_PR_OPENING_GRACE_PERIOD` (default `"10m"`) — how long the remediation-agent reconciler's opening sweep waits, measured against the claim's own `pr_claimed_at`, before releasing a stranded `pr_state='opening'` proposal back to `'failed'` for retry. Sits alongside the existing `REMEDIATION_PR_POLL_INTERVAL` in the same free-form `env` map, so no schema change is required; an unmodified existing values file already gets this default via the chart's own `env` defaults.
- `validation.imageTag` — pins the external `continuo-validation-<engine>` image version independently of the chart `appVersion` (default `v0.4.0`). The key is optional (not in `validation`'s `required` list), so a release that predates it — e.g. one upgraded with `helm upgrade --reuse-values` — still validates and falls back to the chart's current default in `templates/_helpers.tpl`'s `continuo.validation.image`. An explicit `imageTag: null` override collapses to that same "key absent, use the default" path (Helm drops a null-valued override key before merging). A present-but-empty string (`""`) is different — it survives the merge as a deliberate misconfiguration — and is rejected: at `helm lint`/`install` time by the schema's `minLength: 1`, and at `helm upgrade --reuse-values` time (where the schema cannot see the merged value) by a `fail` in the same helper. Accepts an optional `@sha256:<digest>` suffix (`"vX.Y.Z@sha256:<digest>"`) for an immutable pin — a plain tag is mutable, since `continuo-validation`'s publish workflow re-pushes `:vX.Y.Z` on every tag; see `values.yaml`/README.
- `helm upgrade` NOTES.txt now reminds operators that the validation image is externally released and pinned by `validation.imageTag` independently of `appVersion`, and that private-registry mirrors need it mirrored explicitly — an unmirrored bump otherwise renders healthy at install and only fails later, at the next release promotion, as a validation-pod `ErrImagePull`.

### Changed
- The validation image is now the externally released `continuo-validation-<engine>` (from github.com/carolsimone/continuo-validation), replacing the chart-appVersion-tagged `continuo-validation-runner-<engine>`; `global.imageTag` no longer applies to it.
- Bumped the default `validation.imageTag` (and its `_helpers.tpl` fallback) from `v0.2.0` to `v0.4.0`. The runner at that release adds the `build_from_columns` op and the `CANDIDATE_SPEC_URI` env contract, which python-node validation Jobs require; an install still on `v0.2.0` would dispatch a Job the runner rejects as an unknown op.

### Fixed
- Service pods now carry a `checksum/config` annotation over the shared
  ConfigMap, so `helm upgrade` restarts the pods whose configuration actually
  changed. Services read that ConfigMap through `envFrom`, and Kubernetes never
  refreshes environment variables in a running pod, so changing `REDIS_HOST`,
  `S3_BUCKET`, `LOG_LEVEL` or `validation.engine` previously rendered an
  identical Deployment and every pod kept serving the old value indefinitely.
  The engine case corrupted output rather than merely going stale:
  `executor-controller` rolls on an engine change because its validation image
  reference changes, so `manifest-controller` would have kept uploading the
  previous engine's SQL to the new engine's validator. **Upgrade note:** an
  upgrade that changes any shared ConfigMap value now performs a rolling
  restart of the affected services; an upgrade that changes none does not, as
  the digest covers only values derived deterministically from `values.yaml`
  (generated credentials live in Secrets).
- `manifest-controller` now reads and re-renders SQL in the dialect of the
  engine the install actually targets, instead of always assuming Postgres. The
  shared ConfigMap gained a `WAREHOUSE_ENGINE` key derived from the existing
  `validation.engine` value — no new values key, so unmodified overrides keep
  working. On a `validation.engine: trino` install, the candidate SQL uploaded
  for blue/green validation was previously re-rendered through sqlglot's
  `postgres` dialect, emitting constructs Trino rejects (a cast came out as
  `CAST(x AS TEXT)` rather than `CAST(x AS VARCHAR)`), so validation could fail
  on SQL the warehouse would otherwise have accepted. An engine with no dialect
  mapping now fails the service at startup rather than silently emitting another
  engine's SQL.
- Redis-backed services (`manifest-controller`, `ui-service`) no longer start
  before the bundled Redis is reachable. They previously raced the Redis
  StatefulSet, crashed on connect, and entered CrashLoopBackOff — whose
  exponential backoff (10s, 20s, 40s, 80s...) kept them down well after Redis
  became ready, so the restart loop cost more time than the wait it replaced. A
  `wait-for-redis` initContainer now gates them, mirroring the existing
  `wait-for-migrations` gate for Postgres-backed services. Bundled installs only;
  BYO installs point at Redis that is already running and render no initContainer.
- The quickstart's `/etc/hosts` step is documented with the two things operators
  actually get stuck on: it must be run from a real terminal, because sudo reads
  the password from the controlling terminal and exits with `a terminal is
  required to read the password` in an IDE pane or agent shell; and the password
  it asks for is the local account password, not the Dex demo login printed
  directly below it. The chart README now also documents a browser
  `--host-resolver-rules` flag that resolves the issuer hostname without root,
  for operators who cannot use sudo at all.

## [0.1.1] - 2026-07-29

### Fixed
- `values.schema.json` was rejecting the existing `services[].image` override
  and any resource key beyond `cpu`/`memory` (e.g. `ephemeral-storage`,
  `nvidia.com/gpu`) — both are valid, already-supported overrides that would
  have blocked upgrades for anyone using them.
- `templates/NOTES.txt` advertised an HTTPS URL regardless of
  `ingress.tls.enabled`, a fixed "password" login regardless of
  `dex.demoUser.passwordHash` overrides, and omitted the Dex port-forward/
  `/etc/hosts` steps the quickstart login actually needs when Ingress is off.
- `dbt/README.md` described the dispatched dbt Jobs as running
  `base/entrypoint.sh` against a throwaway DuckDB file; in reality
  `executor-controller` sets `Command` explicitly from
  `deploy/continuo/files/dbt-commands.yaml` (bypassing the image entrypoint)
  and every service's `profiles.yml` targets real Postgres.

### Added
- `values.schema.json` validates `values.yaml` structure and types on `helm
  lint`/`install`/`upgrade`, catching typos and wrong-shaped overrides before
  they reach template rendering or the `required` guards in `_helpers.tpl`.
- `templates/NOTES.txt` prints post-install/upgrade access instructions, the
  quickstart Dex login, and an upgrade-notes pointer to this changelog.
- This changelog, and a CI gate requiring it to be updated whenever
  `values.yaml` or `templates/` change.

## [0.1.0] - 2026-07-17

First published chart release (`oci://ghcr.io/carolsimone/charts/continuo`).
Predates this changelog — see repository git history for what shipped.
