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
- `validation.imageTag` — pins the external `continuo-validation-<engine>` image version independently of the chart `appVersion` (default `v0.2.0`).

### Changed
- The validation image is now the externally released `continuo-validation-<engine>` (from github.com/carolsimone/continuo-validation), replacing the chart-appVersion-tagged `continuo-validation-runner-<engine>`; `global.imageTag` no longer applies to it.

### Fixed
- Redis-backed services (`manifest-controller`, `ui-service`) no longer start
  before the bundled Redis is reachable. They previously raced the Redis
  StatefulSet, crashed on connect, and entered CrashLoopBackOff — whose
  exponential backoff (10s, 20s, 40s, 80s...) kept them down well after Redis
  became ready, so the restart loop cost more time than the wait it replaced. A
  `wait-for-redis` initContainer now gates them, mirroring the existing
  `wait-for-migrations` gate for Postgres-backed services. Bundled installs only;
  BYO installs point at Redis that is already running and render no initContainer.

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
