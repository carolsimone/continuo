# continuo Helm chart

A single umbrella chart that installs the whole Continuo platform: every
backend service plus, optionally, quickstart datastores (PostgreSQL, Redis,
Neo4j, MinIO, Dex) so a fresh cluster needs nothing pre-created to try it.
Production installs disable those and bring their own datastores instead.

Requires Kubernetes `>=1.27.0-0` and Helm 3. Local `helm template`/`helm
install` runs against an older client-default Kubernetes capability set, so
pass `--kube-version 1.29.0` (or your real cluster's version) when rendering
outside a live cluster — otherwise Helm rejects the chart's `kubeVersion` gate
before it ever reads a template.

Released versions are published as an OCI chart, so an install needs no repo
clone at all:

```bash
helm install continuo oci://ghcr.io/carolsimone/charts/continuo \
  --version <X.Y.Z> -n continuo --create-namespace
```

No `--set global.imageTag` here: the published chart's `appVersion` is the
release tag (`v<X.Y.Z>`), which pins every continuo image to that release.
Repo-clone installs (the rest of this README) keep passing `global.imageTag`
explicitly because the in-repo `appVersion` is a dev placeholder.

## 1. Quickstart (bundled everything)

```bash
helm install continuo deploy/continuo -n continuo --create-namespace \
  --set global.imageTag=<git-sha-or-release>

kubectl -n continuo get pods -w        # wait for everything Running/Completed

kubectl -n continuo port-forward svc/ui-service 8090:8090 &
kubectl -n continuo port-forward svc/continuo-dex 5556:5556 &

# one-time: let the browser resolve the in-cluster issuer hostname
echo "127.0.0.1 continuo-dex" | sudo tee -a /etc/hosts

open http://localhost:8090   # demo login: admin@example.com / password
```

Why the `/etc/hosts` line exists: ui-service authenticates through OIDC
(OpenID Connect), and OIDC requires a *single* issuer URL that both parties
can resolve — the browser, which drives the login redirect, and ui-service,
which validates the resulting token server-side. ui-service reaches Dex fine
over in-cluster DNS at `http://continuo-dex:5556/dex`; your browser cannot
resolve that name at all. Port-forwarding `continuo-dex` to `localhost:5556`
gets the browser a route to the same pod, but only if it requests the same
hostname the issuer claims to be — hence one loopback line in `/etc/hosts`
mapping `continuo-dex` to `127.0.0.1`. That is the smallest bridge between
"browser-reachable" and "matches the issuer identity ui-service already
trusts"; anything else (a second Dex listener, a reverse proxy) is more
moving parts for the same result. `curl --resolve continuo-dex:5556:127.0.0.1
http://continuo-dex:5556/dex/.well-known/openid-configuration` verifies the
port-forward without touching `/etc/hosts` at all (bundled Dex serves plain
HTTP, not HTTPS).

The bundled datastores and the Dex demo user (`admin@example.com` /
`password`, a bcrypt hash lifted verbatim from Dex's own example config) are
for evaluation only. Passwords for bundled datastores are generated on first
install and kept stable across upgrades (see Security defaults below), but
none of this is meant to hold real data or face real users — there is no
backup story, no HA, and one static login.

**Reinstalling on top of old data.** `helm uninstall` deletes the release's
generated Secrets, but the bundled datastores' `volumeClaimTemplates` PVCs
(Persistent Volume Claims) are not owned by the release and survive it. A
plain reinstall then generates brand-new random passwords while the old PVCs'
data directories still hold the previous ones, and every bundled datastore
crashloops on auth. For a full reset, delete the release's PVCs before
reinstalling (`kubectl -n <namespace> delete pvc -l app.kubernetes.io/instance=<release>`).

To reinstall while keeping existing data, pre-create a Secret with the old
password(s) for every bundled datastore still enabled and point the chart at
it before reinstalling:

| Datastore | Values field | Secret key(s) |
|---|---|---|
| PostgreSQL | `postgresql.auth.existingSecret` (+ `existingSecretPasswordKey`) | `password` by default |
| Redis | `redis.auth.existingSecret` (+ `existingSecretPasswordKey`) | `password` by default |
| Neo4j | `neo4j.auth.existingSecret` (+ `existingSecretPasswordKey`) | `password` by default |
| MinIO | `minio.auth.existingSecret` (+ `existingSecretAccessKeyIdKey` / `existingSecretSecretKeyKey`) | `access-key-id` **and** `secret-access-key` — MinIO's Secret must carry both the root user and its password, unlike the single-key password Secrets above |

Helm silently accepts any of these fields even while the matching `*.enabled`
stays `true` — there is no validation step tying them together — so a typo'd
field name or a Secret missing a key fails at Pod start (`CreateContainer
ConfigError`), not at `helm install` time; double-check the Secret's keys
against the table above before reinstalling.

## 2. Production (bring your own datastores)

Copy [`values-byo.yaml.example`](values-byo.yaml.example), fill in your real
hosts and Secret names, and install with it:

```bash
helm install continuo deploy/continuo -n continuo --create-namespace \
  -f my-values.yaml --set global.imageTag=<pinned-release>
```

Notes that matter before you commit to this path:

- **PostgreSQL only, today.** `externalDatabase.*` is deliberately
  database-agnostic in its key names, but every SQL statement the chart and
  the services emit assumes Postgres. A different engine (MySQL is the only
  one on the roadmap) needs its own migration and query-compatibility work
  first — do not point `externalDatabase.host` at anything else yet.
- **In-cluster encryption is your responsibility.** This chart does not
  configure mTLS (mutual TLS) between pods; service-to-service traffic
  inside the cluster is plaintext unless you run a service mesh (Istio,
  Linkerd) or CNI-level encryption underneath it. External connections
  (`externalDatabase`, `externalRedis`, `externalNeo4j`, `s3.*`) go over
  whatever transport you configure at the endpoint (e.g. `sslMode: require`
  for Postgres).
- **`databaseInit` needs `CREATEDB`.** With it `enabled: true` (the
  default), an init container connects as `externalDatabase.username` and
  idempotently creates all 9 databases, including `continuo_dbt` (the dbt
  warehouse — the one database no Flyway migration directory owns). If your
  DBA provisions databases out-of-band, set `databaseInit.enabled: false` and
  drop `CREATEDB` from the connecting user's grants.
- **Upgrades keep the pre-install hook.** With `postgresql.enabled: false`,
  the migration Job stays a Helm `pre-install,pre-upgrade` hook — the same
  ordering the previous production deploy flow relied on — because your
  database already exists before the release does, so Helm can safely block
  on it before touching anything else. (The bundled-Postgres path can't use a
  hook: pre-install hooks run before *any* release resource exists, so a hook
  Job could never reach a Postgres that Helm hasn't created yet. That path
  runs the migration as a regular, revision-suffixed resource instead, with
  Postgres-backed services gating on it via an init container.) In bundled
  mode, expect new pods to briefly crashloop-converge on an upgrade: the
  `wait-for-migrations` init container only gates on `flyway_schema_history`
  existing in the target database, not on the specific migration the upgrade
  ships being applied yet, so a pod can start before that migration Job has
  finished.

## 3. Security defaults

Every container in this chart, bundled or not, gets:

- **Non-root execution.** `runAsNonRoot: true` at the pod level; Continuo's
  own images run as uid `65532` (ui-service, built on `node`, runs as uid
  `1000`). Bundled datastore images run as their own documented non-root
  uid/gid (e.g. neo4j `7474:7474`).
- **`seccompProfile: RuntimeDefault`** — the kernel syscall filter the
  container runtime ships, applied everywhere rather than left to cluster
  defaults.
- **`allowPrivilegeEscalation: false` and `capabilities: drop: ["ALL"]`** on
  every container — no container in this chart can gain more privileges than
  it starts with or hold a Linux capability it wasn't explicitly given (none
  are).
- **`readOnlyRootFilesystem: true`**, with a defensive `emptyDir` mounted at
  `/tmp` for the rare container that writes scratch files there. Two
  documented exceptions, each with an `ignore-check.kube-linter.io/
  no-read-only-root-fs` annotation on the object stating why:
  - **neo4j** — the community-edition entrypoint rewrites `neo4j.conf` at
    every container start (it has no read-only startup mode), so its root
    filesystem must stay writable.
  - **the Flyway migration Job** — Flyway writes its migration report files
    under `/flyway` on every run; there is no flag to suppress that.
- **Default-deny `NetworkPolicy`s** plus explicit allow rules derived from
  the real service call graph (who calls whom, and which datastores each
  service actually reaches). `networkPolicy.enabled` is on by default; it is
  inert (accepted but not enforced) on a CNI (Container Network Interface)
  that doesn't implement `NetworkPolicy`, so leaving it enabled costs nothing
  even on a cluster that can't act on it.
- **No committed credentials.** Every credential value in `values.yaml`
  defaults to an empty string. Bundled datastores auto-generate a password on
  first install via a Helm `lookup` against the live Secret, so upgrades
  reuse the existing value instead of rotating it out from under a running
  service (`helm template`/`--dry-run` can't perform that lookup and will
  render a fresh random value each time — harmless for a dry render, since
  only real installs and upgrades touch the actual Secret). External
  credentials are `required` and fail closed: leaving one blank fails the
  render instead of installing with an empty password.

## 4. Values reference

| Key | Purpose |
|---|---|
| `global.imageRegistry` / `global.imageRepositoryPrefix` / `global.imageTag` | Compose Continuo image refs as `<registry>/<prefix>/continuo-<service>:<tag>`. Empty `imageTag` falls back to `Chart.appVersion`. |
| `global.teamImagePrefix` | Registry/namespace prefix executor-controller uses to compose per-team dbt images for compile/seed/scheduled Jobs (unrelated to `global.imageRepositoryPrefix`, which names Continuo's own images). |
| `global.storageClass` | Default `StorageClass` for every bundled datastore PVC (Persistent Volume Claim); each datastore's own `persistence.storageClass` overrides it. |
| `postgresql.enabled` / `redis.enabled` / `neo4j.enabled` / `minio.enabled` / `dex.enabled` | Toggle the bundled quickstart instance of each datastore/identity-provider off to bring your own. |
| `postgresql.auth.existingSecret` / `redis.auth.existingSecret` / `neo4j.auth.existingSecret` / `minio.auth.existingSecret` | Pre-created Secret for the *bundled* instance's credentials (see the reinstall table above for keys), instead of letting the chart generate one. Empty (default) = generate and keep stable across upgrades. |
| `externalDatabase.*` / `externalRedis.*` / `externalNeo4j.*` / `s3.*` / `auth.*` (issuer/client fields) | Connection details used when the matching `*.enabled` above is `false`. |
| `externalDatabase.existingSecret` (+ `existingSecretPasswordKey`) | Pre-created Secret holding the Postgres password, instead of `externalDatabase.password` inline. Same pattern for `externalRedis.existingSecret`, `externalNeo4j.existingSecret` (all key `password` by default), `s3.existingSecret` (keys `access-key-id` / `secret-access-key`), and `auth.existingSecret` (key `client-secret`). |
| `databaseInit.enabled` | Idempotently creates all 9 databases (the 8 Flyway-migrated service databases plus `continuo_dbt`) before migrations run. Requires the connecting user to have `CREATEDB`; disable when a DBA pre-creates them. |
| `networkPolicy.enabled` | Default-deny ingress within the release plus allow rules derived from the service graph. |
| `ingress.enabled` / `ingress.className` / `ingress.host` / `ingress.annotations` / `ingress.tls.*` | Front door for `ui-service` only. Fully values-driven — no ingress class or cert-manager/ACME assumptions are baked in; set them yourself (see `values-byo.yaml.example`). |
| `auth.operatorEmails` / `auth.viewerEmails` / `auth.roleMapping` | Role assignment for authenticated users. With `dex.enabled: true`, `operatorEmails` defaults to the Dex demo user's email. |
| `llm.provider` / `llm.model` / `llm.apiKey` (or `llm.existingSecret`) | Optional. Empty `apiKey`: agent-runner and remediation-agent still boot and serve, but LLM (Large Language Model) calls fail until it is set. |
| `github.token` / `github.appId` / `github.installationId` / `github.appPrivateKey` (or `github.existingSecret`) | Optional. `token` is a read-only PAT (Personal Access Token) remediation-agent uses to fetch source; the `app*` fields are a GitHub App ui-service uses to open fix PRs (Pull Requests) — Create-PR returns `503` until they're set. |
| `streamReaper.enabled` / `streamReaper.schedule` / `streamReaper.retention` | CronJob that trims old Redis Stream entries. |
| `services[].resources` / `defaultResources` | Per-service CPU/memory requests and limits; any service without its own `resources` block falls back to `defaultResources`. |

## 5. Release flow and CI gates

Every PR that touches this chart (or the install-test harness under
`scripts/install-test/`) runs `install-test.yml`: `helm lint` +
`helm template` + kube-linter across four values topologies (defaults,
`values-byo.yaml.example`, BYO-inline, BYO-existingSecret), then three real
kind installs — bundled, BYO with inline credentials, and BYO with a
pre-created Secret — each verified for completed migrations, healthy pods,
and answering ui-service/Dex endpoints. Install jobs also layer on a CI-only
low-CPU-request values override so the chart fits the runner's 2 vCPUs. PR
installs use the latest published main-branch images, so they prove the
install path; the e2e suite in `ci.yml` proves the code. The kind cluster
these jobs create enforces `NetworkPolicy`, so the chart's default-deny and
allow policies are behaviorally exercised here, not just rendered — the BYO
fixture datastores need their own explicit ingress-allow policies precisely
because the chart's default-deny would otherwise block them.

Pushing a `vX.Y.Z` git tag runs `release.yml`:

1. `release.yml` first refuses any tag whose commit is not an ancestor of
   `origin/main` — a release ships exactly what main ships. Every published
   image is then retagged from the tagged commit's `:<git-sha>` to
   `:vX.Y.Z`. The tag must point at a main commit whose push ran
   `deploy.yml`'s build-publish job; otherwise the release fails closed with
   remediation instructions
   (`gh workflow run deploy.yml --ref main -f force_publish=true`, then tag
   that head).
2. The same install test runs against the `:vX.Y.Z` images and gates the
   publish.
3. The chart is packaged with `version: X.Y.Z` and `appVersion: vX.Y.Z` and
   pushed to `oci://ghcr.io/carolsimone/charts/continuo`.

ghcr packages created by CI start private, and GitHub has no API for
container-package visibility — the first publish needs a one-time manual flip
to public in the package's UI settings (Package settings → Change visibility).

ghcr OCI tags are mutable: re-pushing an existing `vX.Y.Z` git tag re-runs
this whole flow and silently overwrites both the retagged images and the
published chart version, so treat release tags as immutable by convention —
never force-push or reuse one.
