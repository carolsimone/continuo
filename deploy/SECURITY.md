# Security posture

What the `deploy/continuo` chart guarantees, and what remains the
operator's responsibility. The mechanics behind each guarantee are described
in the chart README's [Security defaults](continuo/README.md#3-security-defaults).

## What the chart guarantees

- **No default credentials.** No secret in this chart has a committed
  default. Bundled quickstart datastores generate strong random passwords at
  install (stable across upgrades); external credentials are fail-closed —
  Helm refuses to render if they are missing. Every credential group also
  accepts `existingSecret`, so secret-manager users never put a password in
  a values file.
- **Non-root, hardened containers.** Every chart-rendered workload runs
  `runAsNonRoot` with `seccompProfile: RuntimeDefault`,
  `allowPrivilegeEscalation: false`, `capabilities: drop: ["ALL"]`, and
  `readOnlyRootFilesystem` wherever the workload tolerates it. The runtime
  Jobs that execution-controller creates (dbt, validation, seed-build)
  carry the same securityContext.
- **Default-deny NetworkPolicies** (`networkPolicy.enabled: true` by
  default) with allow rules derived from the real service graph. On
  policy-enforcing CNIs these are load-bearing — the CI install test
  exercises them behaviorally on kind. Caveat: when your datastores are
  pre-existing pods in the same namespace, the default-deny selects them
  too; either run datastores in another namespace, add your own allow
  policies for them, or disable the policies.
- **Pinned images.** Published charts pin every continuo image to the
  release tag via `appVersion`; nothing references `:latest`. Release tags
  must point at commits reachable from `main`, images are retagged (not
  rebuilt) from the exact commit's digests, and chart publish is gated on a
  passing kind install test in bundled and BYO modes.
- **Least-privilege RBAC.** Every `services` entry runs under its own ServiceAccount.
  Only execution-controller's holds (namespace-scoped, minimal) Kubernetes
  API access; every other one is bound to nothing. egress-proxy, the bundled
  datastores, Dex and the chart's Jobs run as the namespace's `default`
  ServiceAccount.
- **Credentials as files.** With `credentials.delivery: file` the chart
  mounts each credential read-only under `/var/run/secrets/continuo` and sets
  `<VAR>_FILE`, so no credential is in a pod's environment;
  `credentials.files` points a service at files a secret manager writes
  (the Vault Agent injector, a Secrets Store CSI volume). The Postgres and
  Redis passwords are read again for every new connection.
- **One login per service.** Each service signs in to Postgres and Redis as
  its own login, so a leaked credential reaches one service's database and
  one service's streams and keys. The migrate Job revokes `CONNECT` from
  `PUBLIC` on every continuo database, grants each login only its own, and
  fails the deploy when a login holds more (superuser, `CREATEDB`,
  `CREATEROLE`, `REPLICATION`, `BYPASSRLS`, another database). Each Redis
  login's rule comes from the stream contract, and a service refuses to start
  when its login reaches keys outside its own. `credentials.redis.mode:
  shared`, for a Redis with no ACL users, gives this up for Redis.

## What the operator owns

- **Ingress and TLS termination** — `ingress.*` is fully values-driven; TLS
  certificates, cert-manager/ACME, and the ingress controller are yours.
- **Encryption in transit inside the cluster** — run a service mesh or an
  encrypting CNI; the chart is mesh-compatible and configures no app-level
  mTLS.
- **Logins and secret storage** — with your own Postgres and Redis you create
  each service's login, its password and the Secret or file holding it
  ([CREDENTIALS.md](CREDENTIALS.md)); the chart consumes Secrets or files a
  secret manager writes (via `existingSecret` or `credentials.files`), and
  rotating them is operator process. continuo grants each Postgres login its
  rights and checks them; it never creates a login on your datastores, and
  never runs `ACL SETUSER` on your Redis.
- **The warehouse login** — dbt and validation Job pods sign in to your
  warehouse with the Secret you name (`validation.warehouseSecret`); continuo
  builds that Secret only for the bundled Postgres.
- **Datastore hardening and backups** — bundled datastores are
  quickstart-grade (single instance, no backups); production datastores,
  their TLS, HA, and backup story are yours.
- **OIDC provider** — ui is fail-closed on auth (it crashloops
  rather than serving an open UI); you supply a real IdP per
  [AUTH.md](AUTH.md), or accept the quickstart-only bundled Dex demo login.
