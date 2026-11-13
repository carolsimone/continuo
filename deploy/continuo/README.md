# continuo Helm chart

One umbrella chart that installs the whole continuo platform: every backend
service, plus optional quickstart datastores (PostgreSQL, Redis, Neo4j, MinIO,
Dex) so a fresh cluster needs nothing pre-created to try it. Production installs
turn those off and bring their own.

Requires Kubernetes `>=1.27.0-0` and Helm 3. When you render outside a live
cluster, `helm template`/`helm install` assumes an older Kubernetes version, so
pass `--kube-version 1.29.0` (or your cluster's version). Without it, Helm
rejects the chart's `kubeVersion` gate before it reads a template.

Released versions ship as an OCI chart, so an install needs no repo clone:

```bash
helm install continuo oci://ghcr.io/carolsimone/charts/continuo \
  --version <X.Y.Z> -n continuo --create-namespace
```

No `--set global.imageTag` here: the published chart's `appVersion` is the
release tag (`v<X.Y.Z>`), which pins every continuo image to that release.
Repo-clone installs (the rest of this README) still pass `global.imageTag`
explicitly, because the in-repo `appVersion` is a dev placeholder.

## 1. Quickstart (bundled everything)

```bash
helm install continuo deploy/continuo -n continuo --create-namespace \
  --set global.imageTag=<git-sha-or-release>

kubectl -n continuo get pods -w        # wait for everything Running/Completed

kubectl -n continuo port-forward svc/ui 8090:8090 &
kubectl -n continuo port-forward svc/continuo-dex 5556:5556 &

# one-time: let the browser resolve the in-cluster issuer hostname
grep -q "continuo-dex" /etc/hosts || echo "127.0.0.1 continuo-dex" | sudo tee -a /etc/hosts

open http://localhost:8090   # demo login: admin@example.com / password
```

The bundled datastores and the Dex demo user (`admin@example.com` /
`password`) are for evaluation only: passwords are generated on first install
and stay stable across upgrades (see Security defaults below), but there are no
backups, no HA, and one static login. None of this is meant to hold real data
or face real users.

The bundled MinIO server and `mc` client are served from the chart's own
`ghcr.io/carolsimone/continuo-minio` and `continuo-mc` repositories, so the
quickstart pulls its object store anonymously like every other image and needs
no MinIO or quay.io account.

<details>
<summary>Why the <code>/etc/hosts</code> line — and how to skip it</summary>

ui authenticates through OIDC (OpenID Connect), which needs one issuer URL that
both the browser (it drives the login redirect) and ui (it validates the token
server-side) can resolve. ui reaches Dex over in-cluster DNS at
`http://continuo-dex:5556/dex`, but your browser cannot resolve that name.
Port-forwarding `continuo-dex` to `localhost:5556` routes the browser to the
same pod — but only if it asks for the hostname the issuer claims. The one
`/etc/hosts` line maps `continuo-dex` to `127.0.0.1` to make that match. To
verify the port-forward without touching `/etc/hosts`: `curl --resolve
continuo-dex:5556:127.0.0.1
http://continuo-dex:5556/dex/.well-known/openid-configuration` (bundled Dex
serves plain HTTP, not HTTPS).

Run that `sudo` line in a real terminal. sudo reads the password from the
controlling terminal (`/dev/tty`), not from standard input unless you pass
`-S`. In a context with no terminal — an IDE terminal pane, an agent shell, a
non-login shell — it does not prompt at all and exits with `sudo: a terminal is
required to read the password`. The pipe into `tee` does not change this: sudo
ignores standard input either way. The password it wants is your local account
password, not the Dex demo login.

To avoid `/etc/hosts` entirely, point the browser's own resolver at the
loopback port-forward instead of the system hosts file. This needs no root —
use it when sudo is unavailable. Chrome needs the separate `--user-data-dir`,
because an already-running Chrome ignores flags passed to a second launch.

```bash
open -na "Google Chrome" --args \
  --host-resolver-rules="MAP continuo-dex 127.0.0.1" \
  --user-data-dir="$HOME/.continuo-chrome" \
  http://localhost:8090
```

</details>

<details>
<summary>Reinstalling on top of existing data</summary>

`helm uninstall` deletes the release's generated Secrets, but the bundled
datastores' `volumeClaimTemplates` PVCs (Persistent Volume Claims) survive it —
the release does not own them. A plain reinstall then generates new random
passwords while the old PVCs' data directories still hold the previous ones, so
every bundled datastore crashloops on auth. For a full reset, delete the
release's PVCs before reinstalling (`kubectl -n <namespace> delete pvc -l
app.kubernetes.io/instance=<release>`).

To reinstall while keeping existing data, pre-create a Secret with the old
password(s) for every bundled datastore still enabled and point the chart at it
before reinstalling:

| Datastore | Values field | Secret key(s) |
|---|---|---|
| PostgreSQL | `postgresql.auth.existingSecret` (+ `existingSecretPasswordKey`) | `password` by default |
| Redis | `redis.auth.existingSecret` (+ `existingSecretPasswordKey`) | `password` by default |
| Neo4j | `neo4j.auth.existingSecret` (+ `existingSecretPasswordKey`) | `password` by default |
| MinIO | `minio.auth.existingSecret` (+ `existingSecretAccessKeyIdKey` / `existingSecretSecretKeyKey`) | `access-key-id` **and** `secret-access-key` — MinIO's Secret must carry both the root user and its password, unlike the single-key password Secrets above |

Helm accepts any of these fields even while the matching `*.enabled` stays
`true` — nothing validates them together — so a mistyped field name or a Secret
missing a key fails at Pod start (`CreateContainer ConfigError`), not at `helm
install` time. Double-check the Secret's keys against the table above before
reinstalling.

</details>

## 2. Production (bring your own datastores)

Copy [`values-byo.yaml.example`](values-byo.yaml.example), fill in your real
hosts and Secret names, and install with it:

```bash
helm install continuo deploy/continuo -n continuo --create-namespace \
  -f my-values.yaml --set global.imageTag=<pinned-release>
```

Notes that matter before you commit to this path:

- **Every service signs in as its own Postgres and Redis login.** Before the
  install, your platform team creates each login and a Secret holding its
  password, and the values name the Secrets (`credentials.postgres`,
  `credentials.redis`). [CREDENTIALS.md](../CREDENTIALS.md) lists every login,
  its rights and the statements and rules that create it. The migration login
  (`externalDatabase.username`) owns every continuo database, needs no
  superuser, and grants each runtime login its rights on every deploy. The
  warehouse dbt and the validation runner sign in to is yours: set
  `validation.createWarehouseSecret: false` and `validation.warehouseSecret`.
- **PostgreSQL only, today.** `externalDatabase.*` uses database-agnostic key
  names on purpose, but every SQL statement the chart and the services emit
  assumes Postgres. Another engine (MySQL is the only one planned) needs its
  own migration and query-compatibility work first — do not point
  `externalDatabase.host` at anything else yet.
- **The Postgres connection must be session-mode.** Each outbox relay (run-controller,
  execution-controller, release-controller, remediation, agent-remediation,
  dead-letter-controller) wakes on a Postgres `LISTEN`/`NOTIFY` notification
  sent when a row is committed, over one dedicated connection per pod. Two services
  hold more such connections per pod: execution-controller one more, for its
  dispatcher, which wakes when a deployment is accepted or a slot is released,
  and run-controller two more, for its lease waker, which wakes when a task releases
  the write lease on its node, and for its cron loop, which wakes when a
  change to a schedule's cron commits. `LISTEN` needs a session-mode
  connection: behind a transaction-mode pooler (for example PgBouncer in
  transaction mode) it does not work. Delivery then continues on each relay's
  5 s poll instead of immediately, the dispatcher starts queued work on its
  5 s poll, run-controller grants a freed node to the task waiting for it on the lease
  waker's 60 s pass or the attempt reconcile's pass, which grants freed nodes
  before it looks for stalled runs, and a cron edit takes effect on the cron
  loop's next pass, at most 60 s later. Point `externalDatabase.host` at
  Postgres directly or at a session-mode pooler.
- **In-cluster encryption is your responsibility.** The chart does not
  configure mTLS (mutual TLS) between pods; service-to-service traffic inside
  the cluster is plaintext unless you run a service mesh (Istio, Linkerd) or
  CNI-level encryption underneath it. External connections
  (`externalDatabase`, `externalRedis`, `externalNeo4j`, `s3.*`) use whatever
  transport you configure at the endpoint (e.g. `sslMode: require` for
  Postgres).
- **`databaseInit` needs `CREATEDB`.** With `enabled: true` (the default), an
  init container connects as `externalDatabase.username` and idempotently
  creates the 7 Flyway-migrated service databases (including
  `continuo_dead_letter`, which dead-letter-controller owns); the migration
  login owns what it creates. catalog keeps its graph in Neo4j and has no
  database here, and the warehouse is yours. If your DBA provisions databases
  out-of-band, set `databaseInit.enabled: false`, make the migration login
  their owner, and drop `CREATEDB` from its grants.
- **How migrations run depends on the Postgres mode.** With
  `postgresql.enabled: false`, the migration Job is a Helm
  `pre-install,pre-upgrade` hook: your database already exists before the
  release, so Helm blocks on the migration before touching anything else. The
  bundled-Postgres path can't use a hook — pre-install hooks run before *any*
  release resource exists, so a hook Job could never reach a Postgres that Helm
  hasn't created yet. That path runs the migration as a regular,
  revision-suffixed resource, with Postgres-backed services gating on it via an
  init container. In bundled mode, expect new pods to briefly crashloop on an
  upgrade: the `wait-for-migrations` init container, signed in as the
  service's own login, checks that the login may read the target database's
  non-empty `flyway_schema_history` (the migrate Job grants it after Flyway),
  not that the upgrade's specific migration has been applied, so a pod can
  start before the migration Job finishes.

## 3. Security defaults

Every container in this chart, bundled or not, gets:

- **Non-root execution.** `runAsNonRoot: true` at the pod level. continuo's own
  images run as uid `65532` (ui, built on `node`, runs as uid `1000`). Bundled
  datastore images run as their own documented non-root uid/gid (e.g. neo4j
  `7474:7474`).
- **`seccompProfile: RuntimeDefault`** — the kernel syscall filter the
  container runtime ships, applied everywhere rather than left to cluster
  defaults.
- **`allowPrivilegeEscalation: false` and `capabilities: drop: ["ALL"]`** on
  every container — no container in this chart can gain more privileges than it
  starts with, or hold a Linux capability it wasn't given (none are).
- **`readOnlyRootFilesystem: true`**, with a defensive `emptyDir` mounted at
  `/tmp` for the rare container that writes scratch files there. Two
  documented exceptions, each with an `ignore-check.kube-linter.io/
  no-read-only-root-fs` annotation on the object stating why:
  - **neo4j** — the community-edition entrypoint rewrites `neo4j.conf` at
    every container start (it has no read-only startup mode), so its root
    filesystem must stay writable.
  - **the Flyway migration Job** — Flyway writes its migration report files
    under `/flyway` on every run; there is no flag to suppress that.
- **Default-deny `NetworkPolicy`s** plus explicit allow rules derived from the
  real service call graph (who calls whom, and which datastores each service
  actually reaches). `networkPolicy.enabled` is on by default; it is inert
  (accepted but not enforced) on a CNI (Container Network Interface) that
  doesn't implement `NetworkPolicy`, so leaving it enabled costs nothing even
  on a cluster that can't act on it. Scrapers are admitted only through
  `metrics.scrapeNamespaceSelector` (empty by default, so none), and only to the
  metrics port.
  agent-chat and agent-remediation, the two services where a language model
  decides what happens next, also reach the outside world only through
  egress-proxy, which admits the hosts on its allowlist
  ([Outbound traffic](https://github.com/carolsimone/continuo/blob/main/deploy/README.md#outbound-traffic)).
- **One login per service.** Each service signs in to Postgres and to Redis
  as its own login. A Postgres login can connect only to its service's
  database and read and write only its tables; a Redis login reaches only its
  service's streams and keys, under rules generated from the stream contract
  (`deploy/redis-acl-rules.acl`). The migrate Job checks every Postgres
  login's rights on every deploy, `streams-init` signs in as every Redis login
  before the services roll, and each service refuses to start when its Redis
  login reaches keys outside its own. See [CREDENTIALS.md](../CREDENTIALS.md).
- **No committed credentials.** Every credential value in `values.yaml`
  defaults to an empty string. Bundled datastores auto-generate a password on
  first install via a Helm `lookup` against the live Secret, so upgrades reuse
  the existing value instead of rotating it out from under a running service.
  (`helm template`/`--dry-run` can't perform that lookup and renders a fresh
  random value each time — harmless for a dry render, since only real installs
  and upgrades touch the actual Secret.) External credentials are `required`
  and fail closed: leaving one blank fails the render instead of installing
  with an empty password.

## 4. Values reference

| Key | Purpose |
|---|---|
| `global.imageRegistry` / `global.imageRepositoryPrefix` / `global.imageTag` | Compose continuo-owned service image refs as `<registry>/<prefix>/continuo-<service>:<tag>`. Empty `imageTag` falls back to `Chart.appVersion`. Does not apply to the validation image — see `validation.engine` / `validation.imageTag` below. |
| `global.deadLetterControllerGrpcAddr` | In-cluster gRPC address of dead-letter-controller's `DeadLetterService` (default `dead-letter-controller:50055`), set on agent-chat as `CONTINUO_DEAD_LETTER_ADDR` for the `continuo dlq` commands it runs. Set it, together with the service's `grpcPort`, only if you move the service. |
| `global.releaseControllerGrpcAddr` | In-cluster gRPC address of release-controller's `ReleaseQuery` (default `release-controller:50056`), set on agent-chat as `CONTINUO_RELEASE_CONTROLLER_ADDR` for the `continuo release notices` command it runs. Set it, together with the service's `grpcPort`, only if you move the service. |
| `global.env` | Names the install's environment (`ENV` on the shared ConfigMap, default `production`). No service reads it yet. |
| `global.logLevel` | Level every Go service and topology-controller log at (`LOG_LEVEL` on the shared ConfigMap, default `INFO`): `DEBUG`, `INFO`, `WARN`, `WARNING` or `ERROR`, in any case. Rendering fails on any other value. ui's server log has no level. |
| `global.teamImagePrefix` | Registry/namespace prefix execution-controller uses to compose per-team dbt images for compile/seed/scheduled Jobs (unrelated to `global.imageRepositoryPrefix`, which names continuo's own images). |
| `validation.engine` / `validation.imageTag` | Select and pin the external `continuo-python-runtime-<engine>` image, composed as `<registry>/<prefix>/continuo-python-runtime-<engine>:<imageTag>` (one image ships both the python-node runtime and the validation runner). It ships from a separate repository (`github.com/carolsimone/continuo-python-runtime`) on its own release train, so `imageTag` (default `v0.12.0`) is versioned independently of `global.imageTag`/`appVersion` and must be non-empty. The engine is part of the image name, not the tag, leaving the tag position free for a digest. A plain tag is mutable — the publish workflow re-pushes `:vX.Y.Z` (and `:latest`) on every tag, so re-running a tag moves it under existing installs. For an immutable pin, set `imageTag` to `"vX.Y.Z@sha256:<digest>"`. If you mirror images into a private registry, mirror this ref explicitly — `global.imageTag`'s same-appVersion mirroring does not cover it. A tag below `v0.12.0` fails the render: the seed preflight runs on it. The validation warehouse Secret's role must be able to list the objects that depend on a seed's table (the Postgres catalog) and, on Trino, read the views outside continuo that the probe checks; without it the preflight reports the warehouse unreachable, or each probe as failed. |
| `global.storageClass` | Default `StorageClass` for every bundled datastore PVC (Persistent Volume Claim); each datastore's own `persistence.storageClass` overrides it. |
| `postgresql.enabled` / `redis.enabled` / `neo4j.enabled` / `minio.enabled` / `dex.enabled` | Toggle the bundled quickstart instance of each datastore/identity-provider off to bring your own. |
| `postgresql.auth.existingSecret` / `redis.auth.existingSecret` / `neo4j.auth.existingSecret` / `minio.auth.existingSecret` | Pre-created Secret for the *bundled* instance's credentials (see the reinstall table above for keys), instead of letting the chart generate one. Empty (default) = generate and keep stable across upgrades. |
| `externalDatabase.*` / `externalRedis.*` / `externalNeo4j.*` / `s3.*` / `auth.*` (issuer/client fields) | Connection details used when the matching `*.enabled` above is `false`. |
| `externalDatabase.existingSecret` (+ `existingSecretPasswordKey`) | Pre-created Secret holding the migration login's Postgres password, instead of `externalDatabase.password` inline (each service's own login is `credentials.postgres`; the Redis password here is the one of `credentials.redis.mode=shared`). Same pattern for `externalRedis.existingSecret`, `externalNeo4j.existingSecret` (all key `password` by default), `s3.existingSecret` (keys `access-key-id` / `secret-access-key`), and `auth.existingSecret` (key `client-secret`). |
| `databaseInit.enabled` | Idempotently creates the 7 Flyway-migrated service databases, and with the bundled Postgres the warehouse (`validation.warehouseDatabase`), before migrations run. Requires the migration login to have `CREATEDB`; disable when a DBA pre-creates them and makes the migration login their owner. |
| `credentials.postgres.<service>` (`username`, `existingSecret`, `key`) | The Postgres login each of run-controller, execution-controller, release-controller, remediation, agent-remediation, agent-chat and dead-letter-controller signs in as (`POSTGRES_USER`), and the Secret and key holding its password (default `continuo_<service>`, key `password`). The migrate Job grants each login its rights on every deploy and fails the deploy, naming the login, when one is missing or has more. With an external Postgres an enabled service's login needs `existingSecret` or a `POSTGRES_PASSWORD` entry in `credentials.files`, or the render fails; with the bundled one the chart generates the passwords into `<release>-logins` and the migrate Job creates the logins. `credentials.postgres.warehouse.username` (default `continuo_warehouse`) is the bundled quickstart's warehouse login. See [CREDENTIALS.md](../CREDENTIALS.md). |
| `credentials.redis.mode` / `credentials.redis.logins.<name>` | `perService` (default): each Redis service signs in as its own login (`REDIS_USERNAME`), `streams-init` as `continuo_redis_migrate`, each with the password in its login's `existingSecret` (key `key`, default `password`), and the rule published in `deploy/redis-acl-rules.acl` and `files/redis-acl-rules.json`; with the bundled Redis the chart generates every password and starts Redis with the rules, its `default` user off. `shared`: every service signs in with the one Redis password, for a Redis with no ACL users. `REDIS_ACL_MODE` on the shared ConfigMap. With an external Redis in `perService` an enabled service's login, and `streams-init`'s, needs `existingSecret` (or a `REDIS_PASSWORD` entry in `credentials.files`), or the render fails. |
| `redisInit.createStreams` | `true` (default) runs `streams-init`, which creates every Redis stream and consumer group under the `continuo:` prefix (a `pre-install,pre-upgrade` hook Job with `redis.enabled=false`, a Job of the release with the bundled Redis); no service creates one. Set it to `false` when the Redis login the chart uses may not create streams (`XGROUP CREATE` with `MKSTREAM`): the Job then creates nothing, and the Redis owner runs `/streams-init` from the dead-letter-controller image with a login that may. After Redis loses its data (a failover without persistence, a flush, a restore), run `streams-init` again with `helm upgrade` or by running the Job; consumers stop with "consumer group ... does not exist" until it has run. |
| `database.pool.maxOpenConns`, `database.pool.maxIdleConns` | Postgres pool limits for every Go service that keeps a Postgres database (`DB_MAX_OPEN_CONNS`/`DB_MAX_IDLE_CONNS`; catalog and egress-proxy keep none). 0 keeps each service's own default. Set the two together; `maxIdleConns` may not exceed `maxOpenConns`. Each limit is a cap per pod, and the sum of the caps is the worst case: with the defaults and one replica of each service, 117 pooled connections plus nine `LISTEN` connections outside the pools (one per pod of the six services that run an outbox relay: run-controller, execution-controller, release-controller, remediation, agent-remediation and dead-letter-controller; one more in execution-controller and two more in run-controller), 126 in all; consumers handle one message at a time, so steady-state use is far lower. `maxOpenConns` replaces the default of every one of those services at once, the largest of which is execution-controller's 30: a lower value can leave a service's handlers waiting for a connection. To fit a small Postgres, lower the low-concurrency services one at a time through their `services[].env` (`DB_MAX_OPEN_CONNS`/`DB_MAX_IDLE_CONNS`), or raise Postgres `max_connections`. |
| `maintenance.enabled` | Refuse new work while letting running work finish. When enabled (default false), ui, the public release API and the CLI refuse new runs, triggers, releases, remediation retries, schedule edits (a cron saved, paused, resumed or removed) and dead-letter redrives with HTTP 503 / gRPC `UNAVAILABLE` and the message "continuo is in maintenance mode"; run-controller consumes each cron slot that comes due with no run (it is not caught up afterwards); release-controller activates no queued release; agent-remediation records each fix attempt that comes due as skipped, and **Retry remediation** in the UI starts it once maintenance is off. Runs, releases and fix verifications already in progress complete. Changing it restarts every continuo service (the shared ConfigMap changes); turn it on with `helm upgrade <release> <chart> --reuse-values --set maintenance.enabled=true` and off again with `--set maintenance.enabled=false`. Five services (`run-controller`, `release-controller`, `agent-remediation`, `dead-letter-controller`, `ui`) refuse to start unless `MAINTENANCE_ENABLED` is exactly `true` or `false`. The chart always sets it; a deployment that does not use the chart must set it. |
| `networkPolicy.enabled` | Default-deny ingress within the release plus allow rules derived from the service graph and, with `egress.enabled`, the egress rules that keep the agents behind egress-proxy (default true). Inert on a CNI that does not enforce NetworkPolicy. With it false nothing stops a connection that skips the proxy. |
| `egress.enabled` | Sends the outbound HTTP and gRPC traffic of agent-chat and agent-remediation through egress-proxy, a forward proxy that admits a connection only to a host and port on its allowlist (default true). The chart sets `HTTP_PROXY`, `HTTPS_PROXY` and `NO_PROXY` on both agents, and with `networkPolicy.enabled` adds NetworkPolicies that drop every connection that skips the proxy (see [Outbound traffic](https://github.com/carolsimone/continuo/blob/main/deploy/README.md#outbound-traffic)). It renders the allowlist into the ConfigMap `<release>-egress-proxy`, from `llm.baseUrl` (else the provider's public API), the `GITHUB_BASE_URL` of agent-remediation's `services` entry (its `env` only; with `codeload.<host>` for archive downloads), `s3.endpointUrl` when `minio.enabled` is false, and the two lists below. A URL whose host is an IP address stays out of the allowlist, as does every other IP address the agents are given: egress-proxy admits host names only, and the agents reach those directly (see `egress.directDestinations`). A URL whose host is no valid host name (an underscore, for one) or whose port is outside 1-65535 fails the render, naming the setting. When nothing is left to allow, no egress-proxy object renders and the agents get no proxy variable; their NetworkPolicy still applies. With `egress.enabled` false no proxy runs, no proxy variable is set and the agents' egress is open: set your own proxy variables in their `env`, or manage egress natively (Cilium, GKE Dataplane V2). While it is true, a `services` entry for either agent that sets `HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY` or `S3_ENDPOINT_URL`, or a `GITHUB_BASE_URL` in the `secretEnv` of either agent, fails the render. |
| `egress.allowedDomains` | Extra hosts the proxy admits (default `[]`): `host` or `host:port` (port 443 when omitted), or `*.example.com` for every name below `example.com`. An entry is a DNS name with at most 253 characters, in labels of 1-63 letters, digits and inner hyphens. Rendering fails, naming the key and the entry, on an entry that holds whitespace, a scheme, path, query, fragment or `user@`, an IP address (including forms such as `127.1` or `2130706433`), a bare `*` or a wildcard with fewer than two labels after it, a port outside 1-65535 or with a leading zero (`:0443`), or anything that is not a string. An internationalised name is written in its punycode (`xn--`) form: a non-ASCII entry fails the render, and a non-ASCII URL host is left out of the allowlist. |
| `egress.privateDomains` | The same, for hosts that may resolve to private addresses, such as `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, `100.64.0.0/10` and `fc00::/7`. The hosts of `llm.baseUrl`, a GitHub Enterprise `GITHUB_BASE_URL` and `s3.endpointUrl` may resolve to private addresses without being listed. The proxy never connects to loopback, unspecified, link-local (`169.254.0.0/16`, `fe80::/10`) or cloud-metadata addresses, whatever the entry says. |
| `egress.directDestinations` | Addresses the agents reach without the proxy (default `[]`), as `{cidr, ports}` entries such as `{cidr: 203.0.113.10/32, ports: [5432]}`: a Postgres or Redis on a public address, for one. `cidr` is an IPv4 or IPv6 range with a prefix length of 1 or more and no address bit set past it; `ports` are whole numbers from 1 to 65535, written without a leading zero: YAML reads an unquoted `0443` as the octal number 291, which the chart cannot detect. Rendering fails, naming the entry, on a malformed one and on a range that overlaps loopback, link-local, the unspecified network, a cloud metadata address, IPv4-mapped IPv6 or a NAT64 address of those: a whole `100.64.0.0/10` or `fc00::/7` fails because it contains a metadata address, so list the narrower ranges you need. Every IP address the agents are given (the host of `llm.baseUrl`, `GITHUB_BASE_URL`, `s3.endpointUrl`, or of a URL or `host:port` in their `env` or a `global.*GrpcAddr`) and an `externalDatabase.host` or `externalRedis.host` that is a public IP address are added for you, on their ports. An external Postgres or Redis on a private network, or running as a pod of another namespace, needs no entry; one at a host name that resolves to a public address does. |
| `egress.proxy.replicas` | Replicas of egress-proxy (default 2). |
| `egress.proxy.maxTunnels` | The most tunnels and forwarded requests one replica holds open at once (default 1024, `EGRESS_MAX_TUNNELS`); further ones are refused. |
| `egress.proxy.resources` | Resource requests and limits of the proxy container (default requests `50m` / `64Mi`, limits `500m` / `256Mi`). |
| `metrics.port` | Port every Go service, egress-proxy included, and topology-controller serve Prometheus `/metrics` on (`METRICS_PORT`, default 9464; must differ from every service port and, with `egress.enabled`, from the proxy's ports 3128 and 8093 while the proxy renders). |
| `metrics.podAnnotations` | Adds `prometheus.io/{scrape,port,path}` annotations to those pods (default true). |
| `metrics.scrapeNamespaceSelector` | Namespace selector admitted to the metrics port by the NetworkPolicy; empty (default) admits none. |
| `ingress.enabled` / `ingress.className` / `ingress.host` / `ingress.annotations` / `ingress.tls.*` | Front door for `ui` only. Fully values-driven — no ingress class or cert-manager/ACME assumptions are baked in; set them yourself (see `values-byo.yaml.example`). With `networkPolicy.enabled` it also renders an allow rule for cert-manager's HTTP-01 solver pods (matched by their `acme.cert-manager.io/http01-solver` label, port 8089) so ACME challenges pass the default-deny; the rule selects nothing on installs without cert-manager. |
| `auth.operatorEmails` / `auth.viewerEmails` / `auth.roleMapping` | Role assignment for authenticated users. With `dex.enabled: true`, `operatorEmails` defaults to the Dex demo user's email. |
| `llm.provider` | `anthropic`, `openai` or `openai-compatible`. |
| `llm.baseUrl` | Server root the agents send LLM requests to, without `/v1` (default `""`). Empty: the provider's public API. Required for `openai-compatible`. |
| `llm.model` / `llm.apiKey` (or `llm.existingSecret`) | Optional. Empty `apiKey`: agent-chat and agent-remediation still boot and serve, but LLM (Large Language Model) calls fail until it is set. |
| `github.token` / `github.appId` / `github.installationId` / `github.appPrivateKey` (or `github.existingSecret`) | Optional. `token` is a read-only PAT (Personal Access Token) agent-remediation uses to fetch source; the `app*` fields are a GitHub App ui uses to open fix PRs (Pull Requests) — Create-PR returns `503` until they're set. |
| `streamReaper.enabled` / `streamReaper.schedule` / `streamReaper.retention` | Stream trimming, run every 5 minutes by dead-letter-controller (`STREAM_TRIM_ENABLED`, `STREAM_RETENTION`). `retention` (default `72h`, between 0 and `720h`) is how long an entry a lagging consumer group still needs is kept before it is stored as a dead letter and trimmed. `enabled: false` stops trimming. `schedule` is ignored. See the note below the table. |
| `catalog.backup.enabled` | Whether catalog exports each tenant's graph on `catalog.backup.schedule` (default `true`; `CATALOG_BACKUP_ENABLED`). `false` runs no scheduled exports; `catalog-backup export` still works. See [Catalog backup and restore](#5-catalog-backup-and-restore). |
| `catalog.backup.schedule` | When catalog exports, in UTC: a five-field cron or an `@` descriptor such as `@daily` or `@every 6h` (default `0 2 * * *`; `CATALOG_BACKUP_SCHEDULE`). A slot catalog misses while it is down is not caught up; the next slot runs. Rendering fails on a value that is neither five fields nor an `@` descriptor, and catalog refuses to start on a schedule its cron parser rejects, one that starts with `CRON_TZ=` or `TZ=`, or an `@every` under a minute. |
| `catalog.backup.keep` | How many of each tenant's newest exports catalog keeps (default `7`; `CATALOG_BACKUP_KEEP`); after each stored export it deletes the rest. A whole number of at least 1. A hand-run `catalog-backup export` prunes with the same value. |
| `credentials.delivery` | How the chart hands the credentials (`POSTGRES_PASSWORD`, `REDIS_PASSWORD`, `NEO4J_PASSWORD`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `LLM_API_KEY`, `GITHUB_TOKEN`, `GITHUB_APP_PRIVATE_KEY`, `AUTH_OIDC_CLIENT_SECRET`) to the pods. `env` (default): each is an environment variable read from its Secret. `file`: the chart mounts the same Secret keys read-only under `/var/run/secrets/continuo`, one file per variable named after it, and sets `<VAR>_FILE` to the file, so no credential is in the environment; a key the chart knows is empty (`llm.apiKey` or a GitHub key left unset) is left out. Rendering fails on any other value. Install-time only: it shapes the Deployments, and every service reads both forms (see the note below the table). |
| `credentials.files` | Credentials your company puts in the pod as files of its own (the Vault Agent injector, a Secrets Store CSI volume), per service: `{<service>: {<VAR>: <path>}}`, default `{}`. The chart attaches no Secret for those and sets `<VAR>_FILE` to the path, whatever the delivery; add the pod annotations, ServiceAccount annotations and volumes that put the file there to the service's `services` entry. A service with no enabled `services` entry, or a key that is not one of the credentials above, fails the render. With the Vault Agent injector, a Postgres or Redis password given this way also needs the pod annotation `vault.hashicorp.com/agent-init-first: "true"`, so the file exists before the service's wait init containers read it. Install-time only. |
| `execution.jobS3Secret.name` / `.accessKeyIdKey` / `.secretAccessKeyKey` | The Kubernetes Secret and its two keys the validation, seed-build and compile Job pods read the S3 key pair from, by reference (`JOB_S3_SECRET_NAME`, `JOB_S3_ACCESS_KEY_ID_KEY`, `JOB_S3_SECRET_ACCESS_KEY_KEY` on the shared ConfigMap). Empty name (default): the chart's own S3 Secret and keys (MinIO's, `s3.existingSecret`'s, or the chart's credentials Secret). With a name, the keys default to `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Required when execution-controller's S3 keys are listed in `credentials.files`, since a Job pod can only reference a Kubernetes Secret: rendering fails without it. |
| `imagePullSecrets` | Pull secrets for a private registry, as `[{name: <Secret>}]` (default `[]`). The chart puts them on the ServiceAccount of every enabled `services` entry. egress-proxy, the bundled datastores, Dex, the migration Job and the bundled MinIO bucket-init Job run as the namespace's `default` ServiceAccount and get no pull secrets from the chart: attach those to `default`. Install-time only. |
| `services[].serviceAccount.annotations` / `services[].serviceAccount.imagePullSecrets` | Every enabled `services` entry runs under its own ServiceAccount, named after the service; only an entry with `rbac.enabled` also gets a Role and RoleBinding. `annotations` (default none) go on that ServiceAccount, for a cloud role (`eks.amazonaws.com/role-arn` for IRSA, GKE workload identity) or the identity a Vault role is bound to. `imagePullSecrets`, when the entry sets it, replaces the top-level `imagePullSecrets` for that service; an explicit `[]` gives it none. Install-time only. |
| `services[].podAnnotations` | Annotations on the service's pods (default none), such as the Vault Agent injector's. Install-time only. |
| `services[].credentialDelivery` | `env` or `file` for that service, overriding `credentials.delivery`; empty (default) uses it. Install-time only. |
| `services[].volumes` / `services[].volumeMounts` | Extra volumes of any Kubernetes type (`csi`, `secret`, `configMap`, ...) and their mounts. A `configMap` volume with an empty name mounts the chart's ConfigMap for the services that have one. Install-time only. |
| `services[].resources` / `defaultResources` | Per-service CPU/memory requests and limits; any service without its own `resources` block falls back to `defaultResources`. |

Every continuo service reads each credential either from the variable itself or from a file named by `<VAR>_FILE`, whichever the chart, or another deployment, sets. Setting both fails the service's start with `<VAR>: set <VAR> or <VAR>_FILE, not both`, and so does a file that is missing or empty. One trailing newline is trimmed from the file, nothing else. The Postgres and Redis passwords are read again for every new connection a service opens, so a rotated file reaches the next connection without a restart, and connections already open keep working; every other credential is read once, at start.

dead-letter-controller stores the messages stream consumers give up on (`consumer.dead_letter:v1`) and the outbox rows relays give up on (`outbox.dead_letter:v1`) in its own database, `continuo_dead_letter`, and serves `DeadLetterService` on gRPC port 50055 (`global.deadLetterControllerGrpcAddr`). List, inspect and redrive dead letters with `continuo dlq list`, `continuo dlq show` and `continuo dlq redrive`, which agent-chat reaches through `CONTINUO_DEAD_LETTER_ADDR`. A redrive republishes the stored message to its original stream, only to the consumer group that failed it, within 30 days of the original message; older dead letters are deleted. A dead letter whose stream the streams contract has retired, or whose stream or consumer group the contract does not declare, cannot be redriven: `continuo dlq list` says why, and a redrive refuses it.

dead-letter-controller also trims every Redis stream every 5 minutes, up to the oldest entry any of the stream's consumer groups still needs. An entry older than `streamReaper.retention` (default `72h`) that a lagging group still needs is first stored as a dead letter of kind `trimmed`, listed by `continuo dlq list quarantine` and redrivable for 30 days, and only then trimmed. A group whose service has not started yet keeps every entry newer than the retention. dead-letter-controller refuses to start with a retention outside 0 to `720h`; `streamReaper.enabled: false` stops trimming, and the streams then grow without bound.

release-controller serves `release.v1.ReleaseQuery` on gRPC port 50056 (`global.releaseControllerGrpcAddr`), a read-only call that returns a release's seed reload notices. `continuo release notices` uses it, and agent-chat reaches it through `CONTINUO_RELEASE_CONTROLLER_ADDR`.

## 5. Catalog backup and restore

catalog keeps the live topology, the code-version history behind it and the
remediation case base (rejections, error signatures, proposals, PR outcomes) in
Neo4j. Re-announcing the live release rebuilds the topology; nothing else holds
the history or the case base. So, on `catalog.backup.schedule` (UTC, default
02:00 daily), catalog exports each tenant's whole graph to the S3 bucket and
keeps the newest `catalog.backup.keep` exports of each tenant (default 7).
`catalog.backup.enabled: false` runs no scheduled exports.

**The export.** One object per export, at
`tenants/<tenant>/backups/catalog/<UTC time>.jsonl.gz`, written as a stream:
catalog holds one page of records and one 8 MiB upload part in memory at a
time, whatever the size of the graph. It is gzipped JSON lines: a header line
(format version, tenant, time, catalog version and the counts per label and
relationship type), then one line per node, then one line per relationship.
Every value keeps its Neo4j type: an integer is `{"$int":"…"}`, a float
`{"$float":"…"}` and a datetime `{"$datetime":"…"}` (an RFC 3339 timestamp with
an optional `[zone]` suffix), so no reader rounds or reinterprets one.

**Writes pause during an export.** An export takes the tenant's export lease
first. While it is held, on every catalog pod, catalog's consumers back off and
retry that tenant's writes (reads go on), and the writes land once the export
ends: `continuo_catalog_export_duration_seconds` is how long they waited. The
lease is renewed while the export makes progress, expires within two minutes
when its process dies, and an export that moves no record for two minutes stops
and releases it. An export or a restore that finds the lease held by another is
skipped.

**A shutdown during an export.** catalog stops an export in flight within its
drain window, half of `SHUTDOWN_GRACE` (7.5 s by default): the export aborts
its upload and releases the lease, and the next slot exports the tenant again.
When the process ends before that, the lease lapses within two minutes and the
parts already uploaded stay in the bucket until the lifecycle rule
`AbortIncompleteMultipartUpload` (under the S3 access below) removes them.

### The command

`catalog-backup` ships in catalog's image. Run it in a catalog pod; it reads the
`NEO4J_*` and `S3_*` settings and `CATALOG_BACKUP_KEEP` from the pod's
environment and needs nothing else of catalog's configuration.

```bash
# take an export now: every tenant, or one (--tenant default is how an install
# that has not written anything yet takes its first export)
kubectl -n continuo exec deploy/catalog -- catalog-backup export
kubectl -n continuo exec deploy/catalog -- catalog-backup export --tenant default

# list the stored exports, newest first; after a Neo4j loss name the tenant,
# because a bare list covers only the tenants the graph still holds
kubectl -n continuo exec deploy/catalog -- catalog-backup list --tenant default

# return a tenant's graph to an export; the tenant is the one the key names
kubectl -n continuo exec deploy/catalog -- catalog-backup restore \
  tenants/default/backups/catalog/20261030T020000Z.jsonl.gz --yes
```

`--tenant` takes a tenant id and does not check it against the graph. A bare
`export` or `list` covers the tenants the graph holds, and prints a warning on
stderr when it holds none (the case after a Neo4j loss, while the bucket still
holds the exports): name the tenant then. `export --tenant <id>` of a tenant the
graph does not list stores an empty export, with a warning on stderr. So do not
run `export` on a lost graph before `restore`: the empty export counts toward
`catalog.backup.keep` and can displace a good one.

On the compose stack the source is bind-mounted over the image's
`/app/catalog`, which hides the prebuilt binary, so run
`docker exec catalog go run ./cmd/catalog-backup export` from the container's
working directory instead.

stdout carries only JSON; diagnostics go to stderr.

| Command | stdout |
|---|---|
| `export [--tenant T]` | One line per stored export: `{"object_key","bytes","nodes","relationships","duration_seconds"}` (the last is how long the tenant's writes paused) |
| `list [--tenant T]` | A JSON array of `{"key","size","last_modified"}`, newest first |
| `restore <object key> --yes` | `{"object_key","tenant_id","nodes","relationships"}` |

| Exit code | Meaning |
|---|---|
| 0 | Done |
| 1 | An error. `export` of several tenants goes on past a tenant that fails and prints the exports it stored |
| 2 | Usage: an unknown command or flag, `restore` without `--yes`, or a key outside `tenants/<tenant>/backups/catalog/` |
| 3 | Skipped: another export or a restore holds a tenant's lease; run it again once that ends |

`SIGINT` and `SIGTERM` cancel the work in flight: an export aborts its upload
and releases the tenant's lease, so a cancelled export leaves no object behind.
A hand-run export prunes with `catalog.backup.keep` like a scheduled one.

### Restore after Neo4j is lost

Restore as soon as Neo4j is back, before anything else relies on the catalog:
anything catalog wrote into the empty graph meanwhile is replaced by the export.

1. Find the export: `catalog-backup list --tenant default`. Take the newest.
2. `catalog-backup restore <object key> --yes`. It reads and checks the whole
   file first, with no lease and no write, so a file that cannot be loaded
   exactly, or that names another tenant, leaves the graph untouched. Then it
   takes the tenant's lease, deletes the tenant's graph, loads the nodes and
   then the relationships, runs catalog's schema migration (which brings an
   export written by an older schema up to date) and releases the lease.
3. Re-announce the live release, so the live topology is current again:
   `kubectl -n continuo exec deploy/release-controller -- announce-topology --reannounce-current`.

`--yes` is the safeguard: `restore` deletes the tenant's graph, so without it
the command exits 2 having changed nothing, and it exits 2 for a key that is
not an export's. The tenant is read from the key, never from a flag.

**A failed restore.** A restore that fails before it deletes the graph (a
missing key, a file that does not check out) leaves the graph as it was. One
that fails after it, exit 1 with "partial" on stderr, leaves the tenant's graph
partial, and catalog writes into it again once the lease is released. Run the
same `restore` again: it deletes the tenant's graph first, so a second run
repairs it.

**What stays lost.** A restore returns the tenant's graph to the time of the
export. catalog's consumer groups are not rewound: their positions are in Redis,
and the stream trimmer deletes an entry once every group has read it. So what
was written after the export is gone:

- versions of releases promoted after the export, so a node changed since then
  keeps a stale current version until its next release;
- rejections, proposals and PR outcomes recorded after the export.

An export waits while a release's code versions are between two of their batches, so it never holds part of one release's versions. A batch that dies mid-release holds the export back for at most two minutes.

Topology written after the export comes back with the re-announce in step 3.

### Alerts

catalog exports these on its `/metrics` (`metrics.port`), and only while
`catalog.backup.enabled` is true: with the backup disabled none of the five
exists, whatever `catalog-backup export` has stored.

| Series | Answers |
|---|---|
| `continuo_catalog_export_last_success_timestamp_seconds` | When was the newest good export? The oldest tenant's newest export, read from the bucket when Prometheus scrapes (at most once a minute), so an export taken with `catalog-backup` counts (while the backup is enabled) and the value survives a restart. 0 for a tenant with no export. It has no sample until a read from the bucket succeeds |
| `continuo_catalog_exports_total{outcome}` | Scheduled exports this pod ran: `success`, `failure` or `skipped` |
| `continuo_catalog_export_duration_seconds` | How long a scheduled export paused the tenant's writes |
| `continuo_catalog_export_bytes`, `continuo_catalog_export_records{kind}` | Size and record counts of the last scheduled export |

Two alerts for installs that scrape. The first pairs the age with `absent`,
because the gauge has no sample until the first successful read, and a catalog
that cannot read the bucket would otherwise raise nothing:

```yaml
- alert: CatalogExportStale
  # 93600 s = 26 hours: the nightly export plus two hours of slack.
  expr: |
    time() - continuo_catalog_export_last_success_timestamp_seconds{service="catalog"} > 93600
      or
    absent(continuo_catalog_export_last_success_timestamp_seconds{service="catalog"})
  for: 15m
- alert: CatalogMemoryNearLimit
  # An export streams, so its memory does not grow with the graph; this fires
  # when the pod has no room for the export beside catalog's own use.
  expr: |
    max by (namespace, pod) (container_memory_working_set_bytes{container="catalog"})
      / on (namespace, pod)
    max by (namespace, pod) (kube_pod_container_resource_limits{container="catalog", resource="memory"})
      > 0.85
  for: 5m
```

The rules need cAdvisor and kube-state-metrics. Without them, compare
`process_resident_memory_bytes{service="catalog"}` with the memory limit set in
`services[].resources`. With `catalog.backup.enabled: false` the gauge does not
exist, so the first alert, which pairs `absent()`, would fire at once: drop it
for such installs.

### S3 access

An install on the bundled MinIO needs nothing here. With an external bucket
(`s3.*`), the access key, or each pod's IAM role, must allow these actions. The
shared key already covers `tenants/` for release-controller; the action the
backup adds is `s3:AbortMultipartUpload`, which catalog needs on its own prefix
to discard an export it cancels or that fails.

| Prefix | Component | Actions |
|---|---|---|
| `tenants/` (topology artifacts, `tenants/<tenant>/topologies/<release>/topology.json.gz`) | topology-controller | `s3:PutObject` |
| | release-controller | `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket` |
| | run-controller, catalog | `s3:GetObject` |
| `code-bundles/` (`code-bundles/<release>/bundle.json`) | topology-controller | `s3:PutObject` |
| | catalog, agent-remediation | `s3:GetObject` |
| | release-controller | `s3:DeleteObject`, `s3:ListBucket` |
| `candidate-sql/` (`candidate-sql/<release>/candidate_<node>.*`) | topology-controller | `s3:PutObject` |
| | validation Jobs, agent-remediation | `s3:GetObject` |
| | release-controller | `s3:DeleteObject`, `s3:ListBucket` |
| `tenants/<tenant>/backups/catalog/` (the exports) | catalog | `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket`, `s3:AbortMultipartUpload` |

Set no lifecycle expiry rule on `code-bundles/` or `tenants/`. They hold the code
bundle and the topology artifact of the release in production, which external
tools read ([docs/read-contract.md](../../docs/read-contract.md)), and an age
rule cannot tell the live release from an old one. release-controller deletes a
release's objects when its retention prunes the release, and never for the
production release.

`s3:ListBucket` applies to the bucket, not to its objects: grant it on the
bucket ARN and narrow it with an `s3:prefix` condition. A reader that holds it
also sees a missing object as 404 rather than 403. The example below is for one
key shared by all services, which is what the chart configures; with per-pod IAM
roles, give each role the actions of its own rows, so catalog's role gets the
`catalog` rows only. The shared key also needs the bucket's other prefixes (job
logs and results, dbt manifests and parse caches, chat archives and remediation
proposals), which the table leaves out.

The validation and compile Jobs write `<service>/<release>/compile-extras.json`
beside the manifest they upload, so their key needs `s3:PutObject` on that key
too. A key scoped to `*/manifest.json` alone fails the upload of the extras.

topology-controller also reads `<service>/<release>/compile-extras.json` beside
each dbt manifest. With your own bucket, grant it `s3:GetObject` there and
`s3:ListBucket` on the bucket for those prefixes, so that a stored release
without extras reads as 404, not 403. A 403 still counts as "no extras", with a
WARNING in topology-controller's log that names the key, so the release parses
with dbt's checksum for its seeds until the grant is in place. A 404 is also
"no extras", logged at INFO; any other S3 error pauses the release and
redelivers it.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ObjectsOfTheTopologyBundlesAndCandidates",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": [
        "arn:aws:s3:::continuo/tenants/*",
        "arn:aws:s3:::continuo/code-bundles/*",
        "arn:aws:s3:::continuo/candidate-sql/*"
      ]
    },
    {
      "Sid": "AbortCatalogExportUploads",
      "Effect": "Allow",
      "Action": "s3:AbortMultipartUpload",
      "Resource": "arn:aws:s3:::continuo/tenants/*/backups/catalog/*"
    },
    {
      "Sid": "ListThePrefixes",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::continuo",
      "Condition": {
        "StringLike": {
          "s3:prefix": ["tenants/*", "code-bundles/*", "candidate-sql/*"]
        }
      }
    }
  ]
}
```

An export that is killed mid-upload (a node loss, a `SIGKILL`) leaves its parts
in the bucket, where they are billed but never listed as an export. A bucket
lifecycle rule `AbortIncompleteMultipartUpload` (after one day, say) removes
them.

## 6. Release flow and CI gates

How this chart is tested and published — reference for maintainers, not
something you need to install it.

<details>
<summary>CI gates on every PR, and the release-tag flow</summary>

Every PR that touches this chart (or the install-test harness under
`scripts/install-test/`) runs `install-test.yml`: `helm lint` +
`helm template` + kube-linter across four values topologies (defaults,
`values-byo.yaml.example`, BYO-inline, BYO-existingSecret), then three real
kind installs — bundled, BYO with inline credentials, and BYO with a
pre-created Secret. Each install is checked for completed migrations, healthy
pods, answering ui/Dex endpoints, and a catalog export that reaches the bucket
and shows on catalog's `/metrics`. Install jobs also add a CI-only
low-CPU-request values override so the chart fits the runner's 2 vCPUs. PR
installs use the latest published main-branch images, so they prove the
install path; the e2e suite in `ci.yml` proves the code. The kind cluster
these jobs create enforces `NetworkPolicy`, so the chart's default-deny and
allow policies are exercised here, not just rendered — the BYO fixture
datastores need their own explicit ingress-allow policies because the chart's
default-deny would otherwise block them.

A release goes in this order:

1. Merge the release cut, the pull request that adds `## [X.Y.Z]` to
   `CHANGELOG.md`. Its push runs `deploy.yml`, which publishes `:<git-sha>` of
   every image: a commit that changes only the changelog publishes too, because
   `deploy/**` is in the workflow's release filter.
2. Push the `vX.Y.Z` tag on that commit. It can go at once, with no manual
   publish first.
3. Merge the docs-pin PR once the release has succeeded, then run
   `scripts/release/open-site-bump-pr.sh X.Y.Z`.

Pushing a `vX.Y.Z` git tag runs `release.yml`:

1. It waits for `deploy.yml`'s run of the tagged commit to publish its images,
   on a GitHub-hosted runner, for up to three hours.
2. It refuses any tag whose commit is not an ancestor of `origin/main` — a
   release ships exactly what main ships. Every published image is then retagged
   from the tagged commit's `:<git-sha>` to `:vX.Y.Z`. When an image is missing,
   the release fails closed with remediation instructions. That happens on a
   commit outside the release filter and on one whose `deploy.yml` run was
   cancelled while it queued behind a newer push: tag a newer main commit whose
   run published, or run
   `gh workflow run deploy.yml --ref main -f force_publish=true` and tag that
   head.
3. The same install test runs against the `:vX.Y.Z` images and gates the
   publish.
4. The chart is packaged with `version: X.Y.Z` and `appVersion: vX.Y.Z` and
   pushed to `oci://ghcr.io/carolsimone/charts/continuo`.

After the release workflow succeeds and the docs-pin PR (pinning this
repository's install guides to `X.Y.Z` and adding its release notes) merges,
run `scripts/release/open-site-bump-pr.sh X.Y.Z`. It opens the pull request in
`carolsimone/continuo-site` that moves the public site's quickstart
`chartVersion` to `X.Y.Z`. It runs with your own `gh` login, which needs push
access to that repository; the release workflow does not call it.

ghcr packages created by CI start private, and GitHub has no API for
container-package visibility — the first publish needs a one-time manual flip
to public in the package's UI settings (Package settings → Change visibility).

ghcr OCI tags are mutable: re-pushing an existing `vX.Y.Z` git tag re-runs this
whole flow and silently overwrites both the retagged images and the published
chart version. Treat release tags as immutable by convention — never
force-push or reuse one.

</details>
