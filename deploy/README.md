# Deploying continuo

continuo ships as a single Helm chart, [`deploy/continuo`](continuo/), that
installs every backend service plus optional bundled quickstart datastores
(PostgreSQL, Redis, Neo4j, MinIO, Dex). This page is the entry point; the
chart's own [README](continuo/README.md) is the authoritative install
reference.

## Install paths

| I want to… | Go to |
|---|---|
| Try continuo on any cluster with zero external accounts | [Quickstart](continuo/README.md#1-quickstart-bundled-everything) — one `helm install`, bundled datastores, demo login |
| Install from the published chart without cloning this repo | [OCI install](continuo/README.md#continuo-helm-chart) — `helm install continuo oci://ghcr.io/carolsimone/charts/continuo --version <X.Y.Z>` |
| Run it in production with my own Postgres/Redis/Neo4j/S3/OIDC | [Production (BYO datastores)](continuo/README.md#2-production-bring-your-own-datastores) + [`values-byo.yaml.example`](continuo/values-byo.yaml.example) |
| Understand the security posture before adopting | [SECURITY.md](SECURITY.md) and the chart's [Security defaults](continuo/README.md#3-security-defaults) |
| Configure real OIDC authentication | [AUTH.md](AUTH.md) |
| Release a service from my CD pipeline | [Releasing from CI](#releasing-from-ci-github-actions) — the public `/api/v1` API with a GitHub Actions token |
| Release by hand, or try the API on a local install | [Releasing with your own token](#releasing-with-your-own-token) — mint an ID token and call `/api/v1` with it |
| Control which hosts the assistant and the fix proposer can reach | [Outbound traffic](#outbound-traffic) — the egress proxy and its allowlist |
| Build a dbt image for my team's models | [dbt-image-contract.md](dbt-image-contract.md) |
| Understand how releases are cut and verified | [Release flow and CI gates](continuo/README.md#5-release-flow-and-ci-gates) |

## What is NOT in this repo

The infrastructure that backs the reference production deployment (a
Bitnami-based Postgres/Redis/Neo4j stack with Hetzner-specific storage
classes) lives in a separate private repository, `continuo-infra`, deployed
manually from its own runbook. From this chart's point of view that stack is
simply "datastores you bring" via the `external*` values — any equivalently
reachable Postgres 15+, Redis 7+, and Neo4j 5.x work the same way.

## Licensing of the bundled datastores

continuo itself is Apache-2.0. The chart's optional quickstart mode
(`postgresql.enabled`, `redis.enabled`, `neo4j.enabled`, `minio.enabled`) pulls
upstream container images that carry their own licenses:

| Component | Image | License |
| --- | --- | --- |
| PostgreSQL | `postgres:18.3` | PostgreSQL License (permissive) |
| Redis | `redis:8.6.4` | AGPLv3 / RSALv2 / SSPLv1 (tri-licensed) |
| Neo4j | `neo4j:5.26.28-community` | GPLv3 |
| MinIO | `ghcr.io/carolsimone/continuo-minio` (Bitnami packaging of MinIO, mirrored) | AGPLv3 (MinIO) + Apache-2.0 (Bitnami packaging) |
| Dex | `dexidp/dex:v2.41.1` | Apache-2.0 |

These images are pulled and run as separate processes. continuo does not link
against, embed, modify, or redistribute their code, so their licenses do not
extend to continuo or to your use of it.

If your organisation's policy prohibits running copyleft-licensed datastores,
use the external datastore mode (`external*` / `existingSecret` values) and
bring your own — that is the supported production configuration anyway.

For the full dependency inventory across Go, npm, and Python, see
[docs/third-party-licenses.md](../docs/third-party-licenses.md).

## API credentials for python-api nodes

A `python-api` node can name a Secret in its contract (`secret_ref`) to receive
an API key. The Secret is called `continuo-api-<name>`, lives in the namespace
execution-controller runs Jobs in (its `K8S_NAMESPACE`, which the chart sets to
the release namespace), and is created by the operator: `kubectl create
secret`, an External Secrets Operator `ExternalSecret`, or a Vault Secrets
Operator `VaultStaticSecret`. The chart neither creates nor reads these Secrets.
The `continuo-api-` prefix is reserved for them: the chart refuses to render
when the release name, `fullnameOverride` or any Secret name in values falls
inside it.
See [python-api nodes](../docs/run-projects-in-continuo.md#python-api-nodes).

## Releasing from CI (GitHub Actions)

A CD pipeline releases a service with one authenticated HTTPS call to the
`ui`, at `https://<continuo-host>/api/v1/releases`. It needs no kubeconfig, no
SSH access and no stored secret: the workflow proves which repository it runs
in with its GitHub Actions OIDC token, and the chart's `ciAuth` values say which
repository may release which service.

### 1. Find the repository id

continuo trusts the numeric repository id, never the repository name (a name can
be taken over by a new owner after a rename; the id cannot).

```bash
gh api repos/OWNER/REPO --jq .id
```

### 2. Bind the repository to a service

```yaml
ciAuth:
  bindings:
    core:                              # the service name the release carries
      - repositoryId: "812345678"      # quoted: a bare number fails the chart's schema
        ref: [refs/heads/main]
        allowBootstrap: true           # first release only; set false afterwards
```

A binding grants one GitHub identity the right to release one service. Every
field you set must match the token; `repositoryId` is the only required one.

| Field | Matched against | Meaning |
|---|---|---|
| `repositoryId` | `repository_id` | Exact digits (required). |
| `ref` | `ref` | The token's ref is one of the listed values. |
| `refProtected` | `ref_protected` | `true` allows only protected refs. |
| `environment` | `environment` | The job's environment is one of the listed values. |
| `workflowRef` | `workflow_ref` | The workflow file and ref is one of the listed values. |
| `allowBootstrap` | none | Permits `"bootstrap": true` (default `false`). |

A service may list several bindings; a token that matches any one of them may
release it. With no bindings (the default) no pipeline can use the API: `ui`
does not trust the GitHub issuer at all, so every CI token is a `401`. A token
whose repository matches no binding is refused on every route, including
`GET /api/v1/current-prod`. The other `ciAuth` keys are `issuer` (default
`https://token.actions.githubusercontent.com`; GitHub Enterprise Server uses
`https://<host>/_services/token`) and `audience`, which defaults to the origin of
`auth.publicUrl` (scheme, host and port, no path). Changing `ciAuth` rolls the
`ui` on `helm upgrade`.

### 3. Call the API from the workflow

The workflow requests a token whose audience is the same origin continuo expects,
then submits the release and polls it to a terminal status. This example
releases a dbt service, so it sends only the three required fields. `kind`
names how continuo reads the service's artifact: `dbt` (the default when absent)
or `python`. The first release of a service has no production version to
validate against, so it is sent with `"bootstrap": true`, which needs a binding
with `allowBootstrap: true`. These and the other submit fields are described
below.

```yaml
permissions:
  id-token: write
  contents: read
jobs:
  release:
    runs-on: ubuntu-latest
    timeout-minutes: 60
    env:
      # The origin of auth.publicUrl: scheme://host[:port], no path, no trailing slash.
      CONTINUO_URL: https://continuo.example.com
    steps:
      - name: Submit release to continuo
        run: |
          token() {
            curl -sS -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" \
              "$ACTIONS_ID_TOKEN_REQUEST_URL&audience=$CONTINUO_URL" | jq -r .value
          }
          RELEASE_ID="rel-${GITHUB_SHA::7}-${GITHUB_RUN_NUMBER}"
          curl --fail-with-body -sS -X POST "$CONTINUO_URL/api/v1/releases" \
            -H "Authorization: Bearer $(token)" -H 'content-type: application/json' \
            -d "$(jq -n --arg id "$RELEASE_ID" --arg tag "${GITHUB_SHA::7}" \
                  '{release_id:$id, service:"core", image_tag:$tag}')"
          for _ in $(seq 1 300); do              # 300 polls x 10 s = 50 minutes
            s=$(curl -sS -H "Authorization: Bearer $(token)" \
                  "$CONTINUO_URL/api/v1/releases/$RELEASE_ID") || s='{}'
            [ "$(jq -r .terminal <<<"$s")" = true ] && break
            sleep 10
          done
          jq . <<<"$s"; [ "$(jq -r .status <<<"$s")" = promoted ]
```

GitHub Actions tokens expire within minutes, so the example asks for a fresh
one on every call instead of reusing the first.

### The API

All three routes live under `/api/v1` on the `ui`. Each takes
`Authorization: Bearer <token>`; the scheme name is case-insensitive.

| Route | Purpose |
|---|---|
| `POST /api/v1/releases` | Submit a release. Answers `202` with `{release_id, status}`. |
| `GET /api/v1/releases/{id}` | Read a release (below). |
| `GET /api/v1/current-prod` | Which release production is serving: `{current_prod_release_id, node_count, updated_at}`. An empty `current_prod_release_id` means production has never been seeded. |

**Submit body.** `release_id`, `service` and `image_tag` are required;
`bootstrap` (a JSON boolean), `kind` (`dbt`, the default, or `python`), `repo`
and `commit_sha` are optional. Any other field is a `400`. `release_id` must
match `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`: it starts with a letter or digit and
continues with letters, digits, `.`, `_` or `-`, at most 128 characters in all;
anything else is a `400`. With a CI token, `repo` and `commit_sha` are taken from
the token (`repository` and `sha`): leave them out, or send values equal to the
token's, because a different value is a `403` `claim_mismatch`. A person calling
with their own token (see [AUTH.md](AUTH.md#bearer-tokens)) must send `repo` and
`commit_sha` in the body. `"bootstrap": true` promotes without validation and is
for the first release of a service; a CI token may send it only when its
binding sets `allowBootstrap`. Submitting the same `release_id` again with the
same body is idempotent and answers `202`; the same `release_id` with a
different service, `image_tag`, `kind`, bootstrap flag, `repo` or `commit_sha`
is a `409` `release_kind_conflict`, so use a fresh id for a new release.

**Release read.** The answer carries `release_id`, `service`, `status`,
`terminal` (`true` once the status is `promoted`, `rejected` or `superseded`),
`bootstrap`, `repo`, `commit_sha`, `reject_reason`, `reject_detail`, and `ui_url`,
the release's page in the dashboard. A CI token can read only releases of the
services its bindings grant; a release of any other service answers `404`
`not_found`, exactly as a release that does not exist. A pipeline polling for
`terminal` stays well inside the read limit (300 a minute per principal) with
one read every few seconds.

**Errors.** Every failure is JSON, `{"error": "<message>", "code": "<code>"}`.

| Status | `code` | Meaning |
|---|---|---|
| 400 | `bad_request` | Malformed JSON, an unknown or mistyped field, a missing required field, or a `release_id` outside the pattern above. |
| 401 | `invalid_token` | The bearer token is malformed, expired, for the wrong audience, from an untrusted issuer, signed with an algorithm other than RS256, or valid for longer than one hour. The response carries `WWW-Authenticate: Bearer error="invalid_token"`. A request with no credential at all answers `401` with code `unauthenticated`. |
| 403 | `forbidden` | The identity may not do this: a repository not bound to the service it submits, a repository bound to no service at all (on every route), a viewer submitting, or a CI token calling any other route. |
| 403 | `claim_mismatch` | The body's `repo` or `commit_sha` differs from the token's. |
| 403 | `bootstrap_not_allowed` | `"bootstrap": true` from a binding without `allowBootstrap`. |
| 404 | `not_found` | No such release, a release of a service the CI token's repository is not bound to, or an id outside the `release_id` pattern. |
| 409 | `release_kind_conflict` | The `release_id` already names a release with a different service, `image_tag`, `kind`, bootstrap flag, `repo` or `commit_sha`. The release service makes this check. A person's `error` carries its message; a CI token gets `release id already exists with different content`, which names no other release. |
| 429 | `rate_limited` | More than 30 submits, or more than 300 reads (`GET /api/v1/releases/{id}` and `GET /api/v1/current-prod` together), in a minute from one principal (a repository for CI, a user for a person), counted per `ui` pod. Submits and reads have separate budgets. |
| 503 | `auth_unavailable` | The token's issuer could not be reached, its discovery document or signing keys could not be read, or its key set has no usable RS256 key, publishes a `kid` twice, or holds an RSA key shorter than 2048 bits; the dashboard keeps working. Retry. |
| 503 | `upstream_unavailable` | The release service is unreachable or failed. Retry; a submit is idempotent. |

### Networking

- The ingress host (`ingress.host`) must be reachable from GitHub-hosted
  runners. A cluster behind a firewall needs self-hosted runners that can reach
  it.
- The `ui` pod fetches the issuer's signing keys over HTTPS, so it must reach
  `token.actions.githubusercontent.com` (or your GitHub Enterprise Server host)
  on port 443. The chart's NetworkPolicies restrict the `ui`'s ingress only, so
  this works by default; a cluster-wide egress policy or a forced proxy must
  allow it. While the issuer is unreachable, bearer calls answer `503`
  `auth_unavailable` and the browser login is unaffected.
- Where egress must go through a proxy, add `HTTPS_PROXY` together with
  `NODE_USE_ENV_PROXY=1` to the `ui` entry's `env` in `services`, plus
  `NO_PROXY` covering the in-cluster service names the `ui` calls
  (`release-controller`, `state`, `orchestrator`, `agent-chat`,
  `agent-remediation`, and your IdP when it is in-cluster). The `ui` runs on
  Node.js 26, whose built-in `fetch` honours the proxy variables only when
  `NODE_USE_ENV_PROXY=1` is set; without it the key fetch ignores `HTTPS_PROXY`
  and tries the issuer directly.

## Releasing with your own token

A person calls the same `/api/v1` routes with their own bearer token: an ID
token from the `ui`'s login provider, for an identity with the operator role
(see [AUTH.md](AUTH.md#bearer-tokens)). It is valid for at most one hour; mint a
new one when a call answers `401`.

On a local install with the bundled Dex, forward the `ui` (the API) and Dex
(the token issuer), then trade the demo operator account's password for a
token:

```bash
kubectl -n continuo port-forward svc/ui 8090:8090 &
kubectl -n continuo port-forward svc/continuo-dex 5556:5556 &

CLIENT_SECRET=$(kubectl -n continuo get secret continuo-dex -o jsonpath='{.data.client-secret}' | base64 -d)
TOKEN=$(curl -s -u "continuo-ui:${CLIENT_SECRET}" http://localhost:5556/dex/token \
  -d grant_type=password -d scope="openid email profile" \
  -d username=admin@example.com -d password=password | jq -r .id_token)

curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8090/api/v1/current-prod
```

If `echo $TOKEN` prints `null`, the login failed: check the Dex port-forward
and the password. A release posted with a person's token must carry `repo` and
`commit_sha` in its body, since there is no CI token to supply them:

```bash
curl -s -X POST http://localhost:8090/api/v1/releases \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"release_id":"rel-core-v1","service":"core","image_tag":"v1","repo":"<owner>/<repo>","commit_sha":"<sha>"}'
```

With your own IdP, the token is an ID token it issues for the `ui`'s client id;
how a script obtains one depends on the IdP. An operator token may also
bootstrap a service (`"bootstrap": true`).

## Outbound traffic

agent-chat and agent-remediation are the two services where a language model
decides what happens next, so they reach the outside world only through
**egress-proxy** (`egress.enabled`, on by default). The proxy admits a
connection only to a host and port on its allowlist, resolves the name itself,
refuses loopback, unspecified, link-local and cloud-metadata addresses always
and private addresses unless the entry allows them, and connects only to the
addresses it checked. The chart sets `HTTP_PROXY`, `HTTPS_PROXY` and `NO_PROXY`
on both agents, and, with `networkPolicy.enabled`, their NetworkPolicy drops
every other outbound connection.

**The allowlist follows your settings.** The chart builds it from:

| Setting | Allowed |
|---|---|
| `llm.baseUrl` empty, `llm.provider: anthropic` | `api.anthropic.com:443` |
| `llm.baseUrl` empty, `llm.provider: openai` | `api.openai.com:443` |
| `llm.baseUrl` set | its host and port, which may resolve to a private address |
| `GITHUB_BASE_URL` in the `env` of agent-remediation's `services` entry (default `https://api.github.com`) | `api.github.com:443` and `codeload.github.com:443` |
| a GitHub Enterprise `GITHUB_BASE_URL` | its host and `codeload.<host>` on its port, which may resolve to private addresses |
| `s3.endpointUrl` (with `minio.enabled: false`) | its host and port, which may resolve to a private address |
| `egress.allowedDomains` / `egress.privateDomains` | the entries listed |

See what it holds with `kubectl -n <namespace> get configmap
<release>-egress-proxy -o yaml` (for a release named `continuo`, the ConfigMap
is `continuo-egress-proxy`). The agents' S3 client reaches the endpoint through
the proxy, and signs its requests with the static keys the chart gives it
(`s3.accessKeyId` and `s3.secretKey`, or `s3.existingSecret`; an external bucket
requires them), so it makes no call to STS or the instance metadata service.

**A model you host.** Point the chart at its root URL:

```yaml
llm:
  provider: openai-compatible
  baseUrl: http://gpu-box.internal:8000   # the root, without /v1
  model: llama-3.3-70b
```

A host name goes through the proxy and may resolve to a private address. An IP
address (`http://10.0.0.7:8000`) is reached directly: the chart leaves it out of
the allowlist, adds it to `NO_PROXY` and admits it, on its port, in the agents'
NetworkPolicy. Either way nothing else needs configuring, as long as the port is
one the model's pod listens on:

A NetworkPolicy sees a connection after a Service has translated it, as the
**pod's** address and port. So a port you configure must be one the pod listens
on, whether the address is reached directly or through the proxy: a Service
that maps port 80 to a pod port 8000 is not reachable on port 80, because the
proxy reaches private addresses only on the port of the allowlist entry, and
the agents only on the port of the direct destination. An IP address is for a
machine or a pod, not for a Service.

A host must be a DNS name of letters, digits and inner hyphens, or an IP
address. A `llm.baseUrl`, `GITHUB_BASE_URL` or `s3.endpointUrl` whose host is
anything else (`http://vllm_x:8000`, with an underscore, for one) fails the
render with a message that names the setting and the value; use a DNS-valid
name or the IP address. The same holds for a port outside 1-65535.

**Something else the agents reach.** Add a host name to `egress.allowedDomains`
(`host` or `host:port`, port 443 when omitted; `*.example.com` for every name
below it), or to `egress.privateDomains` when it resolves to a private address.

**An address that is not reached through the proxy** goes in
`egress.directDestinations`:

```yaml
egress:
  directDestinations:
    - {cidr: 203.0.113.10/32, ports: [5432]}
```

Postgres and Redis speak their own protocols and ignore the proxy. With
`networkPolicy.enabled`, an external Postgres or Redis on a private network, or
running as a pod of any namespace, is already admitted on its configured port
(`externalDatabase.port`, `externalRedis.port`), and one whose
`externalDatabase.host` or `externalRedis.host` is a public IP address is added
for you. One whose host is a name that resolves to a public address needs an
entry: the chart cannot know where the name points, and `helm upgrade` prints
the configured host as a reminder. Each entry is checked as written, and the
render fails naming it unless:

- `cidr` is an IPv4 or IPv6 range with a prefix length of 1 or more and no address
  bit set past it. `0.0.0.0/0` and `::/0` are refused.
- the range overlaps none of loopback, link-local, the unspecified network, a
  cloud metadata address, IPv4-mapped IPv6 and a NAT64 address standing for one
  of those. A whole `100.64.0.0/10` and a whole `fc00::/7` are refused because
  they contain metadata addresses (`100.100.100.200`, `fd00:ec2::/32`): list the
  narrower ranges you need.
- `ports` is a non-empty list of whole numbers from 1 to 65535. Write them
  without a leading zero: YAML reads an unquoted `0443` as the octal number 291,
  which the chart cannot tell from `291`, so it opens port 291 and renders
  silently. A quoted `"0443"` is not a number and fails the render.

**How the agents are kept to those rules.** `NO_PROXY` lists, so that the
agents reach them directly:

- `localhost`, `127.0.0.1` and `::1`;
- the release's own Services, in every form (`name`, `name.<namespace>`,
  `name.<namespace>.svc`, `name.<namespace>.svc.cluster.local`) and on every port;
- every other in-cluster address the agents are given (a `global.*GrpcAddr`, or a
  URL or `host:port` in their `env`) as `host:port`, when its host has no dot or
  ends in `.svc` or `.svc.cluster.local`, unless the allowlist admits that host
  and port, in which case the proxy carries it;
- every IP address the agents are given (the host of a URL, or of a URL or
  `host:port` in their `env` or a `global.*GrpcAddr`, which egress-proxy would
  refuse because it admits host names only), an external Postgres or Redis at a
  public IP address, and each `egress.directDestinations` range. A loopback
  address needs no entry, nor does a listen address (`0.0.0.0`) in their `env`;
  one in link-local or a cloud metadata range fails the render, naming the
  setting.

Every other address goes through the proxy. A `<name>.<namespace>` address in
another namespace written without `.svc`, and a name with a trailing dot, are not
treated as in-cluster: they go through the proxy, which refuses them unless the
allowlist names them. Write `<name>.<namespace>.svc`.

With `networkPolicy.enabled`, the agents may reach:

- DNS (port 53);
- every pod of the release namespace on any port: the proxy, the services and the
  bundled datastores;
- an external Postgres or Redis on its configured port, on the private ranges and
  on pods of any namespace;
- every pod of another namespace on the port of each in-cluster address the
  agents are given there (`foo.tools.svc:80` admits every pod of `tools` on port
  80, not only the target pods);
- the `egress.directDestinations` ranges and every IP address they are given,
  including an external Postgres or Redis at a public IP address, on their ports.

egress-proxy itself may resolve names, reach public addresses on its allowlisted
ports, and reach private addresses and pods of any namespace only on the ports of
entries that allow private addresses. Only the two agents may connect to it, on
its proxy port.

**When there is no proxy.** An install whose every destination is an IP address
(a model, GitHub Enterprise and S3 at IP addresses, or the bundled MinIO) and
that lists no `egress.allowedDomains` or `egress.privateDomains` has an empty
allowlist, which the proxy refuses to start on. The chart then renders no
egress-proxy object and sets no proxy variable; the agents reach those addresses
directly, and their NetworkPolicy still limits them to the destinations above.

**When a call is refused,** egress-proxy logs a warning `egress denied` with the
host, port, client address, method and reason, and counts it in
`continuo_egress_requests_total{decision="denied"}`:

```bash
kubectl -n <namespace> logs -l app.kubernetes.io/name=egress-proxy --tail=-1 | grep "egress denied"
```

| Reason | Meaning |
|---|---|
| `not_allowed` | The host and port are not on the allowlist. An IP address, which the proxy never admits, belongs in `egress.directDestinations`. |
| `private_address` | The name resolved to a loopback, unspecified or link-local address, or to a private address its entry does not allow. |
| `metadata` | The name resolved to a cloud metadata address. |
| `resolve_failed` | The name did not resolve. |
| `dial_failed` | The connection to the destination failed. |
| `upstream_error` | The exchange with the destination failed other than while connecting. |
| `capacity` | The replica holds its maximum number of open tunnels and forwarded requests (`egress.proxy.maxTunnels`). |
| `bad_request` | The destination host and port of the request cannot be read. |
| `unsupported` | The method is neither `CONNECT` nor an absolute-form HTTP request, or the request asks for a protocol upgrade, which is not forwarded. |

**Your own egress controls.** On Cilium or GKE Dataplane V2 you can write
host-name rules natively. Set `egress.enabled: false` to run no proxy and manage
the agents' egress yourself: the chart then sets no proxy variable and renders no
egress NetworkPolicy, so you may set `HTTP_PROXY`, `HTTPS_PROXY` and `NO_PROXY` in
the agents' `env`.

**What it does not stop.**

- A CNI that does not enforce NetworkPolicy, or `networkPolicy.enabled: false`:
  the proxy variables only direct the agents' own clients, and nothing stops a
  connection that skips the proxy. `helm upgrade` warns when
  `networkPolicy.enabled` is false.
- Name resolution: the agents may resolve any name, so a DNS query can carry data
  to a name server.
- An allowed host: the proxy sees host names and ports, not the encrypted
  traffic. It cannot stop a client that opens a tunnel to an allowed host and then
  reaches another site through a shared network behind it, nor a request to an
  allowed provider made with another account's key.
- Everything inside the release namespace, and the namespaces and ports the lists
  above open.

## Requirements at a glance

- Kubernetes `>=1.27`, Helm 3.14+.
- PostgreSQL only, today. The `externalDatabase.*` keys are deliberately
  engine-agnostic, but every migration and query assumes Postgres; MySQL is
  a roadmap item, not a supported option.
- In-cluster encryption is the operator's: the chart configures no mTLS; run
  a service mesh (Istio, Linkerd) or CNI-level encryption if you need
  encrypted pod-to-pod traffic. External datastore connections use whatever
  transport you configure (`externalDatabase.sslMode: require`, TLS
  endpoints for S3, etc.).
