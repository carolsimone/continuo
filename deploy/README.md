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
  on port 443. The chart's NetworkPolicies restrict ingress only, so this works
  by default; a cluster-wide egress policy or a forced proxy must allow it. While
  the issuer is unreachable, bearer calls answer `503` `auth_unavailable` and
  the browser login is unaffected.
- Where egress must go through a proxy, add `HTTPS_PROXY` together with
  `NODE_USE_ENV_PROXY=1` to the `ui` entry's `env` in `services`, plus
  `NO_PROXY` covering the in-cluster service names the `ui` calls
  (`release-controller`, `state`, `orchestrator`, `agent-remediation`, and your
  IdP when it is in-cluster). The `ui` runs on Node.js 26, whose built-in
  `fetch` honours the proxy variables only when `NODE_USE_ENV_PROXY=1` is set;
  without it the key fetch ignores `HTTPS_PROXY` and tries the issuer directly.

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
