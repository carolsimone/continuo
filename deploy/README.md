# Deploying Continuo

Continuo ships as a single Helm chart, [`deploy/continuo`](continuo/), that
installs every backend service plus optional bundled quickstart datastores
(PostgreSQL, Redis, Neo4j, MinIO, Dex). This page is the entry point; the
chart's own [README](continuo/README.md) is the authoritative install
reference.

## Install paths

| I want to… | Go to |
|---|---|
| Try Continuo on any cluster with zero external accounts | [Quickstart](continuo/README.md#1-quickstart-bundled-everything) — one `helm install`, bundled datastores, demo login |
| Install from the published chart without cloning this repo | [OCI install](continuo/README.md#continuo-helm-chart) — `helm install continuo oci://ghcr.io/carolsimone/charts/continuo --version <X.Y.Z>` |
| Run it in production with my own Postgres/Redis/Neo4j/S3/OIDC | [Production (BYO datastores)](continuo/README.md#2-production-bring-your-own-datastores) + [`values-byo.yaml.example`](continuo/values-byo.yaml.example) |
| Understand the security posture before adopting | [SECURITY.md](SECURITY.md) and the chart's [Security defaults](continuo/README.md#3-security-defaults) |
| Configure real OIDC authentication | [AUTH.md](AUTH.md) |
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

Continuo itself is Apache-2.0. The chart's optional quickstart mode
(`postgresql.enabled`, `redis.enabled`, `neo4j.enabled`, `minio.enabled`) pulls
upstream container images that carry their own licenses:

| Component | Image | License |
| --- | --- | --- |
| PostgreSQL | `postgres:18.3` | PostgreSQL License (permissive) |
| Redis | `redis:8.6.4` | AGPLv3 / RSALv2 / SSPLv1 (tri-licensed) |
| Neo4j | `neo4j:5.26.28-community` | GPLv3 |
| MinIO | `minio/minio` | AGPLv3 |
| Dex | `dexidp/dex:v2.41.1` | Apache-2.0 |

These images are pulled and run as separate processes. Continuo does not link
against, embed, modify, or redistribute their code, so their licenses do not
extend to Continuo or to your use of it.

If your organisation's policy prohibits running copyleft-licensed datastores,
use the external datastore mode (`external*` / `existingSecret` values) and
bring your own — that is the supported production configuration anyway.

For the full dependency inventory across Go, npm, and Python, see
[docs/third-party-licenses.md](../docs/third-party-licenses.md).

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
