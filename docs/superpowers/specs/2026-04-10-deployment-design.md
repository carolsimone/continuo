# Deployment Design — Hetzner Dev Environment

**Date:** 2026-04-10
**Status:** Approved
**Scope:** Dev/testing deployment of all continuo services to a single Hetzner server

---

## Context

The continuo monorepo has 8 application services (6 Go, 1 Node.js, 1 Python) and 4 infrastructure dependencies (PostgreSQL, Redis, Neo4j, S3). The target is a single Hetzner Cloud server used as a persistent dev/testing environment. `executor-controller` and `k8s-controller` require a real Kubernetes API, so Docker Compose alone is not viable — k3s is required regardless.

This design is intentionally production-viable. The split between infra and app charts means the dev setup evolves into production without a rewrite.

---

## Architecture Overview

Single Hetzner server running k3s (single-node). All resources live in the `continuo` namespace.

```
Hetzner Server
└── k3s (single-node)
    ├── Traefik (ingress, bundled with k3s, TLS via Let's Encrypt)
    │
    ├── App services (8 Deployments)
    │   ├── state                  (HTTP :8082, gRPC :50051)
    │   ├── graph                  (HTTP :8081, gRPC :50052)
    │   ├── startup-controller     (HTTP :8083)
    │   ├── executor-controller    (HTTP :8084)
    │   ├── k8s-controller         (HTTP :8085)
    │   ├── dependency-controller  (HTTP :8086)
    │   ├── ui-service             (HTTP :8090)
    │   └── manifest-controller    (no HTTP — Redis stream consumer)
    │
    └── Infrastructure
        ├── PostgreSQL   — Bitnami chart, single-instance, 5 databases via initdb script
        ├── Redis        — Bitnami chart, single-instance (no Sentinel for dev)
        ├── Neo4j        — Official Neo4j Helm chart, single-instance
        └── S3           — Hetzner Object Storage (external, S3-compatible, replaces LocalStack)
```

Persistent volumes for Postgres, Redis, and Neo4j are backed by hcloud-csi-driver (Hetzner Block Storage).

---

## Helm Chart Structure

Two independent charts deployed separately:

```
deploy/
  infra/                   # infrastructure chart — deployed once, rarely changed
    Chart.yaml
    values.yaml
    charts/                # populated by helm dep update
      (bitnami/postgresql)
      (bitnami/redis)
      (neo4j/neo4j)

  app/                     # application chart — deployed on every release
    Chart.yaml
    values.yaml
    values.secret.yaml     # gitignored — passwords, keys
    templates/
      _helpers.tpl         # shared name/label helpers
      deployment.yaml      # single template, iterates over services map
      service.yaml         # same pattern
      ingress.yaml         # only for services with ingress.enabled: true
      configmap.yaml       # shared env vars (DB host, Redis host, S3 endpoint)
```

### Why two charts

Infra (Postgres, Redis, Neo4j) is stable — it changes version occasionally, not on every commit. App services change constantly. Separating them means a `helm upgrade` on the app chart cannot accidentally restart a database pod. This split is also the correct production-grade structure.

### App chart values shape

All 8 services are entries in a single `services` map. One `deployment.yaml` template iterates over the map — no per-service YAML files, no copy-paste.

```yaml
global:
  imageTag: latest
  dockerHubUser: youruser

services:
  state:
    image: continuo-state
    port: 8082
    grpcPort: 50051
    ingress:
      enabled: false
    env: {}

  graph:
    image: continuo-graph
    port: 8081
    grpcPort: 50052
    ingress:
      enabled: false
    env:
      NEO4J_URI: bolt://continuo-infra-neo4j:7687

  ui-service:
    image: continuo-ui-service
    port: 8090
    ingress:
      enabled: true
      host: dev.yourdomain.com
    env: {}

  manifest-controller:
    image: continuo-manifest-controller
    port: null             # no HTTP — Redis consumer only
    ingress:
      enabled: false
    env: {}
    # Services with port: null skip containerPort, HTTP ingress, and HTTP health probes.
    # The deployment template uses an exec probe (e.g. pgrep python) instead.

  # startup-controller, executor-controller, k8s-controller, dependency-controller
  # follow the same pattern
```

Shared config (Postgres host, Redis host, S3 endpoint) lives in a `ConfigMap` mounted by all services. Secrets (`values.secret.yaml`, gitignored) hold passwords and API keys — passed at deploy time with `-f values.secret.yaml`.

### Infra chart — Postgres multi-database

The project has 5 separate databases (one per service: state, startup, executor, dependency, k8s-controller). The Bitnami Postgres chart supports an `initdb` script in `values.yaml` that creates all databases on first boot. One Postgres instance, multiple databases — correct for dev.

### Deploy commands

```bash
# Infrastructure — once (or on version bumps)
helm install continuo-infra ./deploy/infra -n continuo

# Application — every release
helm upgrade --install continuo-app ./deploy/app \
  -f deploy/app/values.yaml \
  -f deploy/app/values.secret.yaml \
  --set global.imageTag=$(git rev-parse --short HEAD) \
  -n continuo
```

---

## CI/CD Pipeline

GitHub Actions triggers on push to `main`.

```
git push main
    │
    ▼
GitHub Actions
    ├── Detect which services changed (path filters)
    ├── Build changed images with docker buildx (linux/amd64)
    ├── Tag with git SHA  →  youruser/continuo-state:abc1234
    ├── Push to Docker Hub
    └── SSH to Hetzner server → helm upgrade --set global.imageTag=abc1234
```

**Selective builds:** only services whose source paths changed are rebuilt. All rebuilt images share the same git SHA tag — simple and auditable.

**Workflow files:**

```
.github/workflows/deploy.yml
  on: push to main
  jobs:
    detect-changes   → outputs matrix of changed services
    build-push       → matrix job, builds and pushes each changed image
    deploy           → SSH to server, helm upgrade with new tag
```

**Required GitHub Actions secrets:**

| Secret | Purpose |
|---|---|
| `DOCKERHUB_USERNAME` | Docker Hub login |
| `DOCKERHUB_TOKEN` | Docker Hub access token |
| `HETZNER_SSH_KEY` | Private key to SSH into the server |
| `HETZNER_HOST` | Hetzner server IP |

The deploy step SSHes into the server and runs `helm upgrade` directly — no need to expose the Kubernetes API publicly. k3s writes its kubeconfig to `/etc/rancher/k3s/k3s.yaml` on the server.

---

## Networking & Ingress

**Internal traffic** (service-to-service) uses Kubernetes ClusterIP DNS names:
- `continuo-state:8082` for HTTP
- `continuo-state:50051` for gRPC
- `continuo-graph:50052` for gRPC

No Ingress is needed for internal traffic.

**Public traffic** via Traefik (bundled with k3s):

```
Internet :443
    │
Traefik
    ├── dev.yourdomain.com       → ui-service:8090
    └── api.dev.yourdomain.com  → state:8082  (optional)
```

DNS A record points to the Hetzner server IP. Traefik handles Let's Encrypt cert provisioning automatically.

**Firewall:** only ports 80 and 443 are publicly accessible. Postgres (5432), Redis (6379), and Neo4j (7474/7687) are cluster-internal only.

---

## Production Evolution Path

This design is production-viable as-is. The natural progression:

| Phase | Change |
|---|---|
| Dev (now) | Umbrella chart split into infra/app. Single-instance infra. |
| Production v1 | Enable HA infra: CloudNativePG, Redis Sentinel. Add External Secrets Operator. Add ArgoCD or Flux for GitOps. |
| Production v2 | Split app chart into per-service charts if independent release cadences are needed. |

No step requires discarding the previous step's work.

---

## Open Decisions

- **Domain name:** `dev.yourdomain.com` — to be set in `values.yaml`
- **Docker Hub org/user:** to be confirmed, affects all image references
- **Hetzner Object Storage bucket:** needs to be created and credentials added to `values.secret.yaml`
- **Server size:** CX21 (2 vCPU, 4GB RAM) is likely sufficient for dev; CX31 if Neo4j needs more headroom
