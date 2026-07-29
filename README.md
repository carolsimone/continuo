# Continuo

[![CI](https://github.com/carolsimone/continuo/actions/workflows/ci.yml/badge.svg)](https://github.com/carolsimone/continuo/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Version](https://img.shields.io/github/v/tag/carolsimone/continuo?sort=semver&label=version)](https://github.com/carolsimone/continuo/tags)

Continuo is a control plane for data pipelines with built-in agentic
remediation. It orchestrates independent dbt (data build tool) projects
without requiring them to know about each other, automatically stitching
them into one dependency graph, and it heals broken pipelines by having an
LLM (Large Language Model) propose a fix before anything reaches production.

## How it works

A DAG (Directed Acyclic Graph) is just the shape of your dependencies — and
those dependencies are already fully described in your SQL. So Continuo
infers them from schema and table names across projects instead of asking
you to declare a DAG by hand. Each dbt project stays fully independent:
hardcode the schema and table names your models read and write, and Continuo
stitches everything into one big DAG behind the scenes.

Onboarding a project is three things: publish a dbt image, publish its
manifest to a known path, and POST to the `/releases` endpoint at CD
(Continuous Deployment) time. Continuo takes it from there.

## How it keeps production safe

Continuo validates every release the way a blue/green deployment validates a
software release: stand up the new version alongside the current one, prove
it's healthy, then cut over. Data doesn't move the way software traffic
does, so the mechanism looks different — Continuo runs the new DAG against a
second, temporary schema (a schema copy, not a data copy, so no data
actually moves) and checks that the resulting lineage is correct and won't
break anything downstream. Only once that passes does the new dbt image
become the production image — which is why we call it "a sort of"
blue/green: the goal is the same, the technique had to be reinvented for
data.

Most orchestration systems only catch a broken model after it has already
run against production data. Continuo validates the full downstream lineage
before a release is promoted, so a breaking change never reaches production
in the first place. If validation fails, production keeps running the last
version that passed — never a broken DAG — while a remediation agent
proposes a fix for a human to review and merge. The same remediation path
for live production runs (not just new releases) is still being refined and
is coming soon.

## Status and roadmap

Beta. dbt is the only supported runtime today; Python is next, on the same
integration contract, within a few weeks.

Remediation is the first agentic capability, not the last. Performance-tuning
agents are planned next: looking at Kubernetes resource usage, the SQL
itself, and the query planner to suggest better-performing SQL based on how
downstream models actually consume the data. This is still in design and not
available yet.

## Deploying it

Run the full system on your own Kubernetes cluster today; a hosted Cloud
version is coming. Bring your own Postgres and Redis, or use the bundled
Helm charts to stand them up alongside Continuo.

## Try it locally

Everything below pulls pre-built images — nothing is compiled from source,
so this takes minutes, not a full local build.

**Prerequisites**

- Docker Desktop, or [colima](https://github.com/abiosoft/colima) (`colima start`)
- [kind](https://kind.sigs.k8s.io/) — `brew install kind`
- [kubectl](https://kubernetes.io/docs/tasks/tools/) — `brew install kubectl`
- [Helm](https://helm.sh/) 3.14+ — `brew install helm`

**Steps**

```bash
# 1. Create a local Kubernetes cluster
kind create cluster --name continuo

# 2. Install Continuo from the published Helm chart (pre-built images, no clone needed)
helm install continuo oci://ghcr.io/carolsimone/charts/continuo \
  --version 0.1.0 -n continuo --create-namespace

# 3. Wait for everything to come up
kubectl -n continuo get pods -w

# 4. Port-forward the UI and the identity provider
kubectl -n continuo port-forward svc/ui-service 8090:8090 &
kubectl -n continuo port-forward svc/continuo-dex 5556:5556 &

# 5. One-time: let your browser resolve the in-cluster login issuer
echo "127.0.0.1 continuo-dex" | sudo tee -a /etc/hosts

# 6. Open it
open http://localhost:8090
```

Log in with the demo account: `admin@example.com` / `password`.

This installs everything bundled — Postgres, Redis, Neo4j, MinIO, and Dex
(the identity provider) — for evaluation only. It's not meant to hold real
data: no backup, no high availability, one static login. For production
installs bringing your own datastores, see [deploy/README.md](deploy/README.md).

## Architecture pack

This folder is the operational architecture map for the current Continuo microservice system.

Use it in this order:

1. [01-topology.md](docs/arch/01-topology.md)
   High-level static view: services, owned datastores, major streams, and external systems.
2. [02-interaction-matrix.md](docs/arch/02-interaction-matrix.md)
   Fast reference for who calls what, who owns what, and which direction data moves.
3. [03-sequence-flows.md](docs/arch/03-sequence-flows.md)
   Dynamic behavior: startup, steady-state execution, retry, and rerun.
4. [04-service-ownership.md](docs/arch/04-service-ownership.md)
   Compact ownership sheet: owned durable state, owned gRPC server surface, and Redis stream roles.
5. `services/`
   Detailed dossier for each service: purpose, storage, Redis, gRPC, S3, and side effects.

This pack is based on:

- The current code under this repository
- The current multi-repo architectural snapshot
- Direct inspection of Redis, gRPC, Neo4j, Postgres, Kubernetes, and S3 integration points

Service dossiers:

- [state.md](docs/arch/services/state.md)
- [orchestrator.md](docs/arch/services/orchestrator.md)
- [executor-controller.md](docs/arch/services/executor-controller.md)
- [k8s-controller.md](docs/arch/services/k8s-controller.md)
- [release-controller.md](docs/arch/services/release-controller.md)
- [remediation.md](docs/arch/services/remediation.md)
- [remediation-agent.md](docs/arch/services/remediation-agent.md)
- [agent-runner.md](docs/arch/services/agent-runner.md)
- [manifest-controller.md](docs/arch/services/manifest-controller.md)
- [ui-service.md](docs/arch/services/ui-service.md)
- [cli.md](docs/arch/services/cli.md)

The list above is every long-running service in the system — each owns a
process, and typically a datastore and/or a gRPC/HTTP surface. Everything
below is a **Job image**: a container `executor-controller` runs to
completion as a one-shot Kubernetes `Job` and then discards. Job images have
no owned datastore, no gRPC/HTTP surface, and a small, fixed set of behaviors,
so they change far less often than the services above:

- [dbt/](dbt/README.md) — runs one dbt model/test/build per Job invocation
- [validation-runner/](validation-runner/README.md) — runs one blue/green validation op per Job invocation
- [validation-contract/](validation-contract/README.md) — the shared interface implemented by validation-runner and every engine adapter package (no runtime of its own)
