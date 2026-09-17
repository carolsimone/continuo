<p align="center">
  <img src="docs/logo/continuo-social-preview.png" alt="Continuo — control plane for data pipelines with agentic remediation" width="100%">
</p>

# Continuo

[![CI](https://github.com/carolsimone/continuo/actions/workflows/ci.yml/badge.svg)](https://github.com/carolsimone/continuo/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Release](https://img.shields.io/github/v/release/carolsimone/continuo?label=release)](https://github.com/carolsimone/continuo/releases)

Continuo is a control plane for data pipelines with built-in agentic
remediation. It orchestrates independent data projects — dbt (data build
tool) models, Python scripts, and CSV (comma-separated values) loads alike —
without requiring them to know about each other, automatically stitching them
into one dependency graph, and it heals broken pipelines by having an LLM
(Large Language Model) propose a fix before anything reaches production.

## 🎯 Use it when

- Several teams own separate dbt or Python projects that read each other's
  tables, and you want a change validated across those boundaries before it
  reaches production.
- You want an agent to propose a fix for a breaking change *before* it is
  deployed, not after the nightly run has already failed.
- You want to ask the platform what is going on in plain language: an LLM
  chat is built into the UI, and it can inspect the platform and, with your
  confirmation, act on it.

## ✅ What it delivers

- **No hand-written DAGs.** Dependencies are inferred from your SQL and
  Python contracts. The DAG is a by-product of your code.
- **One graph across independent projects.** dbt and Python projects are
  stitched together without declaring each other, including edges that cross
  project boundaries in both directions (dbt → Python → dbt).
- **Blue/green validation for data.** Every release runs in a temporary
  schema clone and is checked against its full downstream lineage. Only a
  passing release becomes the production image.
- **Production never runs a broken graph.** A rejected release leaves
  production on the last version that passed; schedules keep producing
  correct data.
- **Breakage surfaces on the person who made the change**, before it reaches
  production, even when the damage is in another team's model.
- **Agentic remediation.** A rejected release is classified, and a fixable one
  gets an LLM-proposed diff — and, with a GitHub App, a pull request — for a
  human to review and merge.
- **LLM chat in the UI.** Ask about releases, runs, and nodes; read-only
  answers come back immediately, and anything that changes state asks for
  your confirmation first.
- **Fully event-driven.** No polling, horizontally scalable, and the same
  events are yours to build on — alerting, dead-letter queues, custom tooling.
- **Two-step onboarding.** Publish an image and POST to `/releases`. Continuo
  compiles the project and derives the rest.
- **Also:** dbt, Python scripts, and contract-only CSV loads on one contract;
  bring your own dbt image; schedules in one YAML file; each node runs as its
  own Kubernetes Job in dependency order; circular dependencies fail at CD;
  runs on your Kubernetes via Helm, so data stays inside your perimeter.

What is not there yet — remediation for failed production runs, streaming and
batch under one control plane, performance regression detection — is in the
[roadmap](docs/roadmap.md).

## ⚙️ How it works

A DAG (Directed Acyclic Graph) is just the shape of your dependencies, and
those are already written down in your SQL. Continuo reads the schema and
table names each model reads and writes and stitches every project into one
graph — dbt models and Python nodes ordered together — so no project has to
know about any other. Onboarding is two steps at CD (Continuous Deployment)
time: publish an image and POST to `/releases`. Continuo compiles the project
itself and derives the rest. (A Python service uploads its contract to object
storage first — one extra `aws s3 cp` in your CD.)

Every release is then validated the way blue/green validates a software
release: stand the new version up next to the current one, prove it is
healthy, then cut over. Data does not move the way traffic does, so Continuo
runs the new DAG in a second, temporary schema (a schema copy, not a data
copy) and checks that the full downstream lineage still works. Only then does
the new image become the production image. If validation fails, production
keeps the last version that passed while a remediation agent proposes a fix
for a human to review.

## 🚦 Status

Beta. Three node types are first-class on one integration contract — **dbt
models**, **Python scripts**, and **contract-only CSV loads** — living in the
same graph, released and validated the same way.

Those nodes run and validate against your data warehouse, and the supported
warehouse engines today are **Postgres** and **Trino**. The engine you point
Continuo at must be one of these; it refuses to start on any other. (That
warehouse is separate from Continuo's own control-plane datastores, covered in
[Deploying it](#-deploying-it).)

## 🚀 Deploying it

Run the full system on your own Kubernetes cluster with the published Helm
chart. Bring your own Postgres and Redis, or let the chart bundle every
datastore alongside Continuo. Install modes, values, and hardening are in
[deploy/README.md](deploy/README.md).

## 🧪 Try it locally

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
  --version 0.6.2 -n continuo --create-namespace

# 3. Wait for everything to come up
kubectl -n continuo get pods -w

# 4. Port-forward the UI and the identity provider
kubectl -n continuo port-forward svc/ui 8090:8090 &
kubectl -n continuo port-forward svc/continuo-dex 5556:5556 &

# 5. One-time: let your browser resolve the in-cluster login issuer.
#    sudo prompts on the terminal, so run this in a real terminal window
#    (not an IDE/agent shell). No sudo? See deploy/continuo/README.md.
echo "127.0.0.1 continuo-dex" | sudo tee -a /etc/hosts

# 6. Open it
open http://localhost:8090
```

Log in with the demo account: `admin@example.com` / `password`.

This installs everything bundled — Postgres, Redis, Neo4j, MinIO, and Dex
(the identity provider) — for evaluation only. It's not meant to hold real
data: no backup, no high availability, one static login. For production
installs bringing your own datastores, see [deploy/README.md](deploy/README.md).

### 💡 Now put real projects on it

The install above gets you an *empty* Continuo. Everything Continuo actually
does — inferring one dependency graph across independent projects,
validating a change against its full downstream lineage before promoting it,
refusing a release that would break another team's model — only becomes visible
once there are real projects on it.

**[Try it locally, with real data projects](docs/try-it-locally.md)** walks
through that end to end, on the same local cluster: build four example
projects — three dbt, one Python — into images, release them, watch Continuo
discover dependencies that cross project boundaries and that no project
declares, run the graph from the UI, add a brand-new model and watch
validation prove it against production before promoting it, then break the
graph on purpose to watch validation reject the change while production keeps
serving the last version that worked — and, if you bring an LLM key, let the
agent propose the fix.
