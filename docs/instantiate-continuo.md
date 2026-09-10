# Instantiate the continuo platform

This guide gets continuo itself running on your laptop — an empty control plane for your data pipelines,
in about ten minutes. The
[Run dbt and Python projects in continuo](run-projects-in-continuo.md) guide puts
the real projects on it.

## What continuo does

What continuo is and why it exists is on the
[continuo homepage](https://continuo-data.com). This guide just gets it running.

> **Deploying to your own Kubernetes cluster?** This guide is the local
> quickstart — a single-node cluster with continuo's own bundled PostgreSQL,
> Redis, Neo4j and MinIO, meant for evaluation. To install continuo into a real
> cluster, against your own datastores and with HA and backups, follow
> [deploy/README.md](../deploy/README.md) instead.

---

## 1. What you need before you start

**A container runtime and a few CLI tools.** The install commands below are
for macOS with [Homebrew](https://brew.sh/). On Linux, install the same tools
from the linked pages; use Docker Engine in place of Docker Desktop or colima,
and skip the colima sizing command further down — the memory and disk figures
then apply to the host itself.

| Tool | Why | Install (macOS, Homebrew) |
|---|---|---|
| Docker Desktop, or [colima](https://github.com/abiosoft/colima) | Runs the cluster and builds the service images | `brew install colima && colima start` |
| [kind](https://kind.sigs.k8s.io/) | The local Kubernetes cluster | `brew install kind` |
| [kubectl](https://kubernetes.io/docs/tasks/tools/) | Talking to that cluster | `brew install kubectl` |
| [Helm](https://helm.sh/) 3.14+ | Installing continuo | `brew install helm` |
| `git`, `curl`, `jq` | Cloning, calling the release API, reading its answers | `brew install jq` |
| [AWS CLI](https://docs.aws.amazon.com/cli/) | Uploading the python service's artifacts to the bundled MinIO (chapter 4 of the [Run dbt and Python projects in continuo](run-projects-in-continuo.md) guide) | `brew install awscli` |

**Room to run it.** continuo brings its own PostgreSQL, Redis, Neo4j, MinIO and
identity provider in this mode, plus ten of its own services, and then runs your
nodes as Kubernetes Jobs alongside all of that. Give the container runtime
**4 CPUs, 12 GiB of memory and a 60 GB disk** — 8 GiB of memory is the bare
floor, and only if nothing else large is running. Close other heavy containers,
and a second local cluster especially, before you start. On colima that is:

```bash
colima start --cpu 4 --memory 12 --disk 60
```

A memory-starved runtime does not fail with a clear message: the cluster's API
server stops answering — `kubectl` hangs or returns `net/http: TLS handshake
timeout` — and the pods sit un-ready for a long time. If you hit that, give the
runtime more memory (or stop other containers), then reinstall.

**A GitHub account.** You will fork the example projects so that the code
you release is yours — which matters in chapter 8 of the
[Run dbt and Python projects in continuo](run-projects-in-continuo.md) guide,
where continuo reads your source to explain (and then propose a fix for) a
failure.

**Credentials: none to install.** The install itself needs no secrets, and so
does every step of the walkthrough except the LLM-backed extras below. An LLM API
key unlocks two optional things: the in-UI **assistant** (section 3) and
**chapter 8 of the [Run dbt and Python projects in continuo](run-projects-in-continuo.md)
guide**, where an LLM proposes a fix for a model you broke. That chapter 8
additionally needs GitHub access.

| For the LLM extras | What it is |
|---|---|
| An LLM API key | Anthropic (the default) or OpenAI — powers the assistant (section 3) and chapter 8 |
| A GitHub personal access token | *Chapter 8 only.* Read-only, fine-grained, `Contents: Read` on your fork — the agent reads the failing model's source through it |
| *(optional)* A GitHub App | *Chapter 8 only.* Needed if you want the UI's "Create PR" button to actually open the pull request rather than just show you the proposed diff |

Where to get them: create an Anthropic key in the
[Anthropic Console](https://console.anthropic.com/settings/keys) (Settings → API
keys — see the [Anthropic API docs](https://docs.claude.com/en/api/overview) for
details), or an OpenAI key at the
[OpenAI platform](https://platform.openai.com/api-keys). The GitHub token and App
are created in your own GitHub account settings (chapter 8 of the
[Run dbt and Python projects in continuo](run-projects-in-continuo.md) guide
walks through the App).

Set none of these and you still get the whole platform and the whole walkthrough
bar the assistant and chapter 8 — a complete story without them.

**Time.** Budget ten to fifteen minutes end to end, nearly all of it waiting for
image pulls on the first install.

---

## 2. Install continuo

Everything here pulls pre-built, multi-architecture images — nothing is compiled
from source, and it works the same on Intel and Apple Silicon.

```bash
# A local Kubernetes cluster
kind create cluster --name continuo

# continuo itself, from the published chart
helm install continuo oci://ghcr.io/carolsimone/charts/continuo \
  --version 0.5.0 -n continuo --create-namespace

# Wait for everything to come up (5-10 minutes on a first install)
kubectl -n continuo get pods -w
```

That single `helm install` brings up PostgreSQL, Redis, Neo4j, MinIO, an
identity provider, and continuo's ten services. It is a quickstart layout meant
for evaluation — one static login, no backups, no high availability. Production
installs bring their own datastores; see
[deploy/README.md](../deploy/README.md).

While you wait, most service pods sit in `Init:0/1`: each one gates on an init
container that waits for the datastores to answer and the database migrations
to finish, so services start in dependency order rather than crash-looping.
The wait is image pulls plus that gate — on a first install expect several
quiet minutes with no restarts. Wait until every pod is `Running` or
`Completed` before moving on.

Then open it:

```bash
kubectl -n continuo port-forward svc/ui 8090:8090 &
kubectl -n continuo port-forward svc/continuo-dex 5556:5556 &
echo "127.0.0.1 continuo-dex" | sudo tee -a /etc/hosts

open http://localhost:8090
```

Log in with `admin@example.com` / `password`.

The `/etc/hosts` line exists because logging in uses OIDC, which requires that
your browser and the `ui` service agree on one issuer hostname. `ui` reaches
the identity provider over in-cluster DNS at `continuo-dex`; your browser cannot
resolve that name at all. One loopback line bridges the two. If you cannot use
`sudo`, [the chart's README](../deploy/continuo/README.md) shows how to do it
with a browser resolver rule instead.

You are now looking at an empty continuo. Everything that follows fills it.

---

## 3. Optional: enable the assistant

The panel on the right of the UI is continuo's assistant — ask it about your
platform in plain language (it has more to say once you load real projects with
the [Run dbt and Python projects in continuo](run-projects-in-continuo.md)
guide). It needs an LLM API key; until one is set the panel answers with a
provider error (`x-api-key header is required`).

Set the key and restart the chat service:

```bash
helm upgrade continuo oci://ghcr.io/carolsimone/charts/continuo \
  --version 0.5.0 -n continuo --reuse-values \
  --set llm.apiKey='<your-api-key>'

kubectl -n continuo rollout restart deploy/agent-chat
```

`llm.provider` defaults to `anthropic` and `llm.model` to `claude-haiku-4-5`; for
OpenAI add `--set llm.provider=openai --set llm.model=<model>`. Once the pod
restarts, the assistant answers live. This is the same `llm.apiKey` chapter 8 of
the [Run dbt and Python projects in continuo](run-projects-in-continuo.md) guide
uses, so setting it here covers both.

---

## Troubleshooting

**A pod is in `CrashLoopBackOff` during install.** Not expected: services gate
on init containers (`wait-for-migrations`, `wait-for-redis`) and start in
dependency order, so a healthy install comes up with zero restarts. A pod
stuck in `Init:0/1` is still waiting on its gate — look at the datastore pods
and the `db-init-migrate` job first. A pod that is actually crash-looping is a
real signal: read its logs.

**`sudo: a terminal is required to read the password`** on the `/etc/hosts`
line. `sudo` reads its password from the controlling terminal, so it cannot
prompt inside an IDE panel or an agent shell. Run it in a real terminal window,
or skip `/etc/hosts` entirely by pointing your browser's resolver at the
port-forward:

```bash
open -na "Google Chrome" --args \
  --host-resolver-rules="MAP continuo-dex 127.0.0.1" \
  --user-data-dir="$HOME/.continuo-chrome" \
  http://localhost:8090
```

**Everything is slow, or pods are being OOM-killed.** The bundled install asks
for roughly 4 GiB in resource requests before your own Jobs even start. Give the
container runtime more memory (chapter 1) and close other large workloads.

---

## Next

You now have an empty continuo running. Fill it with real data projects:
[Run dbt and Python projects in continuo](run-projects-in-continuo.md).
