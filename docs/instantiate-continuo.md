# Instantiate the Continuo platform

This guide gets Continuo itself running on your laptop — an empty control plane,
in about ten minutes. The next guide,
[Run dbt and Python projects in Continuo](run-projects-in-continuo.md), puts the
real projects on it.

---

## 1. What you need before you start

**A container runtime and a few CLI tools.**

| Tool | Why | Install |
|---|---|---|
| Docker Desktop, or [colima](https://github.com/abiosoft/colima) | Runs the cluster and builds the service images | `brew install colima && colima start` |
| [kind](https://kind.sigs.k8s.io/) | The local Kubernetes cluster | `brew install kind` |
| [kubectl](https://kubernetes.io/docs/tasks/tools/) | Talking to that cluster | `brew install kubectl` |
| [Helm](https://helm.sh/) 3.14+ | Installing Continuo | `brew install helm` |
| `git`, `curl`, `jq` | Cloning, calling the release API, reading its answers | `brew install jq` |
| [AWS CLI](https://docs.aws.amazon.com/cli/) | Uploading the python service's artifacts to the bundled MinIO (chapter 4 of the [next guide](run-projects-in-continuo.md)) | `brew install awscli` |

**Room to run it.** Continuo brings its own PostgreSQL, Redis, Neo4j, MinIO and
identity provider in this mode, plus ten of its own services, and then runs your
nodes as Kubernetes Jobs alongside all of that. Give the container runtime
**4 CPUs, 12 GiB of memory and 20 GB of free disk** — 8 GiB is the bare floor,
and only if nothing else large is running. Close other heavy containers, and a
second local cluster especially, before you start. On colima that is:

```bash
colima start --cpu 4 --memory 12 --disk 60
```

A memory-starved runtime does not fail with a clear message: the cluster's API
server stops answering — `kubectl` hangs or returns `net/http: TLS handshake
timeout` — and the pods sit un-ready for a long time. If you hit that, give the
runtime more memory (or stop other containers), then reinstall.

**A GitHub account.** You will fork the example projects so that the code
you release is yours — which matters in chapter 8 of the
[next guide](run-projects-in-continuo.md), where Continuo reads
your source to explain (and then propose a fix for) a failure.

**Credentials: none, until chapter 8 of the next guide.** Every other chapter
needs no API keys and no secrets of any kind. Chapter 8 of the next guide, where
an LLM proposes a fix for the model you broke, is the exception:

| For chapter 8 of the next guide only | What it is |
|---|---|
| An LLM API key | Anthropic (the default) or OpenAI |
| A GitHub personal access token | Read-only, fine-grained, `Contents: Read` on your fork — the agent reads the failing model's source through it |
| *(optional)* A GitHub App | Only if you want the UI's "Create PR" button to actually open the pull request rather than just show you the proposed diff |

If you do not want to set those up, skip chapter 8 of the next guide. The rest
is a complete story without it.

**Time.** Budget about an hour end to end, most of it waiting for image pulls and
for validation Jobs to run.

---

## 2. Install Continuo

Everything here pulls pre-built, multi-architecture images — nothing is compiled
from source, and it works the same on Intel and Apple Silicon.

```bash
# A local Kubernetes cluster
kind create cluster --name continuo

# Continuo itself, from the published chart
helm install continuo oci://ghcr.io/carolsimone/charts/continuo \
  --version 0.4.1 -n continuo --create-namespace

# Wait for everything to come up (5-10 minutes on a first install)
kubectl -n continuo get pods -w
```

That single `helm install` brings up PostgreSQL, Redis, Neo4j, MinIO, an
identity provider, and Continuo's ten services. It is a quickstart layout meant
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

# One-time. Run this in a real terminal window — sudo reads the password from
# the terminal, so it will not prompt inside an IDE panel or an agent shell.
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

You are now looking at an empty Continuo. Everything that follows fills it.

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

You now have an empty Continuo running. Fill it with real data projects:
[Run dbt and Python projects in Continuo](run-projects-in-continuo.md).
