# Try it locally, with real dbt projects

The install in the [README](../README.md#try-it-locally) gets Continuo running
on your laptop in about ten minutes, but it gets you an *empty* Continuo: a
control plane with no pipelines in it. Everything Continuo does — stitching
independent dbt projects into one dependency graph, validating a change against
the whole downstream lineage before it reaches production, healing a broken
model — only becomes visible once there are real dbt projects on it.

This guide puts three of them there.

By the end you will have built four independent projects into container images —
three dbt, one python — released them into a local Continuo one at a time, watched the platform
discover a dependency that crosses project boundaries and that none of the
projects declares, run the resulting graph from the web UI, and then broken it
on purpose to watch validation refuse the change while production keeps serving
the last version that worked.

Everything runs on your machine. Nothing is deployed anywhere, and no step
needs a cloud account.

---

## 1. What you need before you start

**A container runtime and a few CLI tools.**

| Tool | Why | Install |
|---|---|---|
| Docker Desktop, or [colima](https://github.com/abiosoft/colima) | Runs the cluster and builds the dbt images | `brew install colima && colima start` |
| [kind](https://kind.sigs.k8s.io/) | The local Kubernetes cluster | `brew install kind` |
| [kubectl](https://kubernetes.io/docs/tasks/tools/) | Talking to that cluster | `brew install kubectl` |
| [Helm](https://helm.sh/) 3.14+ | Installing Continuo | `brew install helm` |
| `git`, `curl`, `jq` | Cloning, calling the release API, reading its answers | `brew install jq` |

**Room to run it.** Continuo brings its own PostgreSQL, Redis, Neo4j, MinIO and
identity provider in this mode, plus ten of its own services, and then runs your
dbt models as Kubernetes Jobs alongside all of that. Give the container runtime
at least **4 CPUs, 8 GiB of memory and 20 GB of free disk**, and close anything
else large that is running on it. On colima that is:

```bash
colima start --cpu 4 --memory 8 --disk 60
```

**A GitHub account.** You will fork the example dbt projects so that the code
you release is yours — which matters from chapter 8 onward, where Continuo reads
your source to explain (and then propose a fix for) a failure.

**Credentials: none, until chapter 9.** Chapters 2 through 8 need no API keys
and no secrets of any kind. The last chapter, where an LLM proposes a fix for the
model you broke, is the exception:

| For chapter 9 only | What it is |
|---|---|
| An LLM API key | Anthropic (the default) or OpenAI |
| A GitHub personal access token | Read-only, fine-grained, `Contents: Read` on your fork — the agent reads the failing model's source through it |
| *(optional)* A GitHub App | Only if you want the UI's "Create PR" button to actually open the pull request rather than just show you the proposed diff |

If you do not want to set those up, stop after chapter 8. It is a complete
story on its own.

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
  --version 0.4.0 -n continuo --create-namespace

# Wait for everything to come up (5-10 minutes on a first install)
kubectl -n continuo get pods -w
```

That single `helm install` brings up PostgreSQL, Redis, Neo4j, MinIO, an
identity provider, and Continuo's twelve services. It is a quickstart layout meant
for evaluation — one static login, no backups, no high availability. Production
installs bring their own datastores; see
[deploy/README.md](../deploy/README.md).

While you wait, expect some noise: services that depend on a datastore fail
fast and get restarted until that datastore answers, so a few pods pass through
`Error` or `CrashLoopBackOff` on the way up. That is the intended behaviour, not
a broken install. Wait until every pod is `Running` or `Completed` before moving
on.

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
your browser and `ui-service` agree on one issuer hostname. `ui-service` reaches
the identity provider over in-cluster DNS at `continuo-dex`; your browser cannot
resolve that name at all. One loopback line bridges the two. If you cannot use
`sudo`, [the chart's README](../deploy/continuo/README.md) shows how to do it
with a browser resolver rule instead.

You are now looking at an empty Continuo. Everything that follows fills it.

---

## 3. Fork the example projects and read them

```bash
# Fork https://github.com/carolsimone/continuo-dbt-demo on GitHub first,
# then clone your fork:
git clone https://github.com/<your-username>/continuo-dbt-demo.git
cd continuo-dbt-demo
```

Fork rather than clone the original, because from chapter 8 onward the code you
release has to be code you can change and push.

The repository holds seven services under `services/`. Three of them —
`service-1`, `service-2`, `service-3` — are test scaffolding lifted from
Continuo's own end-to-end suite, full of deliberate failure nodes. Ignore those.

Four matter here. `core`, `finance`, and `marketing` are ordinary,
self-contained dbt projects: each with its own `dbt_project.yml`,
`profiles.yml`, models, seeds, and `Dockerfile`. `service-py` is different — it
is a **python-node service**, declaring `contracts/*.yml` and `scripts/*.py`
instead of dbt models. It onboards through the same `POST /releases` call but a
different artifact, which chapter 6 covers.

You need all four. A dbt model in `core` reads a table `service-py` produces, so
releasing only the dbt services leaves the graph with a node that cannot build.

### The dependency that makes this interesting

Open `services/finance/models/fx_transactions_eur.sql`. It converts foreign
currency transactions into euros:

```sql
FROM analytics.seed_fx_transactions t
LEFT JOIN analytics.seed_fx_rates_eur r
```

`seed_fx_rates_eur` is finance's own seed. **`seed_fx_transactions` is not** —
it belongs to `core`.

Now open `services/core/models/daily_transactions.sql`:

```sql
FROM {{ ref('seed_card_transactions') }}

UNION ALL

SELECT
    transaction_id,
    user_id,
    amount_eur,
    ...
FROM analytics.fx_transactions_eur
```

`seed_card_transactions` is core's own seed, referenced with `ref()`.
**`fx_transactions_eur` is finance's model**, referenced by its raw
schema-qualified name.

So the real dependency chain runs:

```
core.seed_fx_transactions  →  finance.fx_transactions_eur  →  core.daily_transactions
```

It crosses the project boundary twice. No single `dbt run` can order it,
because dbt's `ref()` only resolves within one project — and neither project
declares the relationship anywhere. Continuo infers the whole chain from the
SQL itself, which is the entire point of chapter 5.

### The four rules a project must follow

That file also shows the one rule people get wrong. Both reference styles appear
in it, deliberately:

1. **Within a project, use `{{ ref('name') }}`.** dbt resolves it and orders the
   build.
2. **Across projects, use the raw schema-qualified name** — `FROM
   analytics.fx_transactions_eur`, never `ref()`. A `ref()` to another project's
   model fails at `dbt compile` with `depends on a node named '…' which was not
   found`. Continuo sequences cross-project builds itself.
3. **Every node needs `meta.owner`** (set once in `dbt_project.yml`). Nodes
   without it are skipped.
4. **Every model needs a tag naming its schedule** — `{{ config(tags=['daily'])
   }}`. Continuo reads the first tag as the schedule the node belongs to; an
   untagged model is skipped. Seeds are exempt and default to build-on-release.

The full contract, including the `generate_schema_name` macro each project
carries, is in
[deploy/dbt-image-contract.md](../deploy/dbt-image-contract.md).

---

## 4. Build the images and load them into the cluster

Continuo runs your project by running *your image* as a Kubernetes Job, so each
service needs to be built and made visible to the cluster. That is true of the
python service too — it ships as an image exactly like the dbt ones. No registry
is involved:

```bash
for svc in core finance marketing service-py; do
  docker build -t "${svc}:v1" "services/${svc}"
  kind load docker-image "${svc}:v1" --name continuo
done
```

Confirm the node can see all four:

```bash
docker exec continuo-control-plane crictl images | grep -E "core|finance|marketing|service-py"
```

**Why a bare `core:v1` works.** The chart value `global.teamImagePrefix` is empty
by default, which tells Continuo's executor to resolve dbt job images as an
unprefixed `<service>:<image_tag>`, and it launches them with
`imagePullPolicy: IfNotPresent`. So an image side-loaded onto the node is found
and used, and nothing is ever pulled from a registry. Set `teamImagePrefix` to
your Docker Hub or registry namespace when you move to a real cluster, and the
same mechanism resolves `yourteam/core:v1` instead.

The tag `v1` is arbitrary — it just has to match what you send in the next
chapter. Real CD systems use the commit SHA.

---

## 5. Release one: bootstrap

Continuo's release API is an internal ClusterIP service. Port-forward it:

```bash
kubectl -n continuo port-forward svc/release-controller 8088:8088 &
```

Ask production what it is currently running:

```bash
curl -s http://localhost:8088/current-prod | jq
```

```json
{
  "current_prod_release_id": "",
  "node_count": 0,
  "updated_at": "0001-01-01T00:00:00Z"
}
```

An empty `current_prod_release_id` means production has never been seeded, and
this first release must **bootstrap**: promote without validation. That is not a
shortcut — against an empty production, every cross-service dependency looks
like a new one, so normal validation would reject everything. Real CD detects
this the same way, by reading this endpoint.

```bash
curl -s -X POST http://localhost:8088/releases \
  -H 'content-type: application/json' \
  -d '{
    "release_id": "rel-core-v1",
    "service": "core",
    "image_tag": "v1",
    "bootstrap": true,
    "repo": "<your-username>/continuo-dbt-demo",
    "commit_sha": "'"$(git rev-parse HEAD)"'"
  }' | jq
```

```json
{"release_id": "rel-core-v1", "status": "received"}
```

Note what you did *not* send: no manifest, no list of models, no DAG, no S3
upload. One service, one image tag. Continuo derives the rest.

Watch it:

```bash
curl -s http://localhost:8088/releases/rel-core-v1 | jq '{status, transitions}'
```

Within about half a minute it reaches `promoted`, having walked:

```
received → compiling → parsing → validating → promoted
```

### What just happened

Those statuses are the whole pipeline, and each one is a different service:

**`compiling`** — `release-controller` published a request that
`executor-controller` turned into a Kubernetes Job running **your `core:v1`
image**. That Job ran `dbt compile` against your project and uploaded the
resulting `manifest.json` to the bundled MinIO. This is why the release body
carries no manifest: Continuo compiles your project itself, using the same image
it will later run your models with, so what gets analysed is exactly what will
execute.

**`parsing`** — `manifest-controller` read that manifest and worked out the
shape of your project: which nodes exist, what each one reads, and what its
content hashes to. Cross-project dependencies are resolved here by parsing the
compiled SQL with sqlglot — this is the step that discovers `FROM
analytics.fx_transactions_eur` is an edge in the graph.

**`validating` → `promoted`** — bootstrap skips the actual validation, so these
are the same instant. Chapter 6 is where this gets interesting.

Then, on promotion, Continuo materialises the release's seeds into production.

Check the result in the UI: `core` now has four nodes — three seeds and
`daily_transactions` — with one edge between `daily_transactions` and
`seed_card_transactions`.

That edge is the `{{ ref() }}` one, resolved inside a single project. The
cross-project half of the chain is not there yet, because `finance` doesn't
exist yet. Nothing is broken; the graph simply reflects what has been released.

---

## 6. The remaining releases: validated

**Order matters from here.** A release is validated against the topology that
exists when it runs, so a service must be released *after* whatever it reads.
`finance` reads a table `marketing` produces, so marketing goes first. Release
finance before it and validation correctly refuses the release with
`relation "analytics.marketing_cost_per_user" does not exist` — Continuo doing
its job, not a fault.

The order is **marketing → finance → service-py**.

Releases also run a **FIFO queue**: one is active at a time, and each terminal
outcome advances the next. Post them one at a time and wait for each to reach
`promoted`; a release that never finishes blocks everything behind it.

Start with `marketing`, this time with `bootstrap: false`:

```bash
curl -s -X POST http://localhost:8088/releases \
  -H 'content-type: application/json' \
  -d '{
    "release_id": "rel-marketing-v1",
    "service": "marketing",
    "image_tag": "v1",
    "bootstrap": false,
    "repo": "<your-username>/continuo-dbt-demo",
    "commit_sha": "'"$(git rev-parse HEAD)"'"
  }' | jq
```

This one takes longer and passes through an extra stage:

```
received → compiling → seed_building → validating → promoted
```

When it lands, look at what was validated:

```bash
curl -s http://localhost:8088/releases/rel-finance-v1 | jq '.validation_node_ids'
```

```json
[
  "analytics.daily_transactions",
  "analytics.fx_transactions_eur",
  "analytics.operation_cost_per_user_view",
  "analytics.operational_cost_per_user",
  "analytics.operational_costs_monthly",
  "analytics.seed_card_transactions",
  "analytics.seed_fx_transactions",
  "analytics.seed_users"
]
```

**Read that list again.** You released `finance`. Continuo validated eight nodes,
and `analytics.daily_transactions` is not one of finance's — it belongs to
`core`. It was pulled in because it reads `analytics.fx_transactions_eur`, which
this release changes. Core's seeds are in the list too, as the upstream the
change depends on.

Nobody declared that relationship. Both projects were written independently, and
neither mentions the other in its dbt configuration. Continuo found it in the
SQL and worked out that changing finance means re-proving core.

That is the difference between "the release passed its own tests" and "the
release does not break anything downstream of it, in any project".

### How it proves that

`seed_building` and `validating` are the blue/green mechanism. Continuo creates
a temporary candidate schema, builds the release's seeds into it, rewrites every
in-scope model's compiled SQL to read from that schema instead of production,
and runs it there. The lineage is checked against the result. Production is
untouched throughout, and the candidate schema is torn down afterwards. No data
is copied — only schema-level structure.

Only if all of that passes does the new image become the production image for
that service.

Now release `finance` the same way, with `release_id` `rel-finance-v1` and
`service: finance`. Once it promotes, all three dbt projects are live.

### The fourth service is not dbt

`service-py` is a python-node service, and its onboarding differs in one
important way: **you upload its artifact yourself.** A dbt service's manifest is
produced by Continuo's own compile leg — that is what the `compiling` stage
above was doing. A python service has no compile leg; its contract is built in
your CD and uploaded to object storage *before* the release is posted.

This is exactly what the demo repo's CI does, and doing it by hand here is the
point: it is the same sequence your own CD will run.

First build the contract from the service's `contracts/` directory, using the
runtime CLI the release gate uses:

```bash
uv tool install continuo-python-runtime==0.4.0

continuo-runtime validate services/service-py/contracts --dialect postgres

continuo-runtime merge services/service-py/contracts \
  --service service-py \
  --repo-root services/service-py \
  --dialect postgres \
  --out /tmp/contract.yaml
```

Then upload it to the canonical key, `<service>/<release_id>/contract.yaml`.
The local install's object storage is the bundled MinIO, so port-forward it and
point the AWS CLI at it — the same `aws s3 cp` your CD runs, aimed somewhere
else:

```bash
kubectl -n continuo port-forward svc/continuo-minio 9000:9000 &

export AWS_ACCESS_KEY_ID=$(kubectl -n continuo get secret continuo-minio \
  -o jsonpath='{.data.access-key-id}' | base64 -d)
export AWS_SECRET_ACCESS_KEY=$(kubectl -n continuo get secret continuo-minio \
  -o jsonpath='{.data.secret-access-key}' | base64 -d)
export AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url http://localhost:9000 \
  s3 cp /tmp/contract.yaml s3://continuo/service-py/rel-py-v1/contract.yaml
```

Only now post the release, with one extra field — `"kind": "python"` — telling
Continuo to skip the compile leg and read the contract you just uploaded:

```bash
curl -s -X POST http://localhost:8088/releases \
  -H 'content-type: application/json' \
  -d '{
    "release_id": "rel-py-v1",
    "service": "service-py",
    "image_tag": "v1",
    "bootstrap": false,
    "kind": "python",
    "repo": "<your-username>/continuo-dbt-demo",
    "commit_sha": "'"$(git rev-parse HEAD)"'"
  }' | jq
```

If you post this before the upload, the release parks in `parsing` and stays
there: the parse is retrying against a 404 for an object that does not exist.

### The graph you have built

Four services, stitched into one graph, from four API calls that each named a
single service — and the chain crosses both project *and* runtime boundaries:

```
core.seed_fx_transactions   →  finance.fx_transactions_eur   (dbt → dbt)
core.daily_transactions     →  service-py.py_daily_kpis      (dbt → python)
service-py.py_daily_kpis    →  core.dbt_daily_kpis           (python → dbt)
marketing.marketing_cost_per_user → finance.ltv_per_user     (dbt → dbt)
```

The middle two are the interesting pair: a Python job reads a table dbt built,
and a dbt model reads the table that Python job wrote. Neither project declares
the other. Continuo derived the ordering from the SQL and the contract.

---

## 7. Run it

Open the UI, pick the `daily` schedule, and press **▶ Trigger run**.

You will see nodes move through the graph as Continuo dispatches each one as its
own Kubernetes Job, in dependency order, across all four services — including the
hop through the python one. Watch it
from the cluster side too if you like:

```bash
kubectl -n continuo get jobs -w
```

Seeds build first, then `finance.fx_transactions_eur` (which needs core's
seeds), then `core.daily_transactions` (which needs finance's model). The
ordering crosses project boundaries in both directions — which is precisely the
ordering no single `dbt run` could have produced.

Click any node to see its dbt log.

When the run finishes, every node should be green. Now look at the actual
result:

```bash
PGPW=$(kubectl -n continuo get secret continuo-postgresql -o jsonpath='{.data.password}' | base64 -d)
kubectl -n continuo exec continuo-postgresql-0 -- env PGPASSWORD="$PGPW" \
  psql -U continuo -d continuo_dbt -c \
  "select source, count(*) as rows, round(sum(amount_eur)) as total_eur
     from analytics.daily_transactions group by source order by source"
```

```
 source | rows | total_eur
--------+------+-----------
 card   |  100 |     70318
 fx     |  100 |    744400
```

One table, two halves. The `card` rows came from core's own seed through a
`{{ ref() }}`. The `fx` rows arrived via finance's `fx_transactions_eur`, a model
in a different dbt project, built by a different image, released separately —
reached by nothing more than `FROM analytics.fx_transactions_eur`.

---

## 8. Break it on purpose

Everything so far has worked. The point of Continuo is what happens when
something doesn't.

`core.daily_transactions` selects `amount_eur` from finance's table. Remove that
column from finance:

```bash
# services/finance/models/fx_transactions_eur.sql
# replace this line:
#     ROUND((t.amount * r.rate_to_eur)::numeric, 2) AS amount_eur
# with:
      r.rate_to_eur AS unused_placeholder
```

This is an ordinary-looking change. It is valid SQL, the model still compiles,
and finance's own tests would not catch it — the damage is entirely in another
team's project.

Build and release it:

```bash
docker build -t finance:v2 services/finance
kind load docker-image finance:v2 --name continuo

curl -s -X POST http://localhost:8088/releases \
  -H 'content-type: application/json' \
  -d '{"release_id":"rel-finance-v2","service":"finance","image_tag":"v2",
       "bootstrap":false,"repo":"<your-username>/continuo-dbt-demo",
       "commit_sha":"'"$(git rev-parse HEAD)"'"}' | jq
```

This time it ends differently:

```bash
curl -s http://localhost:8088/releases/rel-finance-v2 | jq '{status, reject_reason, failing_nodes}'
```

```json
{
  "status": "rejected",
  "reject_reason": "validation_failed",
  "failing_nodes": ["analytics.daily_transactions"]
}
```

The release was rejected, and the node it names belongs to **`core`** — the
project you did not touch.

Now check production:

```bash
curl -s http://localhost:8088/current-prod | jq '.current_prod_release_id'
```

Still `rel-finance-v1`. Production never saw `finance:v2`. The `daily` schedule
will keep running the last version that passed, indefinitely, and a scheduled
run tonight will produce correct data.

This is the part that is hard to get any other way. In a conventional setup this
change merges, deploys, and breaks `core` the next time it runs — and the person
who gets paged is on the core team, looking at a model they did not change.
Here, the failure surfaced against the person who made the change, before it
reached production, in another team's model they had never heard of.

---

## 9. Let the agent propose a fix

*This chapter needs the credentials from chapter 1. Everything above did not.*

A rejected release tells you something broke. Continuo can also try to fix it.

`remediation` classifies the rejection, and for a fixable one `remediation-agent`
reads the failing model's source from GitHub, asks an LLM for a fix, and surfaces
the proposal for a human to approve. It never writes to your repository on its
own — the output is a diff you review, and a pull request you choose to open.

Because it reads the source from GitHub, it needs the broken code to exist
there. Commit and push your change to your fork, and use that commit:

```bash
git add services/finance && git commit -m "break amount_eur" && git push
```

Then reinstall with the credentials wired in:

```bash
helm upgrade continuo oci://ghcr.io/carolsimone/charts/continuo \
  --version 0.4.0 -n continuo \
  --set llm.apiKey='<your-api-key>' \
  --set github.token='<your-read-only-PAT>'
```

`llm.provider` defaults to `anthropic` and `llm.model` to `claude-haiku-4-5`. For
OpenAI, add `--set llm.provider=openai --set llm.model=<model>`.

Re-release the broken finance with a new `release_id` and the pushed
`commit_sha`. When it is rejected this time, the proposed fix appears in the UI
against the failed release.

Opening the pull request from the UI additionally needs a GitHub App
(`github.appId`, `github.installationId`, `github.appPrivateKey`) — a read-only
token is enough to *see* the proposal, but not to create a PR with it.

---

## 10. Clean up

```bash
kind delete cluster --name continuo
```

That removes everything: the cluster, all Continuo services, both databases, and
every image you side-loaded. The only thing left on your machine is the images in
your local Docker daemon, which `docker image rm core:v1 finance:v1 finance:v2
marketing:v1` clears.

---

## Troubleshooting

**A pod is in `CrashLoopBackOff` during install.** Expected while the datastores
come up. Services that need Postgres, Redis or Neo4j fail fast and are restarted
until their dependency answers; `orchestrator` in particular usually restarts two
or three times waiting for Neo4j's DNS. If a pod is still crash-looping after all
the datastore pods are `Running`, check its logs.

**`ErrImagePull` on a dbt Job.** The image tag in your release body does not
match anything loaded onto the node. Check with:

```bash
docker exec continuo-control-plane crictl images | grep <service>
```

Remember that `kind load` copies the image at that moment — rebuilding an image
does not update what the node has, so rebuild *and* reload.

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

**A release sits in `received` and never moves.** Releases run a FIFO queue —
one release is active at a time, and each terminal outcome advances the next. A
release stuck earlier in the queue therefore blocks every release behind it.
Check the one in front of it:

```bash
curl -s http://localhost:8088/releases | jq '.releases[] | {release_id, status}'
```

**A python release sits in `parsing` and never moves.** Its `contract.yaml` is
not where Continuo expects it. Unlike a dbt service — whose manifest Continuo
compiles itself — a python service's contract is uploaded by the *caller*, and
a missing object leaves the parse retrying against a 404. Confirm the object
exists at the canonical key:

```bash
aws --endpoint-url http://localhost:9000 s3 ls \
  s3://continuo/<service>/<release_id>/contract.yaml
```

**A release sits in `compiling` and then rejects with `compile_failed`.** Your
dbt project does not compile. The most common cause is a `{{ ref() }}` pointing
at a model in a *different* service — see the cross-project rule in chapter 3.

**Everything is slow, or pods are being OOM-killed.** The bundled install asks
for roughly 4 GiB in resource requests before your dbt Jobs even start. Give the
container runtime more memory (chapter 1) and close other large workloads.

---

## Where to go next

You now have the whole model in your hands: a project is onboarded by publishing
an image and calling `POST /releases`, and everything after that — the graph, the
ordering, the validation, the rejection — is derived.

Running it for real changes only two things. Your images come from a registry
rather than `kind load`, which is the `global.teamImagePrefix` value. And your
warehouse is your warehouse rather than the bundled Postgres, which is the
`validation.*` block. Both are covered in
[deploy/README.md](../deploy/README.md), and the full image contract your
projects must satisfy is in
[deploy/dbt-image-contract.md](../deploy/dbt-image-contract.md).
