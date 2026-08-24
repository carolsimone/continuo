# Try it locally, with real dbt projects

The install in the [README](../README.md#try-it-locally) gets Continuo running
on your laptop in about ten minutes, but it gets you an *empty* Continuo: a
control plane with no pipelines in it. Everything Continuo does — stitching
independent dbt projects into one dependency graph, validating a change against
the whole downstream lineage before it reaches production, healing a broken
model — only becomes visible once there are real dbt projects on it.

This guide puts four of them there.

By the end you will have built four independent projects into container images —
three dbt, one python — released them into a local Continuo, watched the platform
discover dependencies that cross project boundaries and that none of the
projects declares, run the resulting graph from the web UI, added a brand-new
model and watched validation prove it against production before promoting it,
and then broken the graph on purpose to watch validation refuse the change
while production keeps serving the last version that worked.

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
| [AWS CLI](https://docs.aws.amazon.com/cli/) | Uploading the python service's artifacts to the bundled MinIO (chapter 6) | `brew install awscli` |

**Room to run it.** Continuo brings its own PostgreSQL, Redis, Neo4j, MinIO and
identity provider in this mode, plus ten of its own services, and then runs your
dbt models as Kubernetes Jobs alongside all of that. Give the container runtime
at least **4 CPUs, 8 GiB of memory and 20 GB of free disk**, and close anything
else large that is running on it. On colima that is:

```bash
colima start --cpu 4 --memory 8 --disk 60
```

**A GitHub account.** You will fork the example dbt projects so that the code
you release is yours — which matters in chapter 10, where Continuo reads
your source to explain (and then propose a fix for) a failure.

**Credentials: none, until chapter 10.** Every other chapter needs no API keys
and no secrets of any kind. Chapter 10, where an LLM proposes a fix for the
model you broke, is the exception:

| For chapter 10 only | What it is |
|---|---|
| An LLM API key | Anthropic (the default) or OpenAI |
| A GitHub personal access token | Read-only, fine-grained, `Contents: Read` on your fork — the agent reads the failing model's source through it |
| *(optional)* A GitHub App | Only if you want the UI's "Create PR" button to actually open the pull request rather than just show you the proposed diff |

If you do not want to set those up, skip chapter 10. The rest is a complete
story without it.

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
your browser and the `ui` service agree on one issuer hostname. `ui` reaches
the identity provider over in-cluster DNS at `continuo-dex`; your browser cannot
resolve that name at all. One loopback line bridges the two. If you cannot use
`sudo`, [the chart's README](../deploy/continuo/README.md) shows how to do it
with a browser resolver rule instead.

You are now looking at an empty Continuo. Everything that follows fills it.

---

## 3. Fork the example projects and read them

```bash
# Fork https://github.com/carolsimone/continuo-demo on GitHub first,
# then clone your fork:
git clone https://github.com/<your-username>/continuo-demo.git
cd continuo-demo
```

Fork rather than clone the original, because the code you release has to be
code you can change — and, in chapter 10, push.

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
chapter. Real CD systems use the commit SHA. (The python service's release
body needs the *full* `service-py:v1` reference rather than the bare tag —
chapter 6 explains why.)

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
    "repo": "<your-username>/continuo-demo",
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

**`parsing`** — `topology-controller` read that manifest and worked out the
shape of your project: which nodes exist, what each one reads, and what its
content hashes to. Cross-project dependencies are resolved here by parsing the
compiled SQL with sqlglot — this is the step that discovers `FROM
analytics.fx_transactions_eur` is an edge in the graph.

**`validating` → `promoted`** — bootstrap skips the actual validation, so these
are the same instant. Chapter 8 is where validation happens for real.

Then, on promotion, Continuo materialises the release's seeds into production.

Check the result in the UI: `core` now has four nodes — three seeds and
`daily_transactions` — with one edge between `daily_transactions` and
`seed_card_transactions`.

That edge is the `{{ ref() }}` one, resolved inside a single project. The
cross-project half of the chain is not there yet, because `finance` doesn't
exist yet. Nothing is broken; the graph simply reflects what has been released.

---

## 6. The remaining releases: bootstrap them all

The obvious next step — release `marketing` normally and let it validate — does
not work yet, and the reason teaches you how validation actually operates. Two
rules collide on a first install:

- **Validation clones what a change reads from production.** To prove a release
  against real structure, Continuo builds a temporary candidate schema by
  cloning the release's unchanged upstream tables from production — so those
  tables must already physically exist.
- **Promotion materialises only seeds.** Models become real tables when a *run*
  executes them (chapter 7), not at promotion.

You have promoted `core`, but nothing has run: core's *models* do not exist as
tables yet. Validate any service that reads them and the clone step fails with
`relation "analytics.daily_transactions" does not exist` — Continuo refusing to
prove a change against tables that are not there. No release order escapes
this, because the demo's services read from each other in both directions
(core ↔ finance, core ↔ service-py): whichever service you validate first needs
another one's models materialised, and that needs a run.

So a cold install is seeded the way production was born: **every service's
first release is a bootstrap.** Promote all four without validation, run the
graph once so every model physically exists, and from then on validation has a
production to clone from. This chapter is the last time you will pass
`"bootstrap": true`; every release after it is validated for real.

Order among the remaining three does not matter. Every release re-parses the
*full* set of promoted manifests plus its own, so the edge between two services
appears as soon as both have been released — whichever release comes last
completes the DAG.

Releases run a **FIFO queue**: one is active at a time, and each terminal
outcome advances the next. Post them one at a time and wait for each to reach
`promoted`; a release that never finishes blocks everything behind it.

`marketing` first:

```bash
curl -s -X POST http://localhost:8088/releases \
  -H 'content-type: application/json' \
  -d '{
    "release_id": "rel-marketing-v1",
    "service": "marketing",
    "image_tag": "v1",
    "bootstrap": true,
    "repo": "<your-username>/continuo-demo",
    "commit_sha": "'"$(git rev-parse HEAD)"'"
  }' | jq
```

Like core's, it promotes in about half a minute. Release `finance` the same
way, with `release_id` `rel-finance-v1` and `service: finance`. Once it
promotes, all three dbt projects are live — and the UI now shows the
cross-project edges from chapter 3, discovered from the SQL alone.

### The fourth service is not dbt

`service-py` is a python-node service, and its onboarding differs in one
important way: **you upload its artifact yourself.** A dbt service's manifest is
produced by Continuo's own compile leg — that is what the `compiling` stage
above was doing. A python service has no compile leg; its contract is built in
your CD and uploaded to object storage *before* the release is posted.

This is exactly what the demo repo's CI does, and doing it by hand here is the
point: it is the same sequence your own CD will run.

**One edit first.** `service-py` declares two nodes. `py_daily_kpis` runs a
Python script against the warehouse. `demo_orders_csv` is a *python-csv* node:
no script at all — the runtime loads a CSV file straight from object storage
and writes it through as a table. A csv node's contract therefore names an
object in *an* object store, and as shipped it names one in the demo author's:

```yaml
# services/service-py/contracts/demo_orders_csv.yml
    reads:
      csv: s3://continuo-dev/static-files/demo/orders.csv
```

Your install has no `continuo-dev` bucket. Point it at your own — this is your
fork, and the file is meant to be yours:

```yaml
    reads:
      csv: s3://continuo/static-files/demo/orders.csv
```

The contract ships *inside* the image (`COPY contracts/` in the service's
Dockerfile), and the runtime reads that baked copy when the node runs — so
rebuild and reload the image you built in chapter 4:

```bash
docker build -t service-py:v1 services/service-py
kind load docker-image service-py:v1 --name continuo
```

Then put a file where the contract now points. Your install's object store is the bundled MinIO;
port-forward it and drive it with the AWS CLI:

```bash
kubectl -n continuo port-forward svc/continuo-minio 9000:9000 &

export AWS_ACCESS_KEY_ID=$(kubectl -n continuo get secret continuo-minio \
  -o jsonpath='{.data.access-key-id}' | base64 -d)
export AWS_SECRET_ACCESS_KEY=$(kubectl -n continuo get secret continuo-minio \
  -o jsonpath='{.data.secret-access-key}' | base64 -d)
export AWS_DEFAULT_REGION=us-east-1

cat > /tmp/orders.csv <<'EOF'
order_id,customer,amount
1001,acme,120.50
1002,globex,80.00
1003,initech,42.25
EOF

aws --endpoint-url http://localhost:9000 \
  s3 cp /tmp/orders.csv s3://continuo/static-files/demo/orders.csv
```

Now build the contract from the service's `contracts/` directory, using the
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

Then upload it to the canonical key, `<service>/<release_id>/contract.yaml` —
the same `aws s3 cp` your CD runs, aimed at the MinIO you forwarded a moment
ago:

```bash
aws --endpoint-url http://localhost:9000 \
  s3 cp /tmp/contract.yaml s3://continuo/service-py/rel-py-v1/contract.yaml
```

Only now post the release, with one extra field — `"kind": "python"` — telling
Continuo to skip the compile leg and read the contract you just uploaded. Note
the `image_tag`: a dbt release passes the bare tag (`v1`) and Continuo composes
the image reference itself, but **a python release's `image_tag` is used
verbatim as the full image reference** — pass `service-py:v1`, not `v1`, or
the node will fail to dispatch at run time:

```bash
curl -s -X POST http://localhost:8088/releases \
  -H 'content-type: application/json' \
  -d '{
    "release_id": "rel-py-v1",
    "service": "service-py",
    "image_tag": "service-py:v1",
    "bootstrap": true,
    "kind": "python",
    "repo": "<your-username>/continuo-demo",
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
 card   | 7208 |   5091176
 fx     | 4786 |  38852881
```

One table, two halves. The `card` rows came from core's own seed through a
`{{ ref() }}`. The `fx` rows arrived via finance's `fx_transactions_eur`, a model
in a different dbt project, built by a different image, released separately —
reached by nothing more than `FROM analytics.fx_transactions_eur`.

This run also unlocked validation. Every model in the graph now exists as a
real production table — which is exactly what the next chapter's release will
be proven against.

---

## 8. Add a model, watch validation work

Everything on the platform so far went in through the bootstrap door. Now do
what a team does on an ordinary Tuesday: add one model and release it — this
time with validation on.

The model computes each marketing channel's return on spend, and its input is
`ltv_per_user` — a model that belongs to **finance**. Create
`services/marketing/models/channel_roi.sql`:

```sql
{{ config(materialized='table', tags=['daily']) }}

-- Per-channel return on marketing spend: lifetime contribution earned per
-- euro spent. analytics.ltv_per_user is produced by the finance project and
-- is referenced by its raw schema-qualified name (see "the four rules").
SELECT
    channel,
    COUNT(*)                                   AS users,
    ROUND(SUM(contribution_margin_eur), 2)     AS contribution_eur,
    ROUND(SUM(marketing_cost_eur), 2)          AS spend_eur,
    ROUND(SUM(contribution_margin_eur) / NULLIF(SUM(marketing_cost_eur), 0), 2)
        AS roi
FROM analytics.ltv_per_user
GROUP BY channel
```

Build it, load it, release it — from here on, `bootstrap` is `false`:

```bash
docker build -t marketing:v2 services/marketing
kind load docker-image marketing:v2 --name continuo

curl -s -X POST http://localhost:8088/releases \
  -H 'content-type: application/json' \
  -d '{
    "release_id": "rel-marketing-v2",
    "service": "marketing",
    "image_tag": "v2",
    "bootstrap": false,
    "repo": "<your-username>/continuo-demo",
    "commit_sha": "'"$(git rev-parse HEAD)"'"
  }' | jq
```

The stages are the same ones every release walks — but this time `validating`
is not an instant no-op. It runs for a minute or two, and this is the
blue/green mechanism at work: Continuo created a temporary candidate schema,
built the release's seeds into it, **cloned the unchanged upstream tables the
change reads from production** — this is why chapter 6 had to bootstrap —
rewrote the in-scope models' compiled SQL to read from that schema instead of
production, and ran them there. Production was untouched throughout, and the
candidate schema was torn down afterwards. No data is copied — only
schema-level structure.

Look at what was in scope:

```bash
curl -s http://localhost:8088/releases/rel-marketing-v2 | jq '.validation_node_ids'
```

```json
[
  "analytics.channel_roi",
  "analytics.daily_transactions",
  "analytics.fx_transactions_eur",
  "analytics.ltv_per_user",
  "analytics.marketing_cost_per_user",
  "analytics.marketing_spend_monthly",
  "analytics.operational_cost_per_user",
  "analytics.operational_costs_monthly",
  "analytics.revenue_per_user",
  "analytics.seed_card_transactions",
  "analytics.seed_fx_rates_eur",
  "analytics.seed_fx_transactions",
  "analytics.seed_marketing_spend",
  "analytics.seed_operational_costs",
  "analytics.seed_user_acquisition",
  "analytics.seed_users"
]
```

**Read that list again.** You added one model to marketing. Continuo put
sixteen of the graph's nineteen nodes in scope: your new node and its entire
upstream lineage — `ltv_per_user` from finance, `revenue_per_user` and
`daily_transactions` from core, and the seeds under all of them. You changed
marketing; Continuo worked out from the SQL that proving the change requires
two other teams' models, cloned their production structure, and ran your model
against it. Nobody declared those relationships anywhere.

Only because all of that passed did `marketing:v2` become marketing's
production image. In the UI, the graph now shows `channel_roi` downstream of
finance's `ltv_per_user` — a node that exists but has never run, which the last
chapter fixes.

---

## 9. Break it on purpose

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
       "bootstrap":false,"repo":"<your-username>/continuo-demo",
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
  "failing_nodes": [
    "analytics.channel_roi",
    "analytics.daily_transactions",
    "analytics.dbt_daily_kpis",
    "analytics.ltv_per_user",
    "analytics.py_daily_kpis",
    "analytics.revenue_per_user"
  ]
}
```

The release was rejected, and look at where the damage landed: models in
**core**, the `channel_roi` you added to **marketing** in the previous
chapter, finance's own `ltv_per_user`, and even **service-py**'s python node —
everything downstream of the column you removed, across all four projects and
both runtimes. You touched one file in finance.

Now check production:

```bash
curl -s http://localhost:8088/current-prod | jq '.current_prod_release_id'
```

Still `rel-marketing-v2`, the release chapter 8 promoted. Production never saw
`finance:v2`. The `daily` schedule will keep running the last version that
passed, indefinitely, and a scheduled run tonight will produce correct data.

This is the part that is hard to get any other way. In a conventional setup this
change merges, deploys, and breaks `core` the next time it runs — and the person
who gets paged is on the core team, looking at a model they did not change.
Here, the failure surfaced against the person who made the change, before it
reached production, in another team's model they had never heard of.

---

## 10. Let the agent propose a fix

*This chapter needs the credentials from chapter 1. Everything above did not.*

A rejected release tells you something broke. Continuo can also try to fix it.

`remediation` classifies the rejection, and for a fixable one `agent-remediation`
reads the failing model's source, asks an LLM for a fix, and surfaces
the proposal for a human to approve. It never writes to your repository on its
own — the output is a diff you review, and a pull request you choose to open.

The proposal is anchored to a commit in your repository, so the broken code
needs to exist there. Commit and push your change to your fork, and use that
commit:

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

Then restart the agent so it picks the credentials up — the upgrade changes
only the Secret, and a Secret change alone does not restart the pod that
reads it at startup:

```bash
kubectl -n continuo rollout restart deploy/agent-remediation
```

Re-release the broken finance with a new `release_id` and the pushed
`commit_sha`. When it is rejected this time, the proposed fix appears in the UI
against the failed release.

Opening the pull request from the UI additionally needs a GitHub App
(`github.appId`, `github.installationId`, `github.appPrivateKey`) — a read-only
token is enough to *see* the proposal, but not to create a PR with it.

---

## 11. Run it again

One green run proved the graph you bootstrapped. Close the loop by proving the
graph as it stands now — new node included, broken release excluded.

Open the UI, pick the `daily` schedule, and press **▶ Trigger run** again.

The run picks up `channel_roi` as just another node: downstream of finance's
`ltv_per_user`, so it is dispatched after it, across the same four services.
The rejected `finance:v2` is nowhere in it — production still runs the finance
that passed.

When it finishes green, read your new model's answer:

```bash
PGPW=$(kubectl -n continuo get secret continuo-postgresql -o jsonpath='{.data.password}' | base64 -d)
kubectl -n continuo exec continuo-postgresql-0 -- env PGPASSWORD="$PGPW" \
  psql -U continuo -d continuo_dbt -c \
  "select channel, users, spend_eur, roi from analytics.channel_roi order by roi desc"
```

```
  channel   | users | spend_eur | roi
------------+-------+-----------+------
 organic    |   310 |      0.00 |
 referral   |   390 |   9750.00 | 3.38
 google_ads |   709 |  18585.46 | 3.03
 meta_ads   |   324 |  11248.70 | 2.35
 tiktok_ads |   190 |   7888.94 | 2.08
 email      |    39 |   1591.56 | 1.75
 affiliate  |    38 |   7939.07 | 0.41
```

That is the whole loop: four projects released independently, one graph derived
from their SQL, a new model validated against production before it could touch
it, a bad change stopped at the same gate, and a run that crosses team
boundaries in the right order every time.

---

## 12. Clean up

```bash
kind delete cluster --name continuo
```

That removes everything: the cluster, all Continuo services, both databases, and
every image you side-loaded. The only thing left on your machine is the images in
your local Docker daemon, which `docker image rm core:v1 finance:v1 finance:v2
marketing:v1 marketing:v2 service-py:v1` clears.

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

**A python node fails at run time with `carries no explicit tag or digest`,
and the run never finishes.** The python release was posted with a bare
`image_tag` like `"v1"`. A python release's `image_tag` is used verbatim as
the full image reference (`service-py:v1`); with a bare tag the executor
cannot build the pod spec and the node fails without ever creating a Job.
Cancel the run, re-release the python service with the full reference (a new
`release_id`, and re-upload its contract under that id), and run again.

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

**A validated release is rejected with `relation "analytics.…" does not
exist`.** The release was validated before the tables it reads physically
existed — usually a cold install where a service was released with
`"bootstrap": false` before the first full run. Bootstrap every service first
(chapter 6), run the DAG once (chapter 7), then re-release with a new
`release_id`.

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
