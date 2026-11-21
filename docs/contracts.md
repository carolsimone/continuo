# Python contracts

A Python node is a table that a script builds. A contract is the small file
that tells continuo everything about that table: who owns it, when it runs,
which script builds it, which tables the script reads, and which columns the
table has.

The file is YAML and follows the
[Open Data Contract Standard](https://bitol-io.github.io/open-data-contract-standard/)
(ODCS) version 3.2.0. That means other tools can read it too, and
[a live folder](#the-live-folder) in your bucket hands them every contract that
is in production.

This page is for the team that writes a Python service. A team that writes a
dbt service reads [dbt nodes](#dbt-nodes) at the end. It covers:

- [the file and its name](#the-file-and-its-name)
- [a full example](#a-full-example)
- [what continuo fills in](#what-continuo-fills-in) and [what it refuses](#what-continuo-refuses)
- [the other two kinds of Python node](#the-other-two-kinds)
- [the CI steps](#the-ci-steps) and [the library version](#the-library-version-ci-must-pin)
- [reading a contract back](#reading-a-contract-back) and [the history](#the-history-of-a-contract)
- [the live folder](#the-live-folder) and [a DataHub recipe](#a-datahub-recipe)
- [the contract bundle](#the-contract-bundle)
- [binding a contract to the company ontology](ontology.md)
- [moving from `contract.yaml`](#moving-from-contractyaml)
- [dbt nodes](#dbt-nodes), [what makes a dbt contract invalid](#what-makes-a-dbt-contract-invalid)
  and [how to check a dbt project in CI](#check-a-dbt-project-in-ci)

A dbt node has a contract too, but no contract file. continuo generates it from
the `manifest.json` of your dbt project and from the tests on the node.
[dbt nodes](#dbt-nodes) says what continuo reads there and what it does with
each part.

## The file and its name

Put one file per node in the `contracts/` folder of your service:

```
contracts/<table>.odcs.yaml
```

When two nodes of one service have the same table name in two schemas, use the
longer name for both:

```
contracts/<schema>.<table>.odcs.yaml
```

The name must match the contract. A contract with `id: analytics.py_daily_kpis`
lives in `py_daily_kpis.odcs.yaml` or in `analytics.py_daily_kpis.odcs.yaml`.
Any other name fails the check, and the message names the two that are allowed.

## A full example

```yaml
# contracts/py_daily_kpis.odcs.yaml
apiVersion: v3.2.0
kind: DataContract
id: analytics.py_daily_kpis
name: py_daily_kpis
version: "0"
description:
  purpose: Daily transaction count and total amount in EUR, rolled up from analytics.daily_transactions.
team:
  name: service-py
customProperties:
  - {vendor: continuo, property: schedule, value: daily}
  - {vendor: continuo, property: criticality, value: SECONDARY}
  - {vendor: continuo, property: script, value: scripts/py_daily_kpis.py}
  - vendor: continuo
    property: readQueries
    value:
      daily: select created_at, amount_eur from analytics.daily_transactions
schema:
  - name: py_daily_kpis
    physicalType: table
    properties:
      - {name: day, physicalType: DATE, required: true}
      - {name: tx_count, physicalType: INTEGER, required: true}
      - {name: total_amount, physicalType: "NUMERIC(12,2)", required: true}
```

What each part says:

| Part | Meaning |
|---|---|
| `apiVersion`, `kind` | Always `v3.2.0` and `DataContract`. |
| `id` | `<schema>.<table>`, in lower case. It is the node's name everywhere in continuo. |
| `name` | The table. You may leave it out. |
| `version` | Any text. continuo ignores it and numbers the versions itself. |
| `description.purpose` | Optional. A sentence about the table. |
| `team.name` | The owner of the node. |
| `schedule` | The schedule the node runs on. |
| `criticality` | `REGULATORY`, `CORE` or `SECONDARY`. |
| `script` | The path of the script that builds the table. |
| `readQueries` | The queries the script reads, by name. Each one is a single `SELECT`. |
| `schema` | One object, with `physicalType: table`, the table name and its columns. |
| `properties` | The columns. There is at least one. `physicalType` is the column's type. `required: true` means the column never holds NULL. |

A path in `script`, and in `assets` below, is relative to the folder you give
`pack` as `--repo-root`. That folder is the service's directory, such as
`services/service-py`.

The same file may also carry these, when the node needs them:

| Property | Meaning |
|---|---|
| `timeoutMinutes` | How long a task of the node may run, a whole number from 1 to 10,080. Without it, the install's `runs.taskTimeoutHours` applies. |
| `modeType` | `replace` (the default), `append` or `upsert`. A `python-csv` node allows only `replace`. |
| `primaryKey: true` on columns, with `primaryKeyPosition` | The key of an `upsert` node. Each key column is `required: true` and is not `DOUBLE PRECISION`. On a `replace` or `append` node the key only describes the data. |
| `extraColumns` | `raise` (the default) or `warn`: what happens when the script returns a column the contract does not declare. |
| `assets` | The paths of files the script reads besides its own code. Your Dockerfile copies each one into the image at the same path. |
| `config` | Table layout, such as indexes. |

The runtime's README describes how each setting changes a run:
[continuo-python-runtime](https://github.com/carolsimone/continuo-python-runtime).

## What continuo fills in

You do not write these. continuo adds them when it reads the file, and the
stored contract holds them:

| Added | Value |
|---|---|
| `service` | The service of the release. |
| `kind` | `python-node`, when you write none. |
| `status` | `active`. |
| `version` | `"0"`. The published files carry the number continuo counted. |
| `physicalName` | The table. |
| `logicalType` on each column | Derived from its `physicalType`: `date`, `integer`, `number`, `string` and so on. |
| `reads` | The tables your read queries name, each as `<schema>.<table>`. |

With the install's ontology switch on, the stored contract of a service the
ontology declares holds more: its `domain`, a link to each thing it is bound to
and what the ontology knows of each bound column. [The company
ontology](ontology.md#what-continuo-fills-in) says what. With the switch off,
continuo stores the contract without its bindings.

For `py_daily_kpis`, the stored contract holds this on top of what you wrote:

```yaml
customProperties:
  - {vendor: continuo, property: kind, value: python-node}
  - {vendor: continuo, property: service, value: service-py}
status: active
schema:
  - name: py_daily_kpis
    physicalName: py_daily_kpis
    properties:
      - {name: day, physicalType: DATE, logicalType: date, required: true}
    customProperties:
      - {vendor: continuo, property: reads, value: [analytics.daily_transactions]}
```

## What continuo refuses

Each row below rejects the release as `invalid_contract`, and the message names
the field. The last row is the one exception: it rejects the release as
`unqualified_reference`.

| Refused | Why |
|---|---|
| `servers`, `tenant`, `contractCreatedTs` | These are facts about the install and the clock. continuo writes `servers` itself, when the operator sets it. |
| A `provenance` property | continuo writes it into the published file. Inside your file it would freeze at the first release. |
| A `service` other than the release's | A contract belongs to one service. |
| A `name`, a `physicalName` or a column `physicalName` other than the real one | A table must not have two names. |
| An `id` with an upper case letter, a non-ASCII one (`Ö`) included | Table names in continuo are lower case. The contract library refuses an ASCII capital. continuo refuses any other, and the message names the file and the id continuo keys the node by. |
| `partitioned: true`, a `partitionKeyPosition` that is not the default | The runtime cannot honour them. |
| `filePath`, `materialization`, `contractEnforced`, `access`, `group`, `deprecationDate`, `freshnessBuildAfter`, `snapshot`, `quoteColumns` | These describe how dbt builds a node. Nothing reads them on a Python node, so a stored one would say something untrue. |
| A continuo property that does not exist | Most often a spelling mistake. |
| An ontology binding of the wrong shape, such as `entity` with a number | The ontology bindings are `entity`, `grain`, `fixed` and `filter` on the table, and `property`, `local` and `aggregate` on a column. continuo checks their shape whether the ontology is on or off. See [the company ontology](ontology.md#writing-a-binding). |
| `assets` on a `python-csv` node | A csv node has no script, so nothing reads the asset. |
| A `NUMERIC(12)` or any other type outside the runtime's list | The runtime must be able to create the column. |
| A read query with two statements | A read is one `SELECT`. |
| A read query that names a table without its schema | continuo must know which table you read to draw the edge. |

Two details of the last rule:

- Write `analytics.daily_transactions`, never `daily_transactions`.
- A name defined by `WITH` hides a table of the same name only inside the part
  of the query where that `WITH` is in scope. Outside it, the name is a real
  table again, and it needs its schema.

Two services that each declare the same `id` are not refused here: the release
is rejected as `duplicate_table`, which names both services, and the contract
bundle of that parse holds neither contract.

Everything else that ODCS allows, for example `description.usage`, `domain`,
`tags`, `quality` or a column's `description`, stays in the file as you wrote
it.

## The other two kinds

A `python-csv` node has no script. The runtime loads a csv file from object
storage and writes it as a table. Use `csvSource` and leave out `script`,
`readQueries` and `assets`:

```yaml
# contracts/demo_orders_csv.odcs.yaml
apiVersion: v3.2.0
kind: DataContract
id: analytics.demo_orders_csv
name: demo_orders_csv
version: "0"
description:
  purpose: "Contract-only csv node: loads the demo orders export from object storage"
team:
  name: service-py
customProperties:
  - {vendor: continuo, property: kind, value: python-csv}
  - {vendor: continuo, property: schedule, value: daily}
  - {vendor: continuo, property: criticality, value: SECONDARY}
  - {vendor: continuo, property: csvSource, value: "s3://continuo/static-files/demo/orders.csv"}
schema:
  - name: demo_orders_csv
    physicalType: table
    properties:
      - {name: order_id, physicalType: INTEGER, required: true}
      - {name: customer, physicalType: "VARCHAR(64)"}
      - {name: amount, physicalType: DOUBLE PRECISION}
```

A `python-api` node has a script that fetches its own data, so it has no
`readQueries`. It may name one Kubernetes Secret with `secretRef`. The name
starts with `continuo-api-`:

```yaml
# contracts/fx_eur_usd_rates.odcs.yaml
apiVersion: v3.2.0
kind: DataContract
id: analytics.fx_eur_usd_rates
version: "0"
team:
  name: data-team
customProperties:
  - {vendor: continuo, property: kind, value: python-api}
  - {vendor: continuo, property: schedule, value: daily}
  - {vendor: continuo, property: criticality, value: SECONDARY}
  - {vendor: continuo, property: script, value: scripts/fx_eur_usd_rates.py}
  - {vendor: continuo, property: secretRef, value: continuo-api-fx}
schema:
  - name: fx_eur_usd_rates
    physicalType: table
    properties:
      - {name: rate_date, physicalType: DATE, required: true}
      - {name: rate, physicalType: DOUBLE PRECISION, required: true}
```

| Kind | Needs | Refuses |
|---|---|---|
| `python-node` | `script`, `readQueries` | `csvSource`, `secretRef` |
| `python-csv` | `csvSource` | `script`, `readQueries`, `secretRef`, `assets` |
| `python-api` | `script` | `readQueries`, `csvSource` |

## The CI steps

Your pipeline does these steps, in this order.

1. **Check the contracts.** `continuo-runtime validate contracts --dialect postgres`
   reads the files with the same checks continuo runs at release time. Use
   `--dialect trino` on a Trino install. The step stops at the first file that
   is not valid and prints every problem of that file. Fix the file, then run
   the step again to check the next one. A mistake fails the pull request and
   never reaches a release. A contract that binds to the company ontology is
   checked against it with `--service <name> --ontology <file>` (see
   [the company ontology](ontology.md#checking-bindings-in-ci)).
2. **Build the image** and push it. The image base is
   `ghcr.io/carolsimone/continuo-python-runtime-<engine>:v0.14.0`.
3. **Pack the contracts.** `pack` checks the files again, lints the scripts and
   writes one bundle:

   ```bash
   continuo-runtime pack contracts/ --service service-py --repo-root . \
     --image "$IMAGE" --dialect postgres --out odcs-bundle.json
   ```

   `pack` records which packages the image holds. `--image <ref>` reads the
   list from the image with `docker run`, so the image must exist on the
   machine. `--image-packages <file>` takes a list you saved for that image
   before. Give one of the two, not both.
4. **Upload the bundle** to `s3://<bucket>/<service>/<release_id>/odcs-bundle.json`.
5. **Post the release** with the kind `python-odcs`. The `image_tag` is the
   full image reference, used exactly as written.

   ```bash
   curl -s -X POST $API/releases \
     -H "Authorization: Bearer $TOKEN" \
     -H 'content-type: application/json' \
     -d '{
       "release_id": "rel-py-v1",
       "service": "service-py",
       "image_tag": "service-py:v1",
       "kind": "python-odcs",
       "repo": "<your-username>/continuo-demo-docs",
       "commit_sha": "<sha>"
     }'
   ```

6. **Poll** `GET /api/v1/releases/<release_id>` until `terminal` is `true`.

If a contract is not valid, the release is rejected with the reason
`invalid_contract`. One rejection names every contract that failed. For each
one it gives the node, the file and every issue. Each issue reads
`<path>: <message>`, and `; ` joins them. The release page in the dashboard
lists them. continuo does not try to fix this rejection for you.

A read that names a table without its schema is rejected as
`unqualified_reference` instead, and continuo can propose a fix for that one.

## The library version CI must pin

The checks of `validate` and of continuo itself are the same code: the
`continuo-contracts` library. If your CI used a newer copy than your install,
a contract could pass in CI and fail in the release.

To prevent that, continuo refuses a bundle that a newer library wrote. The
release is rejected `invalid_artifact` with the detail
`the bundle was written by continuo-contracts <x> and this install runs <y>:
upgrade continuo, or pin an older runtime`. A bundle from an older library is
always safe.

So pin the runtime in CI to a version whose library is not newer than your
install's. Three places tell you the version:

- The chart's [CHANGELOG](../deploy/continuo/CHANGELOG.md) has a line that
  names `continuo-contracts <version>` every time topology-controller moves to
  another version. continuo's own CI fails a change of that version that has
  no such line.
- The runtime's own CHANGELOG names the library version each runtime release
  carries. `continuo-python-runtime` 0.14.0 carries `continuo-contracts` 0.4.1.
- `GET /api/v1/releases/<id>` has the field `continuo_contracts`: the version
  that read this release's contracts. It is `""` until the parse ends. It is set
  for a rejected release too, so a rejection tells you which version said no.

A part of such an answer, with the other fields left out:

```json
{"release_id": "rel-py-v2", "status": "rejected", "terminal": true,
 "reject_reason": "invalid_contract", "continuo_contracts": "0.4.1"}
```

A dbt service has no bundle from its CI: continuo generates its contracts. Its
CI uses the same library through `continuo-contracts dbt check` (see
[how to check a dbt project in CI](#check-a-dbt-project-in-ci)). Install the
version that the chart's CHANGELOG names for your install.

## Reading a contract back

Three read-only commands ask catalog. `<unique_id>` is `<schema>.<table>`, of a
Python node or of a dbt model, seed or snapshot:

```bash
continuo node contract analytics.py_daily_kpis
continuo node contract-history analytics.py_daily_kpis
continuo node contract-diff analytics.py_daily_kpis 1 3
```

`node contract` prints the contract the node has now, with its ODCS document.
`node contract-history` lists the versions, newest first. `node contract-diff`
lists the fields where two versions differ.

The node page in the dashboard has a Contract tab with the same three views:
the contract as YAML, its versions and the diff of any two. Open it with
`?tab=contract`.

## The history of a contract

catalog keeps every version of every contract. Each time a promotion changes a
node's contract, the node gets the next number: 1, 2, 3 and so on. A promotion
that leaves the contract as it was adds nothing. A change back to an earlier
contract still gets a new number, and the history says "same as version N" for
it.

Each version records the release that made it current, the time, and the
version of `continuo-contracts` that read it.

By default catalog keeps every version. To bound it, set
`catalog.contractHistory.retentionDays` in your values file:

```yaml
catalog:
  contractHistory:
    retentionDays: 365
```

`0` (the default) keeps everything. A number above 0, up to 36500, deletes a
version once its promotion is older than that many days. catalog never deletes
the current contract of a node, or the one before it.

A node has a current contract while two things are true:

- the node is live, and
- the newest promotion the live folder was written for carried its contract.

When a node stops having a contract, for example because its model loses its
owner and continuo leaves it out, the next promotion carries no contract for
it. Its file leaves the live folder and the node has no current contract. Its
history stays.

## The live folder

catalog writes the contracts that are in production to your bucket:

```
tenants/<tenant>/contracts/live/<unique_id>.odcs.yaml
tenants/<tenant>/contracts/live/_index.json
```

There is one file for each live node that has a contract. A dbt node has one
too, except an ephemeral model: it makes no table, so it gets no file, but
catalog keeps its contract and its history. catalog rewrites a file only when
its text changes, deletes the file of a node that is no longer live, and writes
`_index.json` last. A promotion that is not newer than the
one the folder already shows rewrites nothing, so a late or repeated promotion
never puts an old contract back.

A published file is the stored contract with these changes:

- `version` is the number catalog counted, as text. It is `"3"` after the third
  change.
- A `servers` entry says where the table lives. It is there only when the
  operator set `contracts.publish.server.host`.
- A custom property `provenance` says which release made this version current:

```yaml
customProperties:
  - vendor: continuo
    property: provenance
    value: {contractHash: "sha256:3f1c...", contentHash: "sha256:91d4...",
            releaseId: rel-77aa0bc-230, promotionSeq: 52,
            promotedAt: "2026-11-27T16:40:12Z", continuoContracts: 0.4.1,
            sourcePath: contracts/py_daily_kpis.odcs.yaml}
```

- A contract that was filled in from the company ontology has one more key in
  `provenance`, `ontologyVersion`: the release of the ontology and the SHA-256 of
  its file, `<release id>@sha256:<hex>`. A contract that no ontology touched has
  no such key.
- When the operator sets `contracts.ontology.baseUrl`, each ontology link of the
  contract (`continuo-ontology:Order`) is the absolute address
  `<baseUrl>/Order`. With no `baseUrl` the links stay as stored. See
  [the company ontology](ontology.md#the-switch).

`_index.json` lists the files and the promotion they belong to:

```json
{"schema_version": 1, "tenant_id": "default", "promotion_seq": 52,
 "release_id": "rel-77aa0bc-230", "written_at": "2026-11-27T16:40:14Z",
 "settings_sha256": "1c9e...",
 "files": [{"unique_id": "analytics.py_daily_kpis",
            "key": "analytics.py_daily_kpis.odcs.yaml",
            "version": 3, "contract_hash": "sha256:3f1c..."}]}
```

An index entry whose file's `servers` entry names another schema than the id's,
such as `Analytics` for `analytics.orders`, also holds that schema as
`"physical_schema": "Analytics"`. The entry of every other file has no such key.
catalog puts a file again when the schema of its `servers` entry changes, even
when only the case changes and the contract does not.

While catalog is part way through a rewrite, the index lists each file it is
about to put or delete with `"version": 0` and an empty `contract_hash`. A file
at version 0 is one that no index vouches for, so wait for the final index.
A rewrite that stops part way leaves those entries, so the next rewrite puts
every one of them again or deletes it.

A rewrite needs the index. If `_index.json` cannot be read (it is not JSON, or
a newer catalog wrote another `schema_version`), every rewrite fails, and
catalog logs an error that names the key. Delete
`tenants/<tenant>/contracts/live/_index.json`: the next rewrite puts every file
again and writes a new index.

catalog does not start a rewrite while an export or a restore of the tenant's
graph runs; it tries the promotion again once that ends.

### The `servers` entry

The operator sets three values, and every file gets the same entry:

```yaml
contracts:
  publish:
    server:
      host: warehouse.internal.example
      port: 5432
      database: continuo_dbt
```

The file then holds:

```yaml
servers:
  - {server: warehouse, type: postgres, host: warehouse.internal.example, port: 5432, database: continuo_dbt, schema: analytics}
```

- `type` is `postgres` or `trino`. It comes from `validation.engine`, and you do
  not set it.
- `schema` is the schema the node is declared in, the one dbt and continuo
  build the table in, in the topology's spelling. A schema declared with
  capitals, such as `Analytics`, keeps them here, while the contract's `id` stays
  in lower case (`analytics.orders`). A node the topology has not reached yet has
  no declared schema, and its entry takes the part of the id before the table
  name. When catalog rewrites the folder before the topology swap of the same
  promotion, the file of a new node names the id's schema, and the file of a node
  whose schema only changes in case keeps the old spelling, until the next
  promotion rewrites it. Where the two spellings are the same, as they are for
  every node declared in a lower-case schema, the entry reads as it would from
  the id.
- On Trino, `database` fills the field `catalog` of the entry. Set it
  explicitly. When it is empty, continuo uses `validation.warehouseDatabase`,
  and that is the name of a Postgres database.
- With no `host` (the default), no file has a `servers` entry.
- With a `host`, `port` must be a whole number from 1 to 65535. Otherwise the
  chart does not render and catalog does not start.

When catalog starts with other settings than it last used, it rewrites every
file. While the catalog pods roll after a change of these settings, a pod still
on the old settings can rewrite the folder after the new pod starts. Those
files keep the old entry. The next promotion, or the next catalog start,
rewrites every file again.

### Access

An outside tool needs read access only. Give it `s3:GetObject` and
`s3:ListBucket` on `tenants/<tenant>/contracts/live/`. catalog's own role needs
`s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` and `s3:ListBucket` on the
same prefix. The chart's README has the full table ("S3 access").

If catalog cannot write a file, the series
`continuo_catalog_live_contract_writes_total{outcome="failed"}` rises and
catalog tries the promotion again. [OBSERVABILITY.md](../deploy/OBSERVABILITY.md)
has an alert rule for it.

## A DataHub recipe

DataHub's `odcs` source reads ODCS files from `s3://` paths. Point it at the
live folder:

```yaml
source:
  type: odcs
  config:
    path: s3://<bucket>/tenants/default/contracts/live/*.odcs.yaml
    aws_connection:
      aws_region: eu-central-1   # the region of your bucket
```

What to expect:

- DataHub needs a version of its `odcs` source that accepts `apiVersion: v3.2.0`.
  The documentation of DataHub 1.8.0 says it does. A file whose version the
  source does not list is skipped with a warning.
- DataHub binds each schema entry to its table through `servers`. With no
  `servers` entry, DataHub leaves the file unbound. Set
  `contracts.publish.server.host` to bind them.
- DataHub shows its own `odcs.*` keys only. The continuo custom properties,
  such as `provenance`, do not reach it.
- A `quality` rule becomes an assertion in DataHub, and a link in
  `authoritativeDefinitions` becomes a link.
- DataHub reads the owner from team members. continuo writes `team.name` and no
  members, so DataHub shows no owner.
- DataHub skips a file above 5 MB.

## The contract bundle

Every release that passes also writes one document with all of its contracts:

```
tenants/default/topologies/<release_id>/contracts.json.gz
```

It holds the contract of every dbt model, seed and snapshot and of every Python
node of the release. It is canonical JSON, compressed with gzip. The events that
announce a release carry its URI and its SHA-256. It is written once and never
changes. Its `ontology` value is `null` unless the install's ontology switch is
on, and otherwise names the ontology the release used and lists what the check
found that did not reject the release (see
[the company ontology](ontology.md#where-to-read-the-result)).
[Reading releases from continuo](read-contract.md) says which of its fields a
tool may rely on.

## Moving from `contract.yaml`

A Python service releases as `python-odcs`. The kind `python`, one
`contract.yaml` for the whole service, is not accepted: `POST /api/v1/releases`
answers `400` with a message that says `kind "python" is no longer accepted`
and names `python-odcs`.

An upgrade of continuo stops while a service in production is still released
as the kind `python`, in one of two ways. With your own database the migration
Job fails and `helm upgrade` fails before any pod changes. With the bundled
Postgres `helm upgrade` can report success while the pods roll, and
release-controller refuses to start; `helm rollback` recovers. Either way the
message names the services. Release each of them as `python-odcs`, on a
continuo version that accepts it, then upgrade.

A release of the kind `python` that is still queued or in flight when the new
version starts is rejected `invalid_artifact`, with the refusal above as its
detail, and promotes nothing. Upgrade with no release in flight.

To move a service, convert the files, change the CI steps above, and post with
the kind `python-odcs`. Every field of a `contract.yaml` has a place in the
`.odcs.yaml` file:

| In `contract.yaml` | In the `.odcs.yaml` file |
|---|---|
| `schema` and `table` | `id: <schema>.<table>` and the name of the schema object |
| `owner` | `team.name` |
| `kind` | the property `kind` |
| `schedule`, `criticality`, `script`, `secret_ref`, `assets`, `config` | the property with the same name, `secretRef` for `secret_ref` |
| `timeout_minutes` | the property `timeoutMinutes` |
| `mode_type`, `extra_columns` | the properties `modeType` and `extraColumns` |
| `reads` of a `python-node` | the property `readQueries` |
| `reads: {csv: <uri>}` of a `python-csv` node | the property `csvSource` |
| `output_columns`, with `type` and `nullable: false` | `properties`, with `physicalType` and `required: true` |
| `primary_key` | `primaryKey: true` and `primaryKeyPosition` on those columns |

Copy each read query, type and script path as it is. The code fingerprint of
a node does not change, so the move leaves it as it was.

## dbt nodes

Every dbt model, seed and snapshot that continuo keeps has an ODCS contract.
You do not write it. continuo generates it from your dbt project each time you
post a release, from the node's entry in `manifest.json` and from the dbt tests
on the node. A dbt test has no contract, and neither has an `unknown-external`
table.

Everything continuo knows about a dbt node comes from your dbt project. When
you post a release, continuo runs `dbt compile` in your
image and reads two files from that compile:

- `manifest.json`, which dbt writes. It holds every node of your project, its
  SQL, its config and its `meta`.
- `compile-extras.json`, which continuo's own script writes beside it in the
  same compile Job. It holds what the manifest leaves out: each seed's file
  hash, its CSV header and its column types, the `flags:` block of
  `dbt_project.yml`, and the versions of the dbt packages in your image.
  [deploy/dbt-image-contract.md](../deploy/dbt-image-contract.md) says what the
  compile Job needs from your image.

continuo uses what it reads in two ways:

- The **contract** of a node holds its facts: who owns it, when it runs, how
  long a task may take, how dbt builds it and which columns it has. The
  `continuo-contracts` library writes it from the node's `meta`, tags, config,
  columns and tests.
- The **code** of a node is its SQL, its config, the macros it uses and, for a
  seed, its file. continuo fingerprints it (see
  [the code version](#the-code-version-of-a-dbt-node)) and finds the edges of
  the graph in the compiled SQL.

The topology of a release says what the contracts say. A node's name, table,
relation, kind, service, schedule, task timeout, materialization and file come
from its contract, and so do the owner and the criticality it carries. Its SQL,
config, hashes and edges come from the manifest and the SQL.

The table below puts a Python contract next to a dbt node. Each row is one fact
continuo needs, and where it comes from for each kind of node.

| Fact | Python contract | dbt node |
|---|---|---|
| The node's name | `id` | `<schema>.<name>` of the manifest node, in lower case, which is the contract's `id` |
| The table it writes | the table of `id` | the node's `alias`, or its `name` when it has no alias: the `physicalName` of the contract's schema object |
| Owner | `team.name` | `meta.owner`, as `team.name` |
| Schedule | `schedule` | the first tag, as `schedule` |
| Criticality | `criticality` | `meta.criticality`, as `criticality` |
| Task timeout | `timeoutMinutes` | `meta.timeout_minutes`, as `timeoutMinutes` |
| The tables it reads | `readQueries` | the compiled SQL (`compiled_code`), and the reads you declare, as the property `reads` |
| Its columns | `properties` | the `columns` the node declares, as `properties` |
| The code continuo fingerprints | the script, its shared code and the contract's code fields | SQL, macros, config and seed files (see [the code version](#the-code-version-of-a-dbt-node)) |

`meta` here means the node's `config.meta`. dbt fills it from
`dbt_project.yml`, the folder and the model itself, so a key set on the model
wins over the same key set for the whole project.

### What the contract holds

The README of `continuo-contracts` (section `dbt`) lists every field. These are
the main ones:

| From the node | In the contract |
|---|---|
| `meta.owner` | `team.name` |
| `tags`, in the order dbt lists them | `tags`. The first tag is also the property `schedule`, which a dormant node does not have |
| `meta.criticality`, `meta.timeout_minutes` | the properties `criticality` and `timeoutMinutes` |
| `config.materialized`, with `unique_key` and `incremental_strategy` when set and `on_schema_change` when it is not `ignore` | the property `materialization`. The schema object's `physicalType` is `view` for a view, `ephemeral` for an ephemeral model and `table` for any other node |
| The service, `original_file_path` and the resource type | the properties `service`, `filePath` and `kind` (`dbt-model`, `dbt-seed` or `dbt-snapshot`) |
| `description` | `description.purpose` |
| `meta.continuo.readers` and `config.grants` | `roles` |
| `config.contract.enforced`, `access`, `group`, `deprecation_date` and `freshness.build_after` | the properties `contractEnforced`, `access`, `group`, `deprecationDate` and `freshnessBuildAfter` |
| A snapshot's `strategy`, `unique_key` and the like | the property `snapshot` |
| A seed's `quote_columns` | the property `quoteColumns` |
| `columns` | `properties`: the name, the description, the tags, the declared type (`physicalType`) with the logical type derived from it, and `classification` from `meta.continuo.classification`. A model's `primary_key` sets `primaryKey` on its columns |
| The dbt tests of the node | `quality` rules on the column or the table. A `not_null` test also marks its column `required`, and a `unique` test marks it `unique`, when the test is strict: severity `error`, no `where`, and dbt's default `warn_if`, `error_if` and `fail_calc`. A strict `relationships` test adds a relationship. Any other test is a `custom` rule |

A field with no value is left out. An empty `meta.timeout_minutes` (`""`, `[]`
or `{}`) is no timeout, and an empty `meta.criticality` is `SECONDARY`.

A `relationships` test points at another node or a source. When the manifest
holds that target neither among its nodes nor among its sources, the test is a
`custom` rule that keeps its `to` and `field`, and the node is still valid. That
happens to a test that points at a `local_stub` model in a manifest stored
without its stubs, as the baseline uploader `dbt/dbt_upload` stores it. continuo
logs one warning per release with the number of such tests. A target the
manifest holds gives a relationship, even when continuo leaves that target out,
as it does a `local_stub` model.

### Which nodes continuo keeps

continuo looks at every entry of the manifest's `nodes`:

| dbt resource | In continuo |
|---|---|
| model | a `dbt-model` node |
| seed | a `dbt-seed` node |
| snapshot | a `dbt-snapshot` node |
| test | a `dbt-test` node, but only when it tests a model, seed or snapshot that continuo keeps |
| anything else, such as an analysis or a hook | no node |

continuo leaves out a model, seed or snapshot when one of these is true. It
checks them in this order:

1. It has no `meta.owner`, or the owner is empty.
2. One of its tags is `local_stub`.
3. It comes from an installed dbt package (one your `packages.yml` names) and
   has no tag.

A node left out does not fail the release, and it has no contract. continuo logs
it and counts it in `continuo_topology_release_skipped_nodes`, with the reason
`no_owner`, `local_stub` or `installed_package`.

Which service a node belongs to is the first part of its dbt `fqn`, with `_`
written as `-`. Your project is the manifest's `metadata.project_name`, written
the same way, or the service the release names when the manifest has none.

A node of an installed package that has an owner and a tag is not left out.
It rejects the release as `invalid_artifact`, because a release of a service
holds only the nodes of its own project. A manifest where no node is kept
rejects the release as `invalid_artifact` too.

A model or snapshot with an owner and no tag is a dormant node. It has no
schedule, and you run it by hand. A release checks it like any other model. A
seed with no tag takes the schedule `seed`. Rule 4 of
[the four rules a dbt project must follow](run-projects-in-continuo.md#the-four-rules-a-dbt-project-must-follow)
explains dormant nodes, packages and `local_stub` in full.

A test writes no table. Its name in continuo is dbt's own `unique_id`, which
starts with `test.`. A test needs no owner and no tag, and no schedule runs
it. During validation, continuo checks the test's compiled SQL against the
release's new tables, so a test that names a column the release dropped
rejects the release. Each test also adds one to the `test_count` of each node
it tests, and so does each unit test of a model.

### The name and the table

A dbt node's name in continuo is `<schema>.<name>`, in lower case, and it is the
contract's `id`. `<schema>` is the schema dbt worked out for the node, after
your `generate_schema_name`. `<name>` is the node's `name`. A versioned model is
`<schema>.<name>.v<version>`, so each version is a node of its own.

A schema you declare with capitals, such as `Analytics`, is lower case in the
node's name and the contract's `id`, so the node is `analytics.orders`. The
topology's schema keeps your spelling, `Analytics`, because dbt builds the
table under it, and validation copies the table from there. The SQL of the
readers names the schema the way dbt wrote it, and continuo finds the node. The
`servers` entry of the node's published file names `Analytics` too, so an
outside tool binds the file to the table that exists.

continuo writes the schema and the name in lower case one at a time, while the
library writes `<schema>.<name>` in lower case as one string, so the contract's
`id` must come out the same both ways. It does not for a capital Greek `Σ` in
the wrong place: a schema that ends in one, such as `DATAΣ` before the table
`Orders`, is `dataς` on its own and `dataσ.orders` as one string. Such a node
rejects the release as `invalid_contract`, and the rejection names both names.
Rename the schema or the table. The check you run in CI does not find it.

The table the node writes is its `alias` when it has one, and the contract keeps
it as the `physicalName`. Another node's SQL reaches it through that table. Two
nodes that write the same table are rejected as `duplicate_table`. So are two
nodes with one contract id, in one service or in two: the release is published
with neither contract in the bundle, and release-controller rejects it.

### Owner, schedule and the other `meta` keys

These keys of `meta`, and the tags, matter most:

| Key | What continuo does with it |
|---|---|
| `meta.owner` | It must be there and not empty, or the node is left out. It is the contract's `team.name`. |
| First tag | The schedule the node belongs to, with no whitespace and at most 50 characters. A schedule runs on its own only once you give it a cron. |
| `local_stub` tag | The node is left out. |
| `meta.timeout_minutes` | How long a task of the node may run: a whole number from 1 to 10,080. Any other value rejects the release as `invalid_contract` and names the node and the field. An empty value is no timeout. See [Declare the task timeout of a node](run-projects-in-continuo.md#declare-the-task-timeout-of-a-node). |
| `meta.criticality` | `REGULATORY`, `CORE` or `SECONDARY`. A missing or empty value is `SECONDARY`. Any other value rejects the release as `invalid_contract` and names the node and the field. |
| `meta.continuo.reads` | A list of tables the node reads that its SQL does not name, each `<schema>.<table>`. See [the tables a node reads](#the-tables-a-node-reads). |
| `meta.continuo.readers` | The roles that read the node. Each one is a role with read access in the contract. |

### Materialization

continuo reads the node's `config.materialized`, keeps it on the node and writes
it into the contract. dbt writes it for every node. A manifest entry without it
is an invalid contract that names the node.

- `ephemeral`: dbt writes no table. It pastes the model into each reader as a
  `WITH` part named `__dbt__cte__<name>`. continuo sees that part and makes the
  ephemeral model an upstream of the reader, when both are in the same service.
- `view` and `materialized_view`: when a seed load drops the seed's table,
  continuo rebuilds the views and materialized views below that seed in the
  same run. It walks through ephemeral models on the way.
  [Seeds that change](run-projects-in-continuo.md#seeds-that-change) has the
  details, and it covers a seed's `full_refresh` setting, which continuo also
  reads.
- Every materialization is part of the node's config, so changing it changes
  the node's code version.

### The tables a node reads

continuo does not use dbt's `ref()` graph. It reads each node's compiled SQL
with sqlglot, in the SQL dialect of your warehouse, and lists every table the
SQL names. When some node builds that table, the reader depends on that node.
That is why a raw name such as `analytics.fx_transactions_eur` works across two
projects, where `ref()` cannot. The rules:

- Every table needs its schema. A table named without one rejects the release
  as `unqualified_reference`, and continuo can propose a fix. `ref()` and
  `source()` normally compile to a name with a schema.
- A name defined by `WITH` hides a table of the same name only where that
  `WITH` is in scope, as for a Python read query.
- A read of the node's own table is not a dependency.
- A function called with a schema, such as `pg_catalog.generate_series(1, 3)`,
  is not a table.
- continuo matches a table by schema and name. A catalog or database in front
  of them does not change which node it is.
- SQL that the parser cannot read rejects the release as `invalid_sql`.

A table that a node reads and no node builds becomes a node of type
`unknown-external`: `<schema>.<table>`, with no service, no schedule and no
code. A table you read with `source()` usually ends up as one. continuo never
runs it. The warehouse's own tables never become nodes: `information_schema`
and every `pg_` schema on Postgres, and `information_schema` and the `system`
catalog on Trino. The table of a left-out model becomes an `unknown-external`
node when another node reads it. The table of a dormant model does not, because
a dormant model counts as built.

The contract lists the tables the node reads: the tables its compiled SQL
names, except the warehouse's own tables, and the tables you declare in
`meta.continuo.reads`. Each is written `<schema>.<table>` in lower case, sorted
and listed once, in the property `reads` of the contract's schema object. A
table or a declared read that cannot be written that way, such as a quoted name
with whitespace or a dot, makes the contract invalid and names the table.

### Columns

The contract of a model or snapshot lists the columns the node declares: the
`columns` of its YAML, in the order dbt gives them. A snapshot's contract also
lists the columns dbt adds to its table (`dbt_scd_id`, `dbt_updated_at`,
`dbt_valid_from` and `dbt_valid_to`, and `dbt_is_deleted` with `hard_deletes:
new_record`) unless the snapshot declares them. A node that declares no column
has none in its contract.

When a model enforces its dbt contract (`contract: {enforced: true}`), its
declared columns and its model constraints are also part of its code version.
Each declared column counts with its name, `data_type`, constraints and `quote`.

A seed's contract lists the columns its YAML declares, with the type of
`config.column_types` when the seed sets one. The compile also writes the seed's
CSV header in file order and its column types to `compile-extras.json`.
continuo uses them to tell a change of columns from a change of rows, which
decides how the seed loads again (see
[Seeds that change](run-projects-in-continuo.md#seeds-that-change)). Without an
entry for the seed in `compile-extras.json`, the seed has no column list in the
topology.

**A seed's declared columns must be its CSV header.** The release compares the
two lists, in both directions, by exact name, and the order does not matter. A
column that is in the header and not declared, and a column that is declared and
not in the header, each reject the release as `invalid_contract`. The rejection
names the seed and each column:

```text
seed.shop.countries: name is in the CSV header but not declared; code is declared but not in the CSV header
```

- `ID` and `id` are two columns. A seed that declares no column is rejected for
  every column of its header.
- One rejection names every problem of a seed, the contract's own issues first.
- The release reads the header from `compile-extras.json`. The
  [check you run in CI](#check-a-dbt-project-in-ci) reads the CSV file of your
  project and words the same lines.
- A seed that `compile-extras.json` does not name has no known header, and the
  release does not compare it. That happens when the compile wrote no
  `compile-extras.json`, or when it leaves the seed out. continuo keeps the seed
  and logs one warning per release, with the number of such seeds in each
  service.

### The code version of a dbt node

Each node has a code version, its `content_hash`. continuo uses it to tell
which nodes a release changes. A changed node, and every node below it, is
built again in the release's validation. A seed whose code version changes is
loaded again after the release is promoted. catalog keeps the history of each
node's code versions.

These change a node's code version:

- The node's SQL file. Only line endings and blank space at the very start and
  end of the file are ignored.
- Its compiled SQL. So does anything that changes what `dbt compile` writes: a
  `var()`, an `env_var()`, a `ref()` that points somewhere else, or an
  ephemeral model it pastes in.
- A seed's CSV file, byte for byte.
- A macro that you or a package wrote and that the node's build reaches.
  dbt's own built-in macros do not count.
- A dbt function the node calls.
- A model's unit tests.
- The parts every build of your project runs: `on-run-start` and `on-run-end`
  hooks, your project's `generate_schema_name` and `generate_alias_name`, and
  the `flags:` block of `dbt_project.yml` when it is not empty.
- The schema and the table the node writes.
- Every key of the node's `config` except `access`, `description`, `docs`,
  `freshness`, `grants`, `group`, `meta` and `tags`. So `materialized`,
  `unique_key`, `full_refresh`, `pre_hook`, `post_hook` and the rest all count.
- The declared columns and constraints of a model whose dbt contract is
  enforced.

These do not change it:

- `meta` keys, `owner`, `criticality` and `timeout_minutes` among them, and
  tags.
- `description`, `docs`, a column's description, `grants`, `access`, `group`
  and `freshness`.
- The values that change on every compile: dbt's invocation id, the time the
  compile started, and the node's database name. continuo replaces them with
  fixed text before it hashes.

So a node whose only change is a `meta` key or a tag is not built again in
validation. There are two exceptions. A `meta` value your SQL reads changes
the compiled SQL. A tag you set in the SQL file's own `config()` line changes
the SQL file. The contract of such a node does change: a new owner, tag or
`meta` value gives the node a new contract version in catalog.

The setting `contract: {enforced: true}` is a dbt setting, and it stays in the
node's config and in the contract as `contractEnforced`. Only a model folds its
declared columns and constraints into the code version, because dbt (1.12.0b1
with the postgres adapter) builds a model's table from them. dbt ignores the
setting on a seed. It enforces it on a snapshot, and the code version does not
fold a snapshot's declared columns or constraints, as the `fingerprints.yaml` of
`continuo-contracts` declares for `dbt-snapshot`.

A seed's file hash comes from `compile-extras.json`. When that file has no
entry for the seed, continuo uses dbt's own checksum instead. For a seed over
1 MiB, dbt's checksum is only the file's path, so a change of rows does not
change the code version. continuo logs a warning for each such seed.

`compile-extras.json` must be valid. A file that is not JSON, has another
`version`, names another service, or holds a value of the wrong type rejects
the release as `invalid_artifact`.

The code bundle of each release (`code-bundles/<release>/bundle.json`) holds
each dbt node's SQL, compiled SQL, config and the four parts of its code
version. [Reading releases from continuo](read-contract.md) says which of its
fields a tool may rely on.

### Where a dbt contract goes

The contract bundle of a release holds the contract of each dbt node the release
keeps, with the family `dbt`, beside the Python ones. For a dbt node, `source.path`
is its `original_file_path`, and `source.sha256` is the SHA-256 of the manifest
fields the contract was generated from, not of a file.

catalog keeps every version of a dbt contract as it does for a Python one.
`continuo node contract <unique_id>` and the Contract tab of the node page show
it. The [live folder](#the-live-folder) has one file for each dbt model, seed
and snapshot, except an ephemeral model. A dbt test and an `unknown-external`
table have no contract. Their Contract tab says `This node has no contract. A
dbt test and an unknown-external table have none.`, and
`continuo node contract <unique_id>` answers `not_found` (exit 3).

Your dbt project, its YAML and its SQL, is where these facts live. To change a
contract, change the project and post a release.

### What makes a dbt contract invalid

A dbt contract is invalid when the library cannot generate it or when it fails
its checks. The release is then rejected as `invalid_contract`. One rejection
names every invalid contract of the release, dbt and Python together. For each
one it gives the node, its file and every problem, joined with `; `. A problem
of the contract itself reads `<path>: <message>`:

```text
customProperties[3].value: `criticality` is 'high', it must be one of REGULATORY, CORE, SECONDARY
```

The other problems read differently. A field the library cannot read starts
with the node's id (`model.shop.orders: tags must be a list of strings, got
str`). A table or a read that cannot be listed starts with `compiled SQL:` or
`reads:`. A seed-header problem reads like `name is in the CSV header but not
declared`, and the detail of such a seed starts with the seed's manifest id.

These make a dbt contract invalid:

- A `meta.criticality` other than `REGULATORY`, `CORE` or `SECONDARY`.
- A first tag with more than 50 characters or with whitespace in it.
- A `meta.timeout_minutes` that is not a whole number from 1 to 10,080, such as
  `30.0` or `"30"`.
- A `meta.owner` that holds only whitespace. An empty one leaves the node out.
- A schema with whitespace, a dot or a slash in it.
- A manifest entry with no `config.materialized` or no `original_file_path`. dbt
  always writes both.
- A field of the node in a shape the library cannot read, such as `tags` that is
  not a list of text, a `meta.owner` that is not text, a column's `data_type`
  that is not text, or a `deprecation_date` that is not written `YYYY-MM-DD`.
- A table that the compiled SQL names, or a read that the node declares, that
  cannot be written `<schema>.<table>`.
- A seed whose declared columns and CSV header differ (see
  [Columns](#columns)).
- A snapshot with `contract: {enforced: true}`. dbt does not support a contract
  on a snapshot: it enforces it on the first run and fails every later run.
  Remove the flag.
- An ontology binding of the wrong shape in a node's `meta.continuo` or in a
  column's, such as `entity: 42`. This is so whether the ontology is on or off
  (see [the company ontology](ontology.md#writing-a-binding)).

Other faults are not about one contract. A manifest that is not valid JSON, a
`metadata.project_name` that is not text, a test whose links to its nodes are
not lists of text, a kept node or test with no `fqn`, and text that UTF-8 cannot
encode, such as a lone surrogate, reject the release as `invalid_artifact`. A
defect in the `continuo-contracts` library itself ends the release as
`internal`: continuo does not blame your project for it.

continuo does not try to fix an `invalid_contract` rejection for you.

### Check a dbt project in CI

Your CI can ask what continuo will make of a dbt project before a release does.
Install `continuo-contracts` in a Python 3.14 environment of its own (the version
your install's CHANGELOG names), and run the check on the project folder:

```bash
continuo-contracts dbt check services/finance --dbt .venv-dbt/bin/dbt
```

The check never imports dbt. It runs the `dbt` program you name with
`--dbt` (the `dbt` on `PATH` when you name none) as a new process, so dbt can
run on another Python than the check does. It runs `dbt parse`, which needs no
warehouse. A profile that reads `env_var('NAME')` with no default gets the text
`continuo-lite-check` when `NAME` is not set.

With the manifest that `dbt parse` wrote, it does what the release does with each
model, seed and snapshot:

1. It leaves out the nodes that continuo leaves out.
2. It checks that each node it keeps belongs to the service (see `--service`
   below).
3. It generates the contract of each node it keeps, and checks it.
4. It compares each seed's declared columns with the first row of the seed's CSV
   file in your project folder.

With `--ontology <file>` it also reads the bindings of each contract against the
company's ontology file (see
[the company ontology](ontology.md#checking-bindings-in-ci)). Without it, it
checks no binding against an ontology.

It prints a line for each thing it finds. A line is `INFO` for a node that is
left out, for a dormant node and for a seed of an installed package, and `ERROR`
for a problem. A node with several problems gets one line for each, and a node
with nothing to say gets no line. The lines go to the error output. Each starts
with its level and then reads `<manifest node id>: <message>` (the message alone
for a line about the whole service):

```text
INFO LITE CHECK. Passing it does not guarantee that continuo will create this service's contracts at release time.
ERROR seed.finance.seed_operational_costs: cost_date is in the CSV header but not declared
INFO What only the release does:
INFO   dbt runs in the team's image: continuo compiles the project in the team's own image, and CI can run another dbt version.
...
INFO LITE CHECK. Passing it does not guarantee that continuo will create this service's contracts at release time.
```

The command exits with 0 when no line is an error. It exits with 1 when a line is
an error or `dbt parse` fails, and with 2 when it cannot run, for example when no
`dbt` program is found. With `--json`, the standard output holds one document:
`lite`, `ok`, `service`, `lines` and `contracts`, the `contract_hash` of each
valid contract by contract id. With `--manifest <file>`, it reads a
`manifest.json` you built already and runs no dbt, and it still reads the seeds'
CSV files from the project folder.

`--service` is the name of the service you deploy the project under. Without
it, the check takes the folder's name, with `_` written `-`. The check compares
the service of each node it keeps with it, as the release does, and a node of
another service is an error line: the release rejects such a manifest as
`invalid_artifact`. So when the folder is not named after the service, pass
`--service`:

```bash
continuo-contracts dbt check dbt/finance_project --service finance --dbt .venv-dbt/bin/dbt
```

`--service` is also the project's service when the manifest names no project.

The check is lite: a run with no error does not promise that the release
creates the service's contracts. It prints that sentence first and last, and it
lists what only the release does between them:

- dbt runs in your image at release time, and CI can run another dbt version.
- The release parses every service of the estate. The checks that need the
  contracts of other services, such as duplicate relations, are not in the lite
  check.
- The release adds the tables that a node's compiled SQL names to the reads the
  node declares. `dbt parse` compiles nothing, so the contract of the lite check
  lists only the declared reads, and its hash differs from the release's for a
  node whose SQL names another table.
- The install runs the `continuo-contracts` version its chart names, and your CI
  can pin another.
- `servers` and absolute links exist only in published files.
- The list ends with the install's ontology. With the ontology switch on, the
  release checks each binding against the ontology the install runs. The lite
  check reads the file `--ontology` names, which can differ, and without
  `--ontology` it checks no binding.

Two more differences concern seeds. The lite check reads the CSV file, and the
release reads the header from `compile-extras.json`. The lite check does not
compare the header of a seed of an installed package. The release compares it
when `compile-extras.json` names the seed.

Run the check on every pull request of a dbt service, and fix every error line,
seed headers included, before you release.

Before you upgrade continuo, run the check with the version the new chart's
CHANGELOG names, and release and promote each fix on the version you run now.
A release parses the production manifest of every service, not only the one it
changes. So after the upgrade, one service with an invalid contract rejects the
release of every other service, and two such services block each other's fix.
If you upgraded first, go back to the chart version you ran before, release and
promote the fixes, then upgrade again.
