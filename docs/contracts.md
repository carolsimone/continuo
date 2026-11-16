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

This page is for the team that writes a Python service. It covers:

- [the file and its name](#the-file-and-its-name)
- [a full example](#a-full-example)
- [what continuo fills in](#what-continuo-fills-in) and [what it refuses](#what-continuo-refuses)
- [the other two kinds of Python node](#the-other-two-kinds)
- [the CI steps](#the-ci-steps) and [the library version](#the-library-version-ci-must-pin)
- [reading a contract back](#reading-a-contract-back) and [the history](#the-history-of-a-contract)
- [the live folder](#the-live-folder) and [a DataHub recipe](#a-datahub-recipe)
- [moving from `contract.yaml`](#moving-from-contractyaml)

A dbt node has no contract file. continuo reads it from your dbt project.

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
| `script` | The script that builds the table, as a path from the repository root. |
| `readQueries` | The queries the script reads, by name. Each one is a single `SELECT`. |
| `schema` | One object, with `physicalType: table`, the table name and its columns. |
| `properties` | The columns, with at least one. `physicalType` is the column's type, and `required: true` means the column never holds NULL. |

The same file may also carry these, when the node needs them:

| Property | Meaning |
|---|---|
| `timeoutMinutes` | How long a task of the node may run, a whole number from 1 to 10,080. Without it, the install's `runs.taskTimeoutHours` applies. |
| `modeType` | `replace` (the default), `append` or `upsert`. A `python-csv` node allows only `replace`. |
| `primaryKey: true` on columns, with `primaryKeyPosition` | The key of an `upsert` node. Each key column is `required: true` and is not `DOUBLE PRECISION`. On a `replace` or `append` node the key only describes the data. |
| `extraColumns` | `raise` (the default) or `warn`: what happens when the script returns a column the contract does not declare. |
| `assets` | Files the script reads besides its own code, as paths from the repository root. Your Dockerfile copies each one into the image at the same path. |
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

Each refusal has a reason. A refusal rejects the release as
`invalid_contract`, and the message names the field. The last row is the one
exception: it rejects the release as `unqualified_reference`.

| Refused | Why |
|---|---|
| `servers`, `tenant`, `contractCreatedTs` | These are facts about the install and the clock. continuo writes `servers` itself, when the operator sets it. |
| A `provenance` property | continuo writes it into the published file. Inside your file it would freeze at the first release. |
| A `service` other than the release's | A contract belongs to one service. |
| A `name`, a `physicalName` or a column `physicalName` other than the real one | A table must not have two names. |
| An `id` with an upper case letter | Table names in continuo are lower case. |
| `partitioned: true`, a `partitionKeyPosition` that is not the default | The runtime cannot honour them. |
| `filePath`, `materialization`, `contractEnforced`, `access`, `group`, `deprecationDate`, `freshnessBuildAfter`, `snapshot`, `quoteColumns` | These describe how dbt builds a node. Nothing reads them on a Python node, so a stored one would say something untrue. |
| A continuo property that does not exist | Most often a spelling mistake. |
| `assets` on a `python-csv` node | A csv node has no script, so nothing reads the asset. |
| A `NUMERIC(12)` or any other type outside the runtime's list | The runtime must be able to create the column. |
| A read query with two statements | A read is one `SELECT`. |
| A read query that names a table without its schema | continuo must know which table you read to draw the edge. |

Two details of the last rule:

- Write `analytics.daily_transactions`, never `daily_transactions`.
- A name defined by `WITH` hides a table of the same name only inside the part
  of the query where that `WITH` is in scope. Outside it, the name is a real
  table again, and it needs its schema.

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
   reads every file with the same checks continuo runs at release time. Use
   `--dialect trino` on a Trino install. It prints every problem it finds, so a
   mistake fails the pull request and never reaches a release.
2. **Build the image** and push it. The image base is
   `ghcr.io/carolsimone/continuo-python-runtime-<engine>:v0.13.0`.
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
one it gives the node, the file and every issue, as `<path>: <message>` joined
by `; `. The release page in the dashboard lists them. continuo does not try
to fix this rejection for you.

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
  carries. `continuo-python-runtime` 0.13.0 carries `continuo-contracts` 0.2.0.
- `GET /api/v1/releases/<id>` has the field `continuo_contracts`: the version
  that read this release's contracts. It is `""` until the parse ends. It is set
  for a rejected release too, so a rejection tells you which version said no.

```json
{"release_id": "rel-py-v2", "status": "rejected", "terminal": true,
 "reject_reason": "invalid_contract", "continuo_contracts": "0.2.0"}
```

## Reading a contract back

Three read-only commands ask catalog. `<unique_id>` is `<schema>.<table>`:

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

## The live folder

catalog writes the contracts that are in production to your bucket:

```
tenants/<tenant>/contracts/live/<unique_id>.odcs.yaml
tenants/<tenant>/contracts/live/_index.json
```

There is one file for each live node that has a contract. catalog rewrites a
file only when its text changes, deletes the file of a node that is no longer
live, and writes `_index.json` last. A promotion that is not newer than the
one the folder already shows rewrites nothing, so a late or repeated promotion
never puts an old contract back.

A published file is the stored contract with three changes:

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
            promotedAt: "2026-11-27T16:40:12Z", continuoContracts: 0.2.0,
            sourcePath: contracts/py_daily_kpis.odcs.yaml}
```

`_index.json` lists the files and the promotion they belong to:

```json
{"schema_version": 1, "tenant_id": "default", "promotion_seq": 52,
 "release_id": "rel-77aa0bc-230", "written_at": "2026-11-27T16:40:14Z",
 "settings_sha256": "1c9e...",
 "files": [{"unique_id": "analytics.py_daily_kpis",
            "key": "analytics.py_daily_kpis.odcs.yaml",
            "version": 3, "contract_hash": "sha256:3f1c..."}]}
```

While catalog is part way through a rewrite, the index lists each file it is
about to put with `"version": 0` and an empty `contract_hash`. A file at
version 0 is one that no index vouches for, so wait for the final index.

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
- `schema` is the contract's schema, the part of the id before the table name.
- On Trino, `database` fills the field `catalog` of the entry. Set it
  explicitly. When it is empty, continuo uses `validation.warehouseDatabase`,
  and that is the name of a Postgres database.
- With no `host` (the default), no file has a `servers` entry.
- With a `host`, `port` must be a whole number from 1 to 65535. Otherwise the
  chart does not render and catalog does not start.

When catalog starts with other settings than it last used, it rewrites every
file.

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

It is canonical JSON, compressed with gzip. The events that announce a release
carry its URI and its SHA-256. It is written once and never changes. A tool
that reads it is bound by [Reading releases from continuo](read-contract.md).

## Moving from `contract.yaml`

The old kind `python` and its single `contract.yaml` keep working. A service
moves when it is ready: convert the files, change the CI steps above, and post
with the kind `python-odcs`. A release of a service uses one kind. A later
release removes the old kind and `contract.yaml`, so move before then.

Every field of the old file has a place in the new one:

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
