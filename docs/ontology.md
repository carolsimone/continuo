# The company ontology

An ontology is one YAML file that says what your company's data is about. It
names the domains of the company, the projects that belong to each domain, and
the things the data describes, such as an order or a user. For each thing it
names the key and the properties.

A contract can bind to a thing. A binding says: this table holds orders, this
column is the order's key, this column is the order's amount. continuo checks
every binding against the ontology and fills the contract in from it. A binding
that names a thing the ontology does not have rejects the release.

The ontology is off by default. With it off, continuo ignores the ontology and
keeps no binding. [The switch](#the-switch) says how to turn it on.

Three groups of people take part:

- The operator of the install turns the switch on.
- The owner of the ontology releases the ontology file.
- Each project team writes bindings in its contracts and adds the check to its CI.

This page starts with an example. The rest says how each part works:

- [an example](#an-example)
- [the switch](#the-switch)
- [releasing the ontology](#releasing-the-ontology)
- [writing a binding](#writing-a-binding)
- [the rules](#the-rules)
- [checking bindings in CI](#checking-bindings-in-ci)
- [where to read the result](#where-to-read-the-result)

## An example

A shop has two projects: `shop-dbt`, a dbt project, and `shop-py`, a Python
project. Both belong to the domain `shop`. Both build a table of orders.

### The ontology file

```yaml
# ontology.yaml
principals:
  domains:
    shop: {owner_team: shop-data}
  projects:
    shop-dbt: {domain: shop, kind: dbt}
    shop-py:  {domain: shop, kind: python}
  teams:
    shop-data: {idp_group: "okta:shop-data"}

entities:
  Order:
    owner: shop
    key: {order_id: integer}
    properties:
      amount_eur: {type: number}
```

`principals` says who acts on data. Here that is one domain with its owner team,
and two projects that name their domain. The `kind` of a project in this file is
the ontology's own word (`dbt` or `python`), the same word as the kind a project
is released with. `entities` says what the data is
about. Here that is one thing, `Order`. It belongs to the domain `shop`, it is
identified by `order_id`, and it has one property, `amount_eur`.

### A dbt model that binds to it

The model binds in its `meta.continuo`. The table says which thing it is a copy
of, and each column says which part of the thing it holds.

```yaml
# models/orders.yml
models:
  - name: orders
    config:
      meta:
        owner: shop-data
        continuo:
          entity: Order
    columns:
      - name: order_id
        meta: {continuo: {property: Order.order_id}}
      - name: amount_eur
        meta: {continuo: {property: Order.amount_eur}}
      - name: created_at
        meta: {continuo: {local: true}}
```

`created_at` is not part of an order, so the column is `local`: no thing owns it.

### A Python contract that binds to it

A Python contract writes the same bindings as custom properties of the
`continuo` vendor:

```yaml
# contracts/py_orders.odcs.yaml
apiVersion: v3.2.0
kind: DataContract
id: shop.py_orders
version: "0"
team:
  name: shop-data
customProperties:
  - {vendor: continuo, property: schedule, value: daily}
  - {vendor: continuo, property: criticality, value: SECONDARY}
  - {vendor: continuo, property: script, value: scripts/py_orders.py}
  - vendor: continuo
    property: readQueries
    value:
      orders: select order_id, amount_eur from shop.orders
schema:
  - name: py_orders
    physicalType: table
    customProperties:
      - {vendor: continuo, property: entity, value: Order}
    properties:
      - name: order_id
        physicalType: INTEGER
        customProperties: [{vendor: continuo, property: property, value: Order.order_id}]
      - name: amount_eur
        physicalType: "NUMERIC(12,2)"
        customProperties: [{vendor: continuo, property: property, value: Order.amount_eur}]
```

### What continuo fills in

You do not write these. With the switch on and the ontology released, continuo
adds them to the stored contract of a project the ontology declares. For the
`orders` model:

```yaml
id: shop.orders
domain: shop
schema:
  - name: orders
    customProperties:
      - {vendor: continuo, property: entity, value: Order}
    authoritativeDefinitions:
      - {type: ontology, url: "continuo-ontology:Order"}
    properties:
      - name: order_id
        primaryKey: true
        customProperties: [{vendor: continuo, property: property, value: Order.order_id}]
        authoritativeDefinitions:
          - {type: ontology, url: "continuo-ontology:Order.order_id"}
      - name: amount_eur
        customProperties: [{vendor: continuo, property: property, value: Order.amount_eur}]
        authoritativeDefinitions:
          - {type: ontology, url: "continuo-ontology:Order.amount_eur"}
      - name: created_at
        customProperties: [{vendor: continuo, property: local, value: true}]
```

- `domain` is the domain of the project, from the ontology.
- Each bound table and column gets a link to its thing. The link reads
  `continuo-ontology:<Thing>` or `continuo-ontology:<Thing>.<field>`.
  The published file turns it into a web address when you set
  `contracts.ontology.baseUrl` ([below](#the-switch)).
- A column that holds a key part is marked `primaryKey`.
- A bound column also gets the type of its property, and `classification:
  personal` when the property is personal.
- A column with no binding whose name is a key part or a property of the table's
  thing is bound to it, as if you had written its `property`.
- A `local` column gets nothing.

### What a failing check looks like

Suppose the model above says `entity: Orders` by mistake. The check in CI says:

```text
ERROR model.shop_dbt.orders: entity Orders is not a thing in the ontology
```

The same mistake in a release rejects it as `invalid_contract`. The rejection
names the node `shop.orders`. Its detail says which rule found the problem and
what it found:

```text
ontology contract: entity Orders is not a thing in the ontology
```

Suppose instead that the Python contract binds a column to `Order.amount`, a
property the thing does not have. The check in CI says:

```text
ERROR continuo_python_runtime.cli: error [contract] shop.py_orders: column amount_eur -> Order.amount: Order.amount is not a property or key part of Order
```

A release rejects that contract the same way: as `invalid_contract`, with the
node and `ontology contract: column amount_eur -> Order.amount: ...`.

## The switch

The ontology has two values in the chart. Both are in `values.yaml`, under
`contracts.ontology`:

| Value | Default | What it does |
|---|---|---|
| `enabled` | `false` | Turns the ontology on for the install. |
| `baseUrl` | `""` | The web address that the ontology links of published contracts start with. Empty, or an `http://` or `https://` address with a host name, no credentials, no query or fragment and no whitespace. |

**With the switch off** (the default), continuo ignores the ontology:

- A release of the kind `ontology` is refused with a 400. A release of that kind
  that is queued or parsing when the switch is turned off is rejected
  `invalid_artifact` with the text of the 400, at its activation or at its parse
  result, and is never promoted, so production never holds an ontology file that
  nobody read.
- The ontology of a release is ignored, and the log says so once.
- Each contract is stored without its bindings and links. A contract that writes
  bindings has the same hash as the same contract with none.
- The contract bundle of the release says `"ontology": null`.

**With the switch on:**

- continuo reads the ontology of production on every release. It fills in the
  contracts of the projects the ontology declares and checks them. An error
  rejects the release.
- A project the ontology does not declare keeps its contracts as written and gets
  no check. Each contract of it that writes a binding is a warning in the bundle.
- With no ontology released yet, every binding is kept as written and nothing
  checks it. The bundle says so with one warning:
  `the ontology switch is on and no ontology is released: bindings are kept as written and not checked`.
- A warning or an information line never rejects a release. They go into the
  bundle ([below](#where-to-read-the-result)).

The `baseUrl` only changes the published form of the links. With
`baseUrl: https://ontology.example/things`, the link of the thing `Order` in a
published contract reads `https://ontology.example/things/Order`, and a link of a
property reads `https://ontology.example/things/Order.amount_eur`. A trailing `/`
is left off. With no `baseUrl` the links stay `continuo-ontology:Order`. The
stored contract and its hash do not change.

The chart sets `ONTOLOGY_ENABLED` for topology-controller and release-controller,
and `ONTOLOGY_BASE_URL` for catalog. A component stops at start when the value is
not `true` or `false`, or when the base address is not valid. A change of either
restarts every component.

## Releasing the ontology

An install has one ontology. It is released like any other project, with the kind
`ontology`, from a repository of its own. The repository holds one file,
`ontology.yaml`. The ontology project has no image, no node and no code, so
continuo does not compile or validate anything for it.

1. **Check the file.** In the ontology repository's CI run
   `continuo-contracts ontology check ontology.yaml`. It checks the file alone:
   each project names a declared domain, each thing an owner that is a declared
   domain, each reference a thing and a property that exist. It exits with 1
   when the file has an error.
2. **Upload the file** to
   `s3://<bucket>/<project>/<release_id>/ontology.yaml`, with the name of the
   ontology project, such as `company-ontology`, as `<project>`.
3. **Post the release** with the kind `ontology`. The `image_tag` is required
   and not used: write any value.

   ```bash
   curl -s -X POST $API/releases \
     -H "Authorization: Bearer $TOKEN" \
     -H 'content-type: application/json' \
     -d '{
       "release_id": "rel-onto-1",
       "project": "company-ontology",
       "image_tag": "ontology",
       "kind": "ontology"
     }'
   ```

4. **Poll** `GET /api/v1/releases/<release_id>` until `terminal` is `true`.

The ontology repository needs a binding in `ciAuth.bindings`, like any other
repository ([deploy/AUTH.md](../deploy/AUTH.md)).

When the ontology is released, topology-controller reads it before any manifest.
It fills in and checks the contracts of every project in production against it,
as a release of one of those projects does. So a new ontology can break a
binding of a project that did not change. Then:

- the release of the ontology is rejected as `invalid_contract`;
- each failed node is a node of another project whose binding the new ontology
  breaks, and the detail says which rule found the problem;
- production keeps the old ontology, and no fix is proposed, because
  `invalid_contract` is not healable. The owners of those nodes change their
  bindings, or the ontology changes.

A file that does not load is rejected as `invalid_artifact` and names the
ontology project. So is a file whose own rules find an error.

When the release passes, catalog records a new contract version for each node
whose stored contract changed, and names the ontology that made it: the id of
the ontology release and the SHA-256 of the file, written
`<release id>@sha256:<hex>`.

These answers can come back from the release routes:

| Answer | When |
|---|---|
| 400 `kind "ontology" is accepted only when the install's ontology switch is on (contracts.ontology.enabled)` | The switch is off. |
| 409 `project "<other>" is the install's ontology: an install has one ontology project` | Another project already has the kind `ontology`, in production or in a release that has not ended. A CI token reads `an install has one ontology project`, without the other project's name. |
| 409 `project "<name>" is released as kind "<kind>": a project cannot change between kind "ontology" and a node kind` | The project's production release has another kind, or the ontology project is released as a node kind. |
| 400 `a fix's release check cannot have kind "ontology"` | A fix's release check names the kind `ontology`. |

The kind-change 409 is given when a release is received. Two queued releases of one
project, one `ontology` and one of a node kind, both pass that check. When the
second of them comes up to run, it is rejected `invalid_artifact` with the same
text, is never promoted and writes no production pointer.

## Writing a binding

A binding is a key in `meta.continuo` of a dbt node, or a custom property of the
`continuo` vendor in a Python contract. A key is optional. A contract of a
project the ontology declares must say what it is, with `entity` or with
`grain`.

On the table:

| Key | Value | Meaning |
|---|---|---|
| `entity` | the name of a thing | The table is a copy of this thing. |
| `grain` | a list of names | The table is an aggregate with this grain. Write `entity` or `grain`, not both. |
| `fixed` | key part to value | The table holds only the rows where a key part has this value. |
| `filter` | field to value | The table holds only the rows where a field has this value. |

On each column:

| Key | Value | Meaning |
|---|---|---|
| `property` | `<Thing>.<field>` | The column holds this key part or property of a thing. |
| `local` | `true` | No thing owns the column. Leave it out, never write `false`. |
| `aggregate` | a mapping | The column is an aggregate. In a dbt model its parts are the keys `from`, `agg`, `where` and `period`, written beside each other. |

A column carries one of the three. A copy of a thing carries a column for every
key part of the thing, or fixes or filters it.

A key of the wrong shape, such as `entity: 42`, rejects the release as
`invalid_contract`, with the switch off too. The check in CI finds it.

## The rules

The check has eight rules. The first one runs over the ontology file alone. The
other seven run over contracts.

| Rule | What it checks |
|---|---|
| `ontology` | The file: owners, domains, references, `derived_from`, access rules. |
| `contract` | A contract's domain, its `entity` or `grain`, `fixed`, `filter`, each column's binding, and the domains of its readers. |
| `coverage` | Each column has one binding, and a copy carries every key part. |
| `quality` | A data-quality test, on the table or on a column, names columns of the contract. |
| `privacy` | A personal property is classified personal, and only one is. |
| `access` | A contract reads only what its domain may read. |
| `order` | The copies are in layers, none is in a cycle, and a thing's copies follow its birth copy. |
| `drift` | Each property and each thing is held by a copy. |

A release runs the seven rules over contracts. The check of a producer's CI runs
`contract`, `coverage`, `quality` and `privacy`.

Each finding is an error, a warning or information. An error rejects the
release. A warning or information does not.

## Checking bindings in CI

Add the check to the CI of each project that writes bindings. It runs four of the
release's rules (`contract`, `coverage`, `quality` and `privacy`) over the
contracts of the project, and prints what a release would say about each
binding. Do it before you release: a mistake then
fails the pull request.

The check needs the ontology file. Take it from your company's ontology
repository, at a commit you pin. It never comes from continuo.

**A dbt project.** Install `continuo-contracts` in a Python 3.14 environment of
its own, at the version your install's chart pins (see the
[CHANGELOG](../deploy/continuo/CHANGELOG.md)), and add `--ontology` to the check
of [the dbt project](contracts.md#check-a-dbt-project-in-ci):

```bash
continuo-contracts dbt check services/shop --project shop-dbt \
  --ontology <ontology file> --dbt .venv-dbt/bin/dbt
```

**A Python project.** Install `continuo-python-runtime`, at the version your
install uses (see [the CI steps](contracts.md#the-ci-steps)), and add
`--project` and `--ontology` to `validate`:

```bash
continuo-runtime validate contracts --dialect postgres \
  --project shop-py --ontology <ontology file>
```

`continuo-runtime validate` needs `--project` whenever it gets `--ontology`: it is
the name your project has in the ontology. The dbt check takes the name of the
project folder, with `_` written `-`, unless you pass `--project`.

The two commands print each finding at its level, and exit with 1 when one is an
error. A warning does not fail them. A file with an error of its own stops the
check: they print one error line and read no contract. Check the file itself with
`continuo-contracts ontology check`.

## Where to read the result

- **The release.** `GET /api/v1/releases/<id>` shows a rejection. The dashboard
  lists each failed node with the rule that found the problem.
- **The contract bundle.** The bundle of a release,
  `tenants/<tenant>/topologies/<release_id>/contracts.json.gz`, holds the
  `ontology` value. It is `null` with the switch off. Otherwise it names the
  ontology the release used (`project`, `release_id` and `sha256`, which are
  `null` when none is released), lists the projects the ontology declares
  (`declared_projects`), and lists every warning and information finding of the
  check as `findings`. Each finding has a `severity`, a `rule`, a `subject`, a
  `message` and a `contract_id`.
- **The release log.** The line of a release that passed has `ontology_warnings`
  and `ontology_information`, the number of findings of each kind, when there are
  some.
- **The data contract.** `continuo node data-contract <unique_id>` prints the published
  contract. Its `provenance` property holds `ontologyVersion`, the ontology the
  version was made against, and its links name the things. The same file is in
  [the live folder](contracts.md#the-live-folder).
- **Metrics.** The `ontology` stage of
  `continuo_topology_stage_duration_seconds` times the reading of the file and
  the check, and `continuo_topology_contract_failures_total` counts each error
  with the stage `ontology` and the rule as the reason
  ([deploy/OBSERVABILITY.md](../deploy/OBSERVABILITY.md)).
