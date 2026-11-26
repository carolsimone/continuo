# Reading releases from continuo

External tools can read what continuo knows about a release: which release is in
production, the code of each node, the topology, and the data contracts. This
page declares what a reader may rely on, which version of each document that holds for, and what a
version bump means. It is the whole promise: anything not named here may change
in any release.

## Find the release

`GET /api/v1/current-prod` answers with the production pointer. The token is the
one the release routes use ([deploy/AUTH.md](../deploy/AUTH.md)); a repository
bound in `ciAuth.bindings` may call it.

```json
{"current_prod_release_id": "rel-2026-11-06-0001", "node_count": 42, "updated_at": "2026-11-06T09:12:45Z"}
```

The read set is `current_prod_release_id`, a string. `node_count` and
`updated_at` are present but are not part of it. The route is versioned by its
path: a change that removes, renames or retypes `current_prod_release_id` is a
new route, `/api/v2/current-prod`.

## The three documents

All three live in the install's bucket (`s3.bucket`, `continuo` on the bundled
MinIO). `<release>` is the id from the previous step, used verbatim. `<tenant>`
is `default`: releases are submitted under the default tenant.

| Document | Key | Written by | Version key |
|---|---|---|---|
| Code bundle | `code-bundles/<release>/bundle.json` | topology-controller | `contract_version` |
| Topology artifact | `tenants/<tenant>/topologies/<release>/topology.json.gz` | topology-controller | `schema_version` |
| Contract bundle | `tenants/<tenant>/topologies/<release>/contracts.json.gz` | topology-controller | `schema_version` |

The code bundle is JSON. The topology artifact and the contract bundle are JSON
compressed with gzip and stored as they are, with no `Content-Encoding`, so a
client decompresses them itself. All three objects are immutable: a release's
documents never change after they are written, so reading them for one release
id always gives a consistent set.

Every release that passes writes a contract bundle. It holds the contract of
every dbt model, seed and snapshot the release keeps and of every Python
node, and `summary.by_family` counts them by family. It is empty when the
release has none. The bundle is canonical JSON (RFC 8785).
`release.parsed:v1` and `release.promoted:v1` carry the SHA-256 of
its stored bytes, so a reader can check what it fetched.

The three versions are integers, count independently of each other and of the
chart's version, and are all `1`.

## Read set, version 1

A reader may rely on these keys, with these types, and on nothing else.

**Code bundle, `contract_version` 1**

| Path | Type | Meaning |
|---|---|---|
| `contract_version` | number | The version of this document. |
| `release_id` | string | The release the bundle belongs to. |
| `nodes` | object | One entry per node, keyed by the node's unique id: `<schema>.<table>`, lowercase. |
| `nodes.<id>.runtime` | string | `dbt` or `python`. A reader skips a runtime it does not know: a new runtime is an added value, not a new version. |
| `nodes.<id>.raw_code` | string | The node's own source before compilation. For a `python` node it is JSON text: the node's contract entry. |
| `nodes.<id>.compiled_code` | string | A `dbt` node's compiled SQL. Empty for a seed and for a `python` node. |
| `nodes.<id>.config.meta.owner` | string | A `dbt` node's owner, when the model declares one. |

For a `python` node, the keys of the JSON in `raw_code` that are in the read set
are `kind` (string), `owner` (string) and `reads` (object: each read's name to
the `SELECT` text it runs). A node of `kind` `python-csv` has one read, `csv`,
which is a file location, not SQL. Every other key of the entry may change.

A Python node's entry comes from its ODCS file (a project is released with
the kind `python`, one ODCS file per node): `owner` is the contract's `team.name`
and `reads` holds its `readQueries`, or its `csvSource` as the read `csv`.

**Topology artifact, `schema_version` 1**

| Path | Type | Meaning |
|---|---|---|
| `schema_version` | number | The version of this document. |
| `tenant_id`, `release_id` | string | The tenant and release it belongs to. |
| `nodes` | array | One entry per node. |
| `nodes[].unique_id` | string | The key the bundle uses for the same node. |
| `nodes[].resolved_relation_id` | string | The warehouse relation the node writes, `<schema>.<relation>`. For a dbt model with an alias it differs from `unique_id`. |
| `nodes[].project_name` | string | The project that owns the node. |

**Contract bundle, `schema_version` 1**

| Path | Type | Meaning |
|---|---|---|
| `schema_version` | number | The version of this document. |
| `tenant_id`, `release_id` | string | The tenant and release it belongs to. |
| `continuo_contracts` | string | The version of the `continuo-contracts` library that read the release's contracts. |
| `contracts` | object | One entry per node that has a contract, keyed by the node's unique id: `<schema>.<table>`, lowercase. |
| `contracts.<id>.family` | string | `dbt` or `python`. A reader skips a family it does not know: a new family is an added value, not a new version. |
| `contracts.<id>.project` | string | The project that owns the node. |
| `contracts.<id>.contract_hash` | string | `sha256:` and 64 hex characters. A reader compares this value. How it is computed is below the table. |
| `contracts.<id>.content_hash` | string | The node's code fingerprint, `sha256:` and 64 hex characters. |
| `contracts.<id>.source.path` | string | The file the contract stands for, as a path in the project's repository: a `python` contract's file, or the file of the `dbt` node. |
| `contracts.<id>.source.sha256` | string | A `python` contract: the SHA-256 of its file's text. A `dbt` contract: the SHA-256 of the manifest fields it was generated from. |
| `contracts.<id>.document` | object | The stored form of the contract, as a whole ODCS document with every root key. Its `status` is `active` and its `version` is `"0"`. The version number a node's contract has is in catalog and in the [live folder](contracts.md#the-live-folder). |

`contract_hash` is the SHA-256 of the canonical JSON (RFC 8785) of `document`
without its four root keys `apiVersion`, `kind`, `status` and `version`. A
reader that recomputes it removes those keys first: the hash of the whole
`document` is a different value.

The keys `odcs_schema_sha256`, `ontology`, `provenance`, `summary`, `failures`
and `timings` are in the document but not in the read set. They may change in any
release.

The `ontology` value is `null` when the install's ontology switch is off, and
then no contract's `document` holds a binding or an ontology link. With the
switch on it is an object with the keys below. The `ontology` key is outside the
read set, so this shape is just as unstable: it may change in any release.

| Key | Type | Meaning |
|---|---|---|
| `enabled` | boolean | `true`. |
| `project`, `release_id`, `sha256` | string or `null` | The ontology the release used: the project that released the file, the release that wrote it and the SHA-256 of its text. Each is `null` when no ontology is released. |
| `declared_projects` | array of strings | The projects the ontology declares, sorted. Empty when no ontology is released. |
| `findings` | array of objects | Every warning and information finding of the ontology check, sorted by severity, rule, subject and message. Each has `severity`, `rule`, `subject`, `message` and `contract_id` (`null` when the finding is not about one contract). An error is not here: it rejects the release. |

With the switch on and an ontology released, the `document` of a contract of a
project the ontology declares is filled in from it, so its `contract_hash` is not
the hash the same contract has with the switch off. [The company
ontology](ontology.md) says what is filled in.

## What changes a version

- **A key of the read set is removed, renamed, retyped, or given another
  meaning:** the document's version is raised. Nothing else raises it.
- **A key is added, or a key outside the read set changes:** the version stays.
  A reader ignores keys it does not know.
- **When a version is raised:** the change is listed under Breaking in the
  [chart changelog](../deploy/continuo/CHANGELOG.md) and in the release notes.
  continuo writes the version of its own build, so a release submitted after the
  upgrade carries the new version, and a release written before keeps the old
  one, because its objects are immutable. A reader refuses a version it does not
  know instead of reading it partly.

## How long the objects stay

- The bundle, the topology artifact and the contract bundle of the release
  `current_prod_release_id` names are never deleted while it is the production
  release. The same holds for a release that is still a project's production
  release.
- release-controller deletes the bundle of any other release when its retention
  prunes the release: 90 days after the release was submitted
  (`RELEASE_RETENTION_DAYS`). It deletes the topology artifact and the contract
  bundle of a release that was not promoted then too, and keeps both of a
  promoted one.
- With your own bucket, set no lifecycle expiry on `code-bundles/` or `tenants/`.
  An age rule cannot tell the live release from an old one. The local
  docker-compose bucket sets none, and `scripts/check-read-retention.sh` fails
  one that is added.
- A reader needs `s3:GetObject` on the two prefixes, and `s3:ListBucket` on the
  bucket if it wants a missing object reported as 404 rather than 403.

## What enforces this

| Promise | Enforced by |
|---|---|
| The bundle's read set and version | `topology-controller/tests/test_code_bundle_contract.py` writes a bundle and compares it with a fixture. `pkg/codebundle/read_contract_test.go` checks each path and type on a copy of that fixture, and `pkg/streams` keeps the two copies identical. |
| The topology's read set and version | `pkg/topologyartifact/read_contract_test.go` checks each path and type on the golden fixture that both writers are pinned to. |
| The contract bundle's read set and version | `topology-controller/tests/test_contract_bundle.py` writes a bundle and compares it with a fixture. `pkg/contractbundle/contractbundle_test.go` reads a copy of that fixture and checks each value, and `pkg/streams` keeps the two copies identical. |
| Readers ignore unknown keys and refuse a newer version | The same tests, in `Decode` of each package. |
| The pointer's key | `tests/e2e/public_api_test.go` reads `current_prod_release_id` through the public route. |
| The live release's objects are kept | `TestPruneFinishedRuns_PassesCutoffAndKeepIDs` (the production release is never pruned) and `TestRunRepository_DeleteFinishedBefore_DeletesCandidateSQLPrefixes` (a kept release's prefixes are never deleted). |
| No expiry rule on the two prefixes | `scripts/check-read-retention.sh`, run by `make guards`. |
