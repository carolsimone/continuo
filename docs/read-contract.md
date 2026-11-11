# Reading releases from continuo

External tools can read what continuo knows about a release: which release is in
production, the code of each node, and the topology. This page declares what a
reader may rely on, which version of each document that holds for, and what a
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

## The two documents

Both live in the install's bucket (`s3.bucket`, `continuo` on the bundled
MinIO). `<release>` is the id from the previous step, used verbatim. `<tenant>`
is `default`: releases are submitted under the default tenant.

| Document | Key | Written by | Version key |
|---|---|---|---|
| Code bundle | `code-bundles/<release>/bundle.json` | topology-controller | `contract_version` |
| Topology artifact | `tenants/<tenant>/topologies/<release>/topology.json.gz` | topology-controller | `schema_version` |

The bundle is JSON. The topology artifact is JSON compressed with gzip and
stored as it is, with no `Content-Encoding`, so a client decompresses it itself.
Both objects are immutable: a release's two documents never change after they
are written, so reading both for one release id always gives a consistent pair.

The two versions are integers, count independently of each other and of the
chart's version, and are both `1`.

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

**Topology artifact, `schema_version` 1**

| Path | Type | Meaning |
|---|---|---|
| `schema_version` | number | The version of this document. |
| `tenant_id`, `release_id` | string | The tenant and release it belongs to. |
| `nodes` | array | One entry per node. |
| `nodes[].unique_id` | string | The key the bundle uses for the same node. |
| `nodes[].resolved_relation_id` | string | The warehouse relation the node writes, `<schema>.<relation>`. For a dbt model with an alias it differs from `unique_id`. |
| `nodes[].service_name` | string | The service that owns the node. |

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

- The bundle and the topology artifact of the release `current_prod_release_id`
  names are never deleted while it is the production release. The same holds for
  a release that is still a service's production release.
- release-controller deletes the bundle of any other release when its retention
  prunes the release: 90 days after the release was submitted
  (`RELEASE_RETENTION_DAYS`). It deletes the topology artifact of a release that
  was not promoted then too, and keeps the topology artifact of a promoted one.
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
| Readers ignore unknown keys and refuse a newer version | The same tests, in `Decode`. |
| The pointer's key | `tests/e2e/public_api_test.go` reads `current_prod_release_id` through the public route. |
| The live release's objects are kept | `TestPruneFinishedRuns_PassesCutoffAndKeepIDs` (the production release is never pruned) and `TestRunRepository_DeleteFinishedBefore_DeletesCandidateSQLPrefixes` (a kept release's prefixes are never deleted). |
| No expiry rule on the two prefixes | `scripts/check-read-retention.sh`, run by `make guards`. |
