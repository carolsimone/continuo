# Loading releases into continuo

## Overview

"Loading a release" is how an external dbt producer ships a dbt model change
into continuo's blue/green pipeline. A release is a single, named delta for one
dbt service: a freshly built container image plus the service's compiled dbt
manifest. The producer publishes those two artifacts (image to a container
registry, manifest to object storage), then registers the release with continuo's
release-controller over HTTP. continuo validates the change against the current
production topology and either promotes it (the new manifest and image become
live) or rejects it. The producer never touches continuo's internal databases,
Redis streams, or Kubernetes — it interacts only through the registry, the
object-storage key convention, and the release-controller HTTP API documented
here.

## Audience and scope

This document is for teams running their own dbt project ("producers") who want
their models executed by a continuo deployment. It describes continuo's **public
integration surface only**:

1. The container-image naming contract.
2. The canonical object-storage (S3-compatible) manifest layout.
3. The release-controller HTTP API.

It deliberately says nothing about continuo's internal services, event streams,
or storage. Those are implementation details and may change; the three surfaces
above are the contract.

A complete, runnable reference implementation lives at
<https://github.com/carolsimone/continuo-dbt-demo>.

## The release model

A release is **single-service**. Each release carries exactly one changed
service and the image tag built for it:

```
{ service, release_id, image_tag }
```

The producer does **not** send the manifests or image tags of any other service.
continuo keeps a per-service production pointer for every service it already
knows about; when a release becomes active, the controller reconstructs the full
production manifest set by combining the changed service's newly uploaded
manifest with every other service's current production pointer. This means:

- You only build and push an image for the service you changed.
- You only compile and upload a manifest for the service you changed.
- Unchanged services keep running their current production images and manifests.

## Step 1 — build and push the service image

Build a container image for the changed dbt service and push it to your registry:

```
<DOCKERHUB_USERNAME>/<service>:<image_tag>
```

Naming contract:

- `<service>` MUST equal the dbt **project name** of the service and the
  service's folder name in your dbt repository. continuo uses this same value to
  key the manifest in object storage and to launch Kubernetes jobs, so the three
  must agree exactly.
- `<image_tag>` is any tag you choose, but it must be content-addressed and
  explicit (for example the commit SHA). continuo refuses to fall back to
  `latest` when launching jobs — an empty image tag is a permanent error.
- `<DOCKERHUB_USERNAME>` is the registry namespace the continuo deployment is
  configured to pull from (its `DOCKERHUB_USERNAME` setting). When continuo
  launches a dbt job it forms the image reference as
  `<DOCKERHUB_USERNAME>/<service>:<image_tag>`. Push to the same namespace.

You may also tag the image `:latest` for convenience, but continuo always
launches the exact `<image_tag>` you register in the release, never `:latest`.

Only the changed service needs a new image. Other services keep their current
production tags.

## Step 2 — compile, filter, and upload the manifest

### Compile

Run `dbt compile` for the changed service. Compilation needs a live Postgres
connection so dbt can resolve `ref()`/`source()` and render Jinja — but it reads
no data and runs no models. It produces `target/manifest.json`.

### Filter

Reduce `manifest.json` to the nodes continuo cares about. Keep only nodes whose
`resource_type` is `model` or `seed`, and drop any node tagged `local_stub`:

```python
manifest["nodes"] = {
    k: v
    for k, v in manifest["nodes"].items()
    if v.get("resource_type") in ("model", "seed")
    and "local_stub" not in v.get("tags", [])
}
```

`local_stub` is the convention for placeholder models that stand in for another
service's tables in your local dbt graph; they must not be published as part of
your service's topology.

### Upload to the canonical key

Upload the filtered `manifest.json` to your continuo deployment's
S3-compatible bucket at the **canonical key**:

```
s3://<bucket>/<service>/<release_id>/manifest.json
```

Rules:

- `<bucket>` is the object-storage bucket the continuo deployment reads from.
- `<service>` is the same value used for the image (project name = folder name).
- `<release_id>` namespaces the path; there is **no** version suffix and no other
  decoration.
- The image tag is **not** stored in object storage. It travels only in the HTTP
  request body (Step 3).
- There is **no** `service_metadata.json` sidecar. Upload exactly one object per
  release: the filtered `manifest.json`.
- Upload only the changed service's manifest.

This key convention is owned, on continuo's side, by a single function
(`CanonicalManifestKey`); the controller derives the key it reads from
`<bucket>`, `<service>`, and `<release_id>` itself. Producing the wrong key means
the controller cannot find your manifest.

## Step 3 — drive the release through the HTTP API

The release-controller exposes a small REST API. All bodies are JSON.

### Connectivity

The API is internal to the continuo cluster: it is a `ClusterIP` service
listening on port **8088**, and the continuo reference deployment does not yet
publish it on a public domain. Today you reach it by tunnelling to the server
and port-forwarding the service, for example:

```bash
ssh <continuo-server> \
  "kubectl -n continuo port-forward svc/release-controller 8088:8088"
```

then issue requests against `http://localhost:8088`. The API is designed to sit
behind an authenticating gateway later; the route shapes below will not change
when it does.

### Endpoint reference

#### `GET /healthz`

Liveness probe.

- Request: none.
- Response: `200 OK`, empty body.

#### `GET /current-prod`

Returns the current production pointer. Use it before a release to decide whether
this is the first push (bootstrap) or a normal validated release.

- Request: none.
- Response: `200 OK`, JSON:

  ```json
  {
    "current_prod_release_id": "rel-2026-06-01",
    "node_count": 21,
    "updated_at": "2026-06-01T10:00:00Z"
  }
  ```

  - `current_prod_release_id` (string): the promoted release currently live. An
    **empty string** means production has never been seeded — this run must
    bootstrap (see Bootstrap below).
  - `node_count` (number): nodes in the live topology snapshot.
  - `updated_at` (string, RFC 3339): when production last changed.

#### `POST /releases`

Register a new release candidate. Idempotent on `release_id`.

- Request body:

  | Field        | Type    | Required | Default | Meaning |
  |--------------|---------|----------|---------|---------|
  | `release_id` | string  | yes      | —       | Unique id for this release. Re-posting the same id is a no-op. |
  | `service`    | string  | yes      | —       | The changed dbt service (project name = folder name). |
  | `image_tag`  | string  | yes      | —       | The tag you pushed in Step 1. |
  | `bootstrap`  | boolean | no       | `false` | When `true`, promote without validation (see Bootstrap). |

  Example:

  ```json
  {
    "release_id": "rel-2026-06-13-001",
    "service": "shop",
    "image_tag": "a1b2c3d",
    "bootstrap": false
  }
  ```

- Response: `202 Accepted`, JSON:

  ```json
  { "release_id": "rel-2026-06-13-001", "status": "received" }
  ```

- Errors: `400 Bad Request` on invalid JSON, or when any of `release_id`,
  `service`, or `image_tag` is missing/empty. The error body is a plain-text
  message naming the offending field.

Accepting a release only enqueues it; promotion or rejection happens
asynchronously. Poll `GET /releases/{id}` for the outcome.

#### `GET /releases/{id}`

Full detail for one release. This is the endpoint you poll to a terminal status.

- Request: none.
- Response: `200 OK`, JSON:

  ```json
  {
    "release_id": "rel-2026-06-13-001",
    "status": "validating",
    "changed_service": "shop",
    "transitions": [{ "to": "received", "at": "..." }, { "to": "parsing", "at": "..." }],
    "validation_node_ids": ["model.shop.orders", "model.shop.line_items"],
    "reject_reason": "",
    "failing_nodes": [],
    "per_node_results": [
      { "node_id": "model.shop.orders", "status": "ok", "dbt_log_uri": "...", "duration_ms": 1200 }
    ],
    "image_tags": { "shop": "a1b2c3d" },
    "bootstrap": false
  }
  ```

  Key fields:

  - `status` (string): one of `received`, `parsing`, `validating`, `promoted`,
    `rejected`, `superseded`. The terminal statuses are **`promoted`** (success)
    and **`rejected`** (failure).
  - `changed_service` (string): the single service this release changed.
  - `transitions` (array of `{to, at}`): the status history.
  - `validation_node_ids` (array of string): nodes selected for validation.
  - `reject_reason` (string): populated when `status` is `rejected`.
  - `failing_nodes` (array of string): nodes that failed validation.
  - `per_node_results` (array of `{node_id, status, dbt_log_uri, duration_ms}`):
    per-node validation outcomes.
  - `image_tags` (object): the per-service image tags continuo assembled for this
    release.
  - `bootstrap` (boolean): whether this release skipped validation.

- Errors: `404 Not Found` if `release_id` is unknown.

#### `GET /releases`

Paginated release history, newest first.

- Query parameters (all optional):
  - `status`: exact-match filter (e.g. `promoted`).
  - `limit`: page size; unparseable, non-positive, or over-limit values fall back
    to the server default.
  - `cursor`: opaque keyset cursor from a previous response's `next_cursor`.
- Response: `200 OK`, JSON:

  ```json
  {
    "releases": [
      {
        "release_id": "rel-...",
        "status": "promoted",
        "created_at": "2026-06-13T09:00:00Z",
        "resolved_at": "2026-06-13T09:03:00Z",
        "node_count": 5,
        "bootstrap": false,
        "reject_reason": ""
      }
    ],
    "next_cursor": ""
  }
  ```

  `resolved_at` is `null` until the release reaches a terminal status.
  `next_cursor` is an empty string when there are no further pages.

- Errors: `400 Bad Request` on a malformed `cursor`.

### Bootstrap: detection and semantics

A normal release is validated against the current production topology. The first
release into an **empty** production has nothing to validate against — every
cross-service upstream looks "new" relative to empty production — so a normal
release would be rejected. Bootstrap is the escape hatch for that first push (or
a deliberate, trusted re-baseline).

Detection:

1. Call `GET /current-prod`.
2. If `current_prod_release_id` is the empty string, set `bootstrap: true` in
   your `POST /releases` body.
3. Otherwise set `bootstrap: false`.

Semantics:

- `bootstrap: true` promotes the release **without validation**. continuo
  records whatever topology the release carries as the new production base.
  Because it is unvalidated, the first push must be trusted.
- Every subsequent release uses `bootstrap: false` and goes through validation.

### Polling to a terminal status

After `POST /releases` returns `202`, poll `GET /releases/{id}` on an interval
until `status` is `promoted` or `rejected`:

- `promoted`: the change is live. The new manifest and image are now production;
  `current_prod_release_id` will reflect this release.
- `rejected`: the change did not promote. Read `reject_reason` and
  `failing_nodes`/`per_node_results` to diagnose. Production is unchanged.

## End-to-end worked sequences

### Normal release (production already seeded)

1. `GET /current-prod` → `current_prod_release_id` is non-empty ⇒ normal release.
2. Build and push `<DOCKERHUB_USERNAME>/<service>:<image_tag>`.
3. `dbt compile` the changed service.
4. Filter `target/manifest.json` (keep models + seeds, drop `local_stub`).
5. Upload to `s3://<bucket>/<service>/<release_id>/manifest.json`.
6. `POST /releases` with `{release_id, service, image_tag, bootstrap: false}` →
   `202`.
7. Poll `GET /releases/{release_id}` until `status` is `promoted` or `rejected`.

### First-run bootstrap (production never seeded)

1. `GET /current-prod` → `current_prod_release_id` is `""` ⇒ bootstrap.
2. Build and push the image, compile, filter, and upload the manifest exactly as
   in steps 2–5 above.
3. `POST /releases` with `{release_id, service, image_tag, bootstrap: true}` →
   `202`.
4. Poll `GET /releases/{release_id}` — a bootstrap release promotes without
   validation; expect `status: promoted`. Production is now seeded; all later
   releases use `bootstrap: false`.

## Contract invariants and guarantees

- **Single source of the manifest layout.** The canonical key is
  `s3://<bucket>/<service>/<release_id>/manifest.json`, owned on continuo's side
  by `CanonicalManifestKey`. The producer must write exactly this key.
- **The image tag travels in the POST body, not in object storage.** Object
  storage holds only the filtered `manifest.json`.
- **No sidecar.** There is no `service_metadata.json` (or any other) sidecar in
  the flow — one object per release.
- **Idempotent on `release_id`.** Re-posting the same `release_id` is a no-op;
  choose a fresh id per release.
- **Single changed service per release.** The body carries exactly one
  `{service, image_tag}`; continuo reconstructs the full manifest set from its
  own per-service production pointers and the canonical keys it derives.
- **Explicit, content-addressed image tags.** continuo never falls back to
  `latest`; an empty image tag is a permanent error.

## Reference implementation

A working producer that performs every step above —
build/push, compile/filter/upload, bootstrap detection, `POST /releases`, and
poll-to-terminal — is published at
<https://github.com/carolsimone/continuo-dbt-demo>.
