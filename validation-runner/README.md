# continuo-validation-runner

The slim, continuo-owned validation harness. It dispatches on `VALIDATION_OP`
(`build_from_sql` / `clone_from_prod` / `ensure_schema` / `drop_schema`), fetches
a node's candidate SQL from S3 when relevant, discovers the single installed
engine adapter (entry-point group `continuo_validation.adapters`), runs the
corresponding warehouse DDL through it, and prints exactly one sentinel-framed
result block on stdout.

It depends on `continuo-validation-contract` (the port + result-block) and, at
image-build time, on exactly one `continuo-validation-<engine>` library —
`Dockerfile.postgres` bakes in `continuo-validation-postgres`. The container
entrypoint is `python /validation_runner.py`, which continuo's executor invokes.

## Job, not a service

This is not one of continuo's long-running services (`state`, `orchestrator`,
`executor-controller`, etc. — see the [top-level README](../README.md)). It
ships as a container image (`continuo-validation-runner-<engine>`) that
`executor-controller` dispatches as a one-shot Kubernetes `Job`
(`BackoffLimit: 0`, `RestartPolicy: Never`) per node or per schema op. The
process reads `VALIDATION_OP`, runs it once, prints one result block, and
exits — there is no process to keep alive, no gRPC/HTTP surface, and no owned
datastore.

Its entire behavior is the four ops dispatched through the `WarehouseAdapter`
port (see [`continuo-validation-contract`](../validation-contract/README.md))
plus the sentinel result-block wire format. Because that surface is small and
fixed, this runner itself is very unlikely to need changes: adding support for
a new warehouse engine means adding a new `continuo-validation-<engine>`
adapter package and pointing `VALIDATION_IMAGE` at an image built from it, not
editing this code.
