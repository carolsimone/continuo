# continuo-validation-runner

The slim, continuo-owned validation harness. It dispatches on `VALIDATION_OP`
(`build_from_sql` / `clone_from_prod`), fetches a node's candidate SQL from S3,
discovers the single installed engine adapter (entry-point group
`continuo_validation.adapters`), runs the empty-table DDL through it, and prints
exactly one sentinel-framed result block on stdout.

It depends on `continuo-validation-contract` (the port + result-block) and, at
image-build time, on exactly one `continuo-validation-<engine>` library —
`Dockerfile.postgres` bakes in `continuo-validation-postgres`. The container
entrypoint is `python /validation_runner.py`, which continuo's executor invokes.
