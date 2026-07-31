# continuo-validation-contract

The contract shared by the continuo validation runner and every engine adapter
library: the `ValidationAdapter` port (engine-specific empty-table DDL) plus
entry-point discovery, and the sentinel-framed result-block wire format the
runner emits and the k8s-controller parses. Each `continuo-validation-<engine>`
library depends on this package and implements the port.

## Job, not a service

This package has no runtime of its own — it isn't a service and it isn't the
Job either. It's the interface both sides of the validation Job agree on:
[`continuo-validation-runner`](../validation-runner/README.md) (the Job that
calls it) and every `continuo-validation-<engine>` adapter package (which
implements it). The whole contract is the seven `ValidationAdapter` methods
(`required_env`, `from_env`, `ensure_schema`, `drop_schema`,
`build_empty_from_sql`, `clone_empty_from_prod`, `close`) plus the sentinel
result-block format `discover_adapter` resolves against at import time.

Because the port *is* the contract, this package is very unlikely to need
changes — new warehouse support is a new adapter package implementing the
existing methods, not a change here. Touching this file means bumping a
version that every adapter package (in the separate `continuo-validation-runners`
repo) and the runner both pin, so treat it as a deliberate, coordinated change.
