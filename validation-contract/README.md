# continuo-validation-contract

The contract shared by the continuo validation runner and every engine adapter
library: the `WarehouseAdapter` port (engine-specific empty-table DDL) plus
entry-point discovery, and the sentinel-framed result-block wire format the
runner emits and the k8s-controller parses. Each `continuo-validation-<engine>`
library depends on this package and implements the port.
