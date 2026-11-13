package model

import pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"

// The reasons a result block names that map to a failure reason of the
// attempt that printed it.
const (
	// resultBlockReasonImageContractMismatch is the reason a python harness
	// names when the image it runs from does not match the content hash it
	// was dispatched with.
	resultBlockReasonImageContractMismatch = "image_contract_mismatch"
	// resultBlockReasonUnownedDependents is the reason a seed preflight names
	// when an object reads the seed outside the views the run rebuilds and
	// the objects an operator named.
	resultBlockReasonUnownedDependents = "unowned_dependents"
	// resultBlockReasonEngineMismatch is the reason a seed preflight names
	// when the runner's engine adapter declares another drop family than the
	// one it was asked to check for.
	resultBlockReasonEngineMismatch = "engine_mismatch"
	// resultBlockReasonWarehouseUnreachable is the reason a seed preflight
	// names when it cannot connect to the warehouse.
	resultBlockReasonWarehouseUnreachable = "warehouse_unreachable"
)

// ReasonFromResultBlock maps the reason a result block names to the failure
// reason of the attempt that printed it: image_contract_mismatch from a python
// harness, and unowned_dependents, engine_mismatch and warehouse_unreachable
// from a seed preflight. A block that names no reason, or one this service
// does not know, maps to "" and leaves the attempt to be classified from what
// Kubernetes shows of it.
func ReasonFromResultBlock(reason string) pkgmodel.FailureReason {
	switch reason {
	case resultBlockReasonImageContractMismatch:
		return pkgmodel.FailureReasonImageContractMismatch
	case resultBlockReasonUnownedDependents:
		return pkgmodel.FailureReasonUnownedDependents
	case resultBlockReasonEngineMismatch:
		return pkgmodel.FailureReasonEngineMismatch
	case resultBlockReasonWarehouseUnreachable:
		return pkgmodel.FailureReasonWarehouseUnreachable
	default:
		return ""
	}
}
