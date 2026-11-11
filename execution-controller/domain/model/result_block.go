package model

import pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"

// resultBlockReasonImageContractMismatch is the reason a python harness names
// in its result block when the image it runs from does not match the content
// hash it was dispatched with.
const resultBlockReasonImageContractMismatch = "image_contract_mismatch"

// ReasonFromResultBlock maps the reason a result block names to the failure
// reason of the attempt that printed it. A block that names no reason, or one
// this service does not know, maps to "" and leaves the attempt to be
// classified from what Kubernetes shows of it.
func ReasonFromResultBlock(reason string) pkgmodel.FailureReason {
	switch reason {
	case resultBlockReasonImageContractMismatch:
		return pkgmodel.FailureReasonImageContractMismatch
	default:
		return ""
	}
}
