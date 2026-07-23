package validationresult

import (
	"testing"
)

// TestSentinelMarkersMatchWireContract guards the cross-repo structured-result
// contract. The validation pod (the external continuo-validation-contract package,
// continuo_validation_contract/result.py) prints the result block framed by these
// sentinel markers, and the Go side splits the pod log on the constants in this
// package. Nothing generates one side from the other, so a drift would silently
// empty run_results_uri and degrade remediation to the text-log path with no error
// surfaced. The marker strings are the fixed wire contract; this test pins the Go
// constants to their literal values so any local edit fails CI immediately.
func TestSentinelMarkersMatchWireContract(t *testing.T) {
	if SentinelBegin != "===CONTINUO_VALIDATION_RESULT_BEGIN===" {
		t.Errorf("sentinel BEGIN drift: go %q != wire contract", SentinelBegin)
	}
	if SentinelEnd != "===CONTINUO_VALIDATION_RESULT_END===" {
		t.Errorf("sentinel END drift: go %q != wire contract", SentinelEnd)
	}
}
