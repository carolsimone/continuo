package validationresult

import (
	"testing"
)

// TestSentinelMarkersMatchWireContract guards the cross-repo structured-result
// contract. The validation pod (the external continuo-engine-contract package,
// continuo_engine_contract/result.py, published from
// github.com/carolsimone/continuo-python-runtime) prints the result block framed by
// these sentinel markers, and the Go side splits the pod log on the constants in this
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

// TestSchemaVersionMatchesWireContract pins SchemaVersion to its current wire
// value (1, matching continuo_engine_contract/result.py's SCHEMA_VERSION
// as of this writing). Nothing generates one side from the other, so this
// only catches a local edit to the constant — it cannot detect the contract
// itself bumping its schema_version, which would need a corresponding change
// here. Parse uses SchemaVersion to tell the contract's result block apart
// from an unrelated status-bearing JSON object that happens to appear in the
// same pod log; a silent drift here would make that check reject every real
// block.
func TestSchemaVersionMatchesWireContract(t *testing.T) {
	if SchemaVersion != 1 {
		t.Errorf("schema_version drift: go %d != wire contract's SCHEMA_VERSION", SchemaVersion)
	}
}
