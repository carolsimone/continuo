// Package validationresult holds the Go side of the structured validation-result
// contract. The validation pod (Python, the external continuo-validation-contract
// package's continuo_validation_contract/result.py) prints the per-node result block
// framed by these sentinel markers; k8s-controller splits the pod log on them to
// extract the structured JSON it uploads as run_results_uri. These constants are the
// single Go source of truth — the cross-repo guard in markers_test.go pins them to
// the literal wire strings so the two sides cannot drift silently.
package validationresult

const (
	SentinelBegin = "===CONTINUO_VALIDATION_RESULT_BEGIN==="
	SentinelEnd   = "===CONTINUO_VALIDATION_RESULT_END==="
)
