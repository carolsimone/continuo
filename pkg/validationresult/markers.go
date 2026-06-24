// Package validationresult holds the Go side of the structured validation-result
// contract. The validation pod (Python, dbt/base/validation_result.py) prints the
// per-node result block framed by these sentinel markers; k8s-controller splits
// the pod log on them to extract the structured JSON it uploads as
// run_results_uri. These constants are the single Go source of truth — the
// cross-language guard in markers_test.go binds them to the Python source so the
// two sides cannot drift silently.
package validationresult

const (
	SentinelBegin = "===CONTINUO_VALIDATION_RESULT_BEGIN==="
	SentinelEnd   = "===CONTINUO_VALIDATION_RESULT_END==="
)
