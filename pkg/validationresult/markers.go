// Package validationresult holds the Go side of the structured validation-result
// contract. The validation pod (Python, the external continuo-engine-contract
// package's continuo_engine_contract/result.py, published from
// github.com/carolsimone/continuo-python-runtime) prints the per-node result block
// framed by these sentinel markers; k8s-controller splits the pod log on them to
// extract the structured JSON it uploads as run_results_uri. These constants are the
// single Go source of truth — the cross-repo guard in markers_test.go pins them to
// the literal wire strings so the two sides cannot drift silently.
package validationresult

const (
	SentinelBegin = "===CONTINUO_VALIDATION_RESULT_BEGIN==="
	SentinelEnd   = "===CONTINUO_VALIDATION_RESULT_END==="
)

// SchemaVersion is the structured-result JSON contract's schema_version field,
// emitted by every call to continuo_validation_contract/result.py's
// result_block() (its SCHEMA_VERSION constant). k8s-controller checks a
// candidate's schema_version against this before trusting it as the contract's
// result block, rather than an unrelated status-bearing JSON object that
// happens to appear in the same log.
const SchemaVersion = 1
