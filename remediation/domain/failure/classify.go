package failure

import "strings"

// Classification is the result of triaging one failed node.
type Classification struct {
	Category  Category
	Signature string
	Decision  Decision
	Reason    string // matched rule, e.g. "infra:connection_refused" or "logic:missing_relation"
}

// rule pairs a lowercase substring matcher with the reason it records.
type rule struct {
	needle string
	reason string
}

// Hard-drop infra signals. Deliberately narrow: only unambiguous
// infrastructure failures are dropped (under-drop policy). Order does not
// matter — first match wins per group.
var infraRules = []rule{
	{"connection refused", "infra:connection_refused"},
	{"could not connect to database", "infra:could_not_connect"},
	{"oomkilled", "infra:oomkilled"},
	{"imagepullbackoff", "infra:image_pull"},
	{"back-off pulling image", "infra:image_pull"},
	{"invalidaccesskeyid", "infra:s3_credentials"},
	{"accessdenied", "infra:s3_credentials"},
}

// Test-node failures.
var testRules = []rule{
	{"failure in test", "test:assertion_failed"},
	{"configured to fail if", "test:assertion_failed"},
}

// Logic failures (the SQL/model itself is wrong).
var logicRules = []rule{
	{"does not exist", "logic:missing_object"},
	{"compilation error", "logic:compilation_error"},
	{"syntax error", "logic:syntax_error"},
	{"depends on a node named", "logic:missing_ref"},
	{"type mismatch", "logic:type_mismatch"},
	{"is ambiguous", "logic:ambiguous_column"},
}

// Classify deterministically sorts a failed node. Precedence: infra (drop) is
// checked first so a genuine infrastructure failure is never mistaken for a
// logic error; then test, then logic; anything else — including the ambiguous
// resource/permission class (statement timeout, permission denied, deadlock,
// out of memory) — falls through to unknown and is emitted. The classifier is
// pure and never errors; an empty log is classified unknown:log_unavailable.
func Classify(ev FailureEvidence, logText string) Classification {
	if strings.TrimSpace(logText) == "" {
		return Classification{
			Category:  CategoryUnknown,
			Signature: NormalizeSignature(CategoryUnknown, "log_unavailable"),
			Decision:  DecisionEmit,
			Reason:    "unknown:log_unavailable",
		}
	}
	lower := strings.ToLower(logText)
	line := keyErrorLine(logText)

	if r, ok := firstMatch(lower, infraRules); ok {
		return Classification{CategoryInfraTransient, NormalizeSignature(CategoryInfraTransient, line), DecisionDrop, r.reason}
	}
	if r, ok := firstMatch(lower, testRules); ok {
		return Classification{CategoryTest, NormalizeSignature(CategoryTest, line), DecisionEmit, r.reason}
	}
	if r, ok := firstMatch(lower, logicRules); ok {
		return Classification{CategoryLogic, NormalizeSignature(CategoryLogic, line), DecisionEmit, r.reason}
	}
	return Classification{CategoryUnknown, NormalizeSignature(CategoryUnknown, line), DecisionEmit, "unknown:unmatched"}
}

func firstMatch(lower string, rules []rule) (rule, bool) {
	for _, r := range rules {
		if strings.Contains(lower, r.needle) {
			return r, true
		}
	}
	return rule{}, false
}

// keyErrorLine extracts the most informative line for signature derivation:
// the first line mentioning "error" or "failure", else the first non-blank
// line. This keeps the signature focused on the cause, not the surrounding
// dbt boilerplate.
func keyErrorLine(logText string) string {
	var firstNonBlank string
	for _, ln := range strings.Split(logText, "\n") {
		t := strings.TrimSpace(ln)
		if t == "" {
			continue
		}
		if firstNonBlank == "" {
			firstNonBlank = t
		}
		l := strings.ToLower(t)
		if strings.Contains(l, "error") || strings.Contains(l, "failure") {
			return t
		}
	}
	return firstNonBlank
}
