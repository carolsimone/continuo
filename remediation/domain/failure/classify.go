package failure

import (
	"strings"
	"unicode/utf8"
)

// Classification is the result of triaging one failed node.
type Classification struct {
	Category  Category
	Signature string
	Decision  Decision
	Reason    string // matched rule, e.g. "infra:connection_refused" or "logic:missing_relation"
	// Excerpt is the raw key error line the signature was derived from, kept
	// for the precedent case base. Capped at maxExcerptBytes; empty when no
	// log text exists.
	Excerpt string
}

// maxExcerptBytes bounds the excerpt carried on the trigger event: it is one
// log line kept as precedent evidence, not a transport for whole logs.
const maxExcerptBytes = 4 * 1024

// excerptOf caps the key error line on a rune boundary.
func excerptOf(s string) string {
	if len(s) <= maxExcerptBytes {
		return s
	}
	cut := maxExcerptBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// rule pairs a lowercase substring matcher with the reason it records.
type rule struct {
	needle string
	reason string
}

// Hard-drop infra signals. Only unambiguous infrastructure signals are dropped
// (under-drop policy). First match wins.
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
			Excerpt:   "",
		}
	}
	lower := strings.ToLower(logText)
	line := keyErrorLine(logText)

	if r, ok := firstMatch(lower, infraRules); ok {
		return Classification{Category: CategoryInfraTransient,
			Signature: NormalizeSignature(CategoryInfraTransient, line),
			Decision:  DecisionDrop, Reason: r.reason, Excerpt: excerptOf(line)}
	}
	if r, ok := firstMatch(lower, testRules); ok {
		return Classification{Category: CategoryTest,
			Signature: NormalizeSignature(CategoryTest, line),
			Decision:  DecisionEmit, Reason: r.reason, Excerpt: excerptOf(line)}
	}
	if r, ok := firstMatch(lower, logicRules); ok {
		return Classification{Category: CategoryLogic,
			Signature: NormalizeSignature(CategoryLogic, line),
			Decision:  DecisionEmit, Reason: r.reason, Excerpt: excerptOf(line)}
	}
	return Classification{Category: CategoryUnknown,
		Signature: NormalizeSignature(CategoryUnknown, line),
		Decision:  DecisionEmit, Reason: "unknown:unmatched", Excerpt: excerptOf(line)}
}

// ClassifyWithStructured prefers the structured validation result when present:
// a dbt "fail" status is a test assertion (deterministic, no heuristic); an
// "error" status routes the structured message through the same infra/logic
// rules used for the text log, but against the exact error message rather than
// scraped console output. When structured is nil (no run_results_uri, or the
// fetch/parse failed) — or the structured record carries no message — it degrades
// to the text-log Classify path unchanged.
func ClassifyWithStructured(ev FailureEvidence, structured *StructuredResult, logText string) Classification {
	if structured == nil {
		return Classify(ev, logText)
	}
	msg := strings.TrimSpace(structured.Message)
	if structured.Status == "fail" {
		return Classification{Category: CategoryTest,
			Signature: NormalizeSignature(CategoryTest, msg),
			Decision:  DecisionEmit, Reason: "test:status_fail", Excerpt: excerptOf(msg)}
	}
	if msg == "" {
		// Structured record present but no message — fall back to the text log
		// rather than emitting a contentless unknown.
		return Classify(ev, logText)
	}
	lower := strings.ToLower(msg)
	if r, ok := firstMatch(lower, infraRules); ok {
		return Classification{Category: CategoryInfraTransient,
			Signature: NormalizeSignature(CategoryInfraTransient, msg),
			Decision:  DecisionDrop, Reason: r.reason, Excerpt: excerptOf(msg)}
	}
	if r, ok := firstMatch(lower, logicRules); ok {
		return Classification{Category: CategoryLogic,
			Signature: NormalizeSignature(CategoryLogic, msg),
			Decision:  DecisionEmit, Reason: r.reason, Excerpt: excerptOf(msg)}
	}
	return Classification{Category: CategoryUnknown,
		Signature: NormalizeSignature(CategoryUnknown, msg),
		Decision:  DecisionEmit, Reason: "unknown:unmatched", Excerpt: excerptOf(msg)}
}

// ClassifyDuplicateTable classifies a duplicate-relation rejection from the
// evidence alone. It reads no log: the rejection happens at parse time, before
// any Job runs, so there is none, and the log-driven rules would label a
// naming conflict unknown:log_unavailable. The signature keys on the
// contested relation, not the release and not the target claimant's own
// node id: the target flips between releases (Target prefers whichever
// service the release actually changed), so keying on node id would fork one
// physical collision into two signatures the moment the changed service
// alternates. RelationID falls back to NodeID when empty (a trigger from
// before the field existed), which keeps this degenerate-but-safe for the
// duration of a rollout.
func ClassifyDuplicateTable(ev FailureEvidence) Classification {
	relationID := ev.RelationID
	if relationID == "" {
		relationID = ev.NodeID
	}
	return Classification{
		Category:  CategoryLogic,
		Signature: NormalizeSignature(CategoryLogic, "duplicate_table "+relationID),
		Decision:  DecisionEmit,
		Reason:    "logic:duplicate_table",
		Excerpt:   excerptOf("multiple nodes produce the relation " + relationID),
	}
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
