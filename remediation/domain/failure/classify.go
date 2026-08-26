package failure

import (
	"regexp"
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

// ReasonShadowVerification is the reason recorded when a rejection is dropped
// because it came from a shadow release.
const ReasonShadowVerification = "shadow_verification"

// forShadow applies the one routing rule that depends on which kind of release
// was rejected rather than on what went wrong: a shadow release exists only to
// verify a proposed fix, so its rejection means the fix did not work.
// Remediating that would answer a failed fix attempt with another fix attempt,
// without bound, so the rejection is dropped. Everything that diagnoses the
// failure — category, signature, excerpt — is left exactly as classified, so
// the recorded decision still explains what went wrong; only the routing and
// the reason for it change. Every entry point applies this last, so no
// classification can escape the domain without it.
func (c Classification) forShadow(shadow bool) Classification {
	if !shadow {
		return c
	}
	c.Decision = DecisionDrop
	c.Reason = ReasonShadowVerification
	return c
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
// A rejection from a shadow release is dropped whatever the log says — see
// forShadow.
func Classify(ev FailureEvidence, logText string) Classification {
	return classifyLogText(logText).forShadow(ev.Shadow)
}

// classifyLogText applies the rule tables to a dbt log's text. It is the
// diagnosis alone: the shadow rule is applied by the exported entry points.
func classifyLogText(logText string) Classification {
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
// to the text-log Classify path unchanged. A rejection from a shadow release is
// dropped whatever the structured result says — see forShadow.
func ClassifyWithStructured(ev FailureEvidence, structured *StructuredResult, logText string) Classification {
	return classifyStructured(structured, logText).forShadow(ev.Shadow)
}

// classifyStructured is ClassifyWithStructured's diagnosis alone: the shadow
// rule is applied by the exported entry point.
func classifyStructured(structured *StructuredResult, logText string) Classification {
	if structured == nil {
		return classifyLogText(logText)
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
		return classifyLogText(logText)
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
// duration of a rollout. A rejection from a shadow release is dropped — see
// forShadow.
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
	}.forShadow(ev.Shadow)
}

func firstMatch(lower string, rules []rule) (rule, bool) {
	for _, r := range rules {
		if strings.Contains(lower, r.needle) {
			return r, true
		}
	}
	return rule{}, false
}

// keyErrorLine extracts the most informative text for signature derivation:
// the first line mentioning "error" or "failure", else the first non-blank
// line, with ANSI colour codes removed. This keeps the signature focused on
// the cause, not the surrounding dbt boilerplate.
//
// dbt reports every error under a lead-in line, `[ERROR]: Encountered an
// error:`, and prints the message on the lines that follow. That lead-in is
// the first line mentioning "error", but it says nothing about the failure —
// keyed on it, every error a node ever raises shares one signature. So when
// the error line is that lead-in, the key is the message block after it: its
// consecutive non-blank lines, joined with a space, up to messageBlockLines
// of them or until the next timestamped log line, whichever comes first. A
// log that ends at the lead-in (the Job died before dbt printed the message)
// keys on the lead-in itself, so the signature is never empty.
func keyErrorLine(logText string) string {
	lines := strings.Split(logText, "\n")
	var firstNonBlank string
	for i, ln := range lines {
		t := strings.TrimSpace(reANSI.ReplaceAllString(ln, ""))
		if t == "" {
			continue
		}
		if firstNonBlank == "" {
			firstNonBlank = t
		}
		l := strings.ToLower(t)
		if !strings.Contains(l, "error") && !strings.Contains(l, "failure") {
			continue
		}
		if strings.HasSuffix(l, errorLeadIn) {
			if block := messageBlock(lines[i+1:]); block != "" {
				return block
			}
		}
		return t
	}
	return firstNonBlank
}

// errorLeadIn is the line dbt prints before every error message, lower-cased.
const errorLeadIn = "encountered an error:"

// messageBlockLines bounds how many lines of an error message feed the key:
// enough to carry the error kind, the affected node, and the detail dbt puts
// on the following line, without dragging in a rendered source excerpt.
const messageBlockLines = 3

// messageBlock joins the message lines that follow dbt's error lead-in:
// consecutive non-blank, non-timestamped lines, at most messageBlockLines of
// them. It returns "" when the lead-in has nothing after it.
func messageBlock(lines []string) string {
	var parts []string
	for _, ln := range lines {
		t := strings.TrimSpace(reANSI.ReplaceAllString(ln, ""))
		if t == "" || reLogLineStart.MatchString(t) {
			break
		}
		parts = append(parts, t)
		if len(parts) == messageBlockLines {
			break
		}
	}
	return strings.Join(parts, " ")
}

var (
	// reANSI matches the terminal colour codes dbt wraps its log levels in.
	reANSI = regexp.MustCompile("\x1b\\[[0-9;]*m")
	// reLogLineStart matches the clock-time prefix every dbt log line starts
	// with (`13:28:04  ...` on stdout, `13:28:04.340090 [error] ...` in the
	// file log), which is what separates a message from the log line after it.
	reLogLineStart = regexp.MustCompile(`^\d{2}:\d{2}:\d{2}`)
)
