package sanitizer

import (
	"regexp"
	"strings"

	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// Level selects how aggressively the dbt log is scrubbed before it reaches the
// LLM prompt.
type Level int

const (
	// LevelRedacted scrubs likely warehouse-data values (emails, quoted
	// literals, UUIDs, long numbers, constraint/row-dump tuple values) from the
	// whole log while preserving the error structure the LLM needs. This is the
	// strict default.
	LevelRedacted Level = iota
	// LevelSummary applies the same redaction but additionally keeps only the
	// lines that carry diagnostic signal (error/detail/hint/constraint/SQLSTATE
	// markers and SQL position context), dropping run-progress noise.
	LevelSummary
	// LevelRaw forwards the log unchanged. It exists for local debugging against
	// a trusted, self-hosted LLM where no external egress occurs.
	LevelRaw
)

// ParseLevel maps a configuration string to a Level, defaulting to the strict
// LevelRedacted for empty or unrecognised input. Matching is case-insensitive.
func ParseLevel(s string) Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "raw":
		return LevelRaw
	case "summary":
		return LevelSummary
	default:
		return LevelRedacted
	}
}

// Stable placeholders substituted for redacted data values. They are short,
// self-describing, and distinct so the LLM can still reason about the shape of
// the original value.
const (
	placeholderEmail = "<email>"
	placeholderUUID  = "<uuid>"
	placeholderStr   = "<str>"
	placeholderNum   = "<num>"
)

var (
	// emailRe matches an email address anywhere in the text.
	emailRe = regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`)

	// uuidRe matches a canonical 8-4-4-4-12 UUID.
	uuidRe = regexp.MustCompile(`\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b`)

	// singleQuotedRe matches a single-quoted SQL string literal. In SQL,
	// single quotes always delimit string values, never identifiers, so the
	// whole literal is data.
	singleQuotedRe = regexp.MustCompile(`'[^']*'`)

	// invalidInputValueRe matches the double-quoted value in Postgres
	// "invalid input syntax ... : "value"" errors, where double quotes wrap a
	// data value rather than an identifier.
	invalidInputValueRe = regexp.MustCompile(`(invalid input syntax for [^:]+:\s*)"[^"]*"`)

	// constraintDetailValueRe matches the value side of a Postgres constraint
	// DETAIL line, e.g. `Key (email)=(john@x.com)`. The column list before `=`
	// is structural and kept; only the parenthesised value after `=` is data.
	constraintDetailValueRe = regexp.MustCompile(`(\bKey \([^)]*\)=)\([^)]*\)`)

	// rowDumpRe matches a Postgres failing-row tuple, e.g.
	// `Failing row contains (a, b, c)` or `row for relation ... contains (...)`.
	// The entire tuple body is warehouse data.
	rowDumpRe = regexp.MustCompile(`(?i)(row[^()\n]*contains\s*)\([^)]*\)`)

	// longNumberRe matches a standalone run of 6+ digits (ids, account numbers,
	// epoch values). Shorter numbers (LINE markers, SQLSTATE codes, small
	// counts) are kept because they carry structural meaning.
	longNumberRe = regexp.MustCompile(`\b\d{6,}\b`)

	// sqlstateRe matches a `SQLSTATE: <code>` field. The 5-character code is a
	// structural diagnostic and is preserved verbatim; this pattern is used to
	// shield it from the generic number/literal passes.
	sqlstateRe = regexp.MustCompile(`(?i)(sqlstate)[:=]?\s*([0-9A-Z]{5})\b`)
)

// summaryKeepRe selects lines worth keeping in LevelSummary: those carrying an
// error class, a Postgres diagnostic field, an SQL position marker, or a
// SQLSTATE code.
var summaryKeepRe = regexp.MustCompile(`(?i)(error|fatal|panic|exception|detail|hint|context|constraint|sqlstate|relation|column|\bline \d+:|\^|violates|does not exist|not present)`)

// Redactor is a LogSanitizer that scrubs likely warehouse-data values from a
// dbt log while preserving the error structure (error classes, relation and
// column identifiers, LINE markers, node ids, SQLSTATE codes) the LLM relies on
// to diagnose the failure.
type Redactor struct {
	level Level
}

// NewRedactor builds a Redactor at the given level.
func NewRedactor(level Level) *Redactor { return &Redactor{level: level} }

// Sanitize applies the configured redaction level to the dbt log.
func (r *Redactor) Sanitize(log string) string {
	if log == "" {
		return ""
	}
	if r.level == LevelRaw {
		return log
	}
	redacted := redactValues(log)
	if r.level == LevelSummary {
		return summarize(redacted)
	}
	return redacted
}

// redactValues applies the data-value redactions in an order that prevents one
// pattern from masking another. Constraint-DETAIL and failing-row tuple bodies
// are scrubbed in place (preserving the shape of each value as <uuid>/<num>/…)
// rather than blanked, so the LLM still sees how many values failed and of what
// kind, without seeing the values themselves.
func redactValues(log string) string {
	out := log

	// Shield the SQLSTATE code from the value passes by stashing it behind a
	// sentinel, then restore it at the end.
	const sqlstateSentinel = "\x00SQLSTATE\x00"
	var sqlstateCodes []string
	out = sqlstateRe.ReplaceAllStringFunc(out, func(m string) string {
		sub := sqlstateRe.FindStringSubmatch(m)
		sqlstateCodes = append(sqlstateCodes, sub[2])
		return sub[1] + ": " + sqlstateSentinel
	})

	// Constraint DETAIL value side: keep the `Key (cols)=` prefix, scrub the
	// `(value)` body.
	out = constraintDetailValueRe.ReplaceAllStringFunc(out, func(m string) string {
		sub := constraintDetailValueRe.FindStringSubmatch(m)
		body := strings.TrimSuffix(strings.TrimPrefix(m[len(sub[1]):], "("), ")")
		return sub[1] + "(" + redactTupleBody(body) + ")"
	})

	// Failing-row tuples: keep the `... contains ` prefix, scrub the tuple body.
	out = rowDumpRe.ReplaceAllStringFunc(out, func(m string) string {
		sub := rowDumpRe.FindStringSubmatch(m)
		body := strings.TrimSuffix(strings.TrimPrefix(m[len(sub[1]):], "("), ")")
		return sub[1] + "(" + redactTupleBody(body) + ")"
	})

	out = redactScalars(out)

	// Restore the preserved SQLSTATE codes.
	for _, code := range sqlstateCodes {
		out = strings.Replace(out, sqlstateSentinel, code, 1)
	}
	return out
}

// redactTupleBody scrubs the comma-separated fields of a Postgres tuple body
// (constraint DETAIL value or failing-row dump), where every field is warehouse
// data. Known shapes (email, UUID, number, quoted literal) keep their
// placeholder so the LLM still sees the column types; any other non-empty field
// (including bare tokens and free text) collapses to <str>. Postgres NULL is
// preserved as a structural marker.
func redactTupleBody(body string) string {
	fields := strings.Split(body, ",")
	for i, f := range fields {
		lead := f[:len(f)-len(strings.TrimLeft(f, " "))]
		v := strings.TrimSpace(f)
		switch {
		case v == "":
			// leave empty field as-is
		case strings.EqualFold(v, "null"):
			fields[i] = lead + v
		case emailRe.MatchString(v):
			fields[i] = lead + placeholderEmail
		case uuidRe.MatchString(v):
			fields[i] = lead + placeholderUUID
		case longNumberRe.MatchString(v):
			fields[i] = lead + placeholderNum
		default:
			fields[i] = lead + placeholderStr
		}
	}
	return strings.Join(fields, ",")
}

// redactScalars replaces individual data values (emails, UUIDs, quoted string
// literals, long numbers) with stable placeholders, leaving SQL keywords and
// double-quoted Postgres identifiers (relation/column names) intact.
func redactScalars(s string) string {
	out := s

	// Emails before quoted-literal and number passes so an address embedded in a
	// literal collapses to a single <email>.
	out = emailRe.ReplaceAllString(out, placeholderEmail)

	// UUIDs before the long-number pass (UUIDs contain digit runs).
	out = uuidRe.ReplaceAllString(out, placeholderUUID)

	// Quoted string values. Single quotes are always literals. Double quotes are
	// only treated as data in the "invalid input syntax: "value"" form; other
	// double-quoted tokens are Postgres identifiers and are preserved.
	out = invalidInputValueRe.ReplaceAllString(out, "${1}\""+placeholderStr+"\"")
	out = singleQuotedRe.ReplaceAllString(out, "'"+placeholderStr+"'")

	// Standalone long numbers last.
	out = longNumberRe.ReplaceAllString(out, placeholderNum)

	return out
}

// summarize keeps only diagnostic-bearing lines from an already-redacted log,
// dropping run-progress and compilation noise.
func summarize(log string) string {
	lines := strings.Split(log, "\n")
	kept := make([]string, 0, len(lines))
	for _, l := range lines {
		if summaryKeepRe.MatchString(l) {
			kept = append(kept, l)
		}
	}
	if len(kept) == 0 {
		// Nothing matched the diagnostic filter; fall back to the full redacted
		// log rather than emit an empty prompt.
		return log
	}
	return strings.Join(kept, "\n")
}

var _ ports.LogSanitizer = (*Redactor)(nil)
