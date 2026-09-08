package proposal

import "regexp"

// identifierPattern matches a plausible SQL identifier: a schema-qualified or
// bare name made of letters, digits, underscore, and dollar sign, not
// starting with a digit. A double-quoted span matching this is a schema
// identifier (a column or relation name), not a data value.
var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*)*$`)

var (
	singleQuotedSpan = regexp.MustCompile(`'[^']*'`)
	parenthetical    = regexp.MustCompile(`\([^)]*\)`)
	doubleQuotedSpan = regexp.MustCompile(`"[^"]*"`)
)

// RedactDataValues strips warehouse data values out of a warehouse error
// message while preserving schema identifiers, so the redacted text can be
// persisted and published without leaking whatever data triggered the error.
// It replaces every single-quoted span (a SQL string literal is always
// data), the content of every parenthetical group — this covers the "Key
// (col)=(value)" unique/FK-violation shape and Postgres's constraint-DETAIL
// shape ("Failing row contains (v1, v2, ...)"), where the offending values sit
// unquoted in a paren-delimited list — and every double-quoted span that is
// not a plausible SQL identifier: a double-quoted schema or column name such
// as "amount" or "public.wrong_name" is left untouched, but a double-quoted
// data value such as "customer@example.test" or "2026-01-05" is redacted.
//
// This is heuristic, best-effort redaction over the structured Postgres/dbt
// error shapes actually observed, not a guarantee: a data value that is
// unquoted, sits outside any parenthesis, and happens to be shaped like a
// bare identifier (for example a bare username token in an error sentence)
// can still pass through unredacted.
func RedactDataValues(s string) string {
	s = singleQuotedSpan.ReplaceAllString(s, "'?'")
	s = parenthetical.ReplaceAllString(s, "(?)")
	s = doubleQuotedSpan.ReplaceAllStringFunc(s, func(m string) string {
		inner := m[1 : len(m)-1]
		if identifierPattern.MatchString(inner) {
			return m
		}
		return `"?"`
	})
	return s
}
