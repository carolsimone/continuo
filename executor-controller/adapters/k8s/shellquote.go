package k8s

import (
	"regexp"
	"strings"
)

// shellSafeRe matches strings that need no quoting on a POSIX `sh -c` line.
var shellSafeRe = regexp.MustCompile(`^[A-Za-z0-9_@%+=:,./-]+$`)

// shellQuote returns s safe for interpolation into a `sh -c` command line:
// shell-safe strings pass through unchanged (keeping the default compile line
// byte-identical to the plain-dbt form), everything else is single-quoted
// with embedded single quotes escaped.
func shellQuote(s string) string {
	if shellSafeRe.MatchString(s) {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// shellJoin quotes each argv element and joins them into one command line.
func shellJoin(argv []string) string {
	quoted := make([]string, len(argv))
	for i, a := range argv {
		quoted[i] = shellQuote(a)
	}
	return strings.Join(quoted, " ")
}
