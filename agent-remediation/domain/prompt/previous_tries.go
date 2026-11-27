package prompt

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	// maxPreviousTries is how many earlier tries the section shows: the most
	// recent ones, which already learned from the ones before them.
	maxPreviousTries = 3
	// maxPreviousTryDiffBytes caps each diff an earlier try applied, so one
	// large edit cannot dominate the prompt.
	maxPreviousTryDiffBytes = 8192
)

// TryDiff is one file's unified diff from an earlier fix try, named by
// the repository path that try changed.
type TryDiff struct {
	Path string
	Diff string
}

// PreviousTry is one earlier try at the same failure as the model is shown it:
// the heading it is shown under, the diffs it applied, one line per thing its
// pre-flight checks found, and why the gate failed it. Every text
// is already sanitized.
type PreviousTry struct {
	Label     string
	Diffs     []TryDiff
	Findings  []string
	GateError string
}

// WithPreviousTries returns req with a section on the earlier tries at the same
// failure appended to its user message after a blank line: a
// "## Earlier tries at this failure" heading, then one "### <label>" part per
// try, oldest first, with the diffs it applied, what pre-flight found and why
// the gate failed. Only the last maxPreviousTries tries are shown, and each diff
// is cut at maxPreviousTryDiffBytes and shown without its trailing newlines. No
// tries leave req as it is. req is not changed.
func WithPreviousTries(req ProposeRequest, tries []PreviousTry) ProposeRequest {
	if len(tries) == 0 {
		return req
	}
	if len(tries) > maxPreviousTries {
		tries = tries[len(tries)-maxPreviousTries:]
	}
	var u strings.Builder
	u.WriteString(req.User)
	u.WriteString("\n\n## Earlier tries at this failure\n")
	u.WriteString("These tries at the same failure did not pass. Do not repeat a change that did not pass; correct what each try was refused for.\n")
	for _, t := range tries {
		fmt.Fprintf(&u, "\n### %s\n", t.Label)
		if len(t.Diffs) == 0 && len(t.Findings) == 0 && t.GateError == "" {
			u.WriteString("Nothing was recorded for this try.\n")
			continue
		}
		for _, d := range t.Diffs {
			diff := strings.TrimRight(TruncateDiff(d.Diff, maxPreviousTryDiffBytes), "\n")
			fmt.Fprintf(&u, "Changed %s:\n```diff\n%s\n```\n", d.Path, diff)
		}
		if len(t.Findings) > 0 {
			u.WriteString("What pre-flight found:\n")
			for _, f := range t.Findings {
				fmt.Fprintf(&u, "- %s\n", f)
			}
		}
		if t.GateError != "" {
			fmt.Fprintf(&u, "Why the gate failed: %s\n", t.GateError)
		}
	}
	req.User = strings.TrimSuffix(u.String(), "\n")
	return req
}

// TruncateDiff caps a diff at max bytes, appending a marker when it was cut.
// The cut is backed up to a UTF-8 rune boundary so truncation never splits a
// multibyte character, which would render as U+FFFD in the prompt.
func TruncateDiff(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "\n… (diff truncated)"
}
