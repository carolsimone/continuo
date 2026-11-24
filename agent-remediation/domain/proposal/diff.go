package proposal

import (
	"fmt"
	"strings"
)

// ComputeUnifiedDiff produces a human-readable unified diff between the broken
// candidate SQL and the proposed SQL, labelled with the model's file path. It
// is deterministic and dependency-free: a longest-common-subsequence algorithm
// provides the line-by-line comparison sufficient for the small single-file
// dbt models this service handles. Identical inputs yield an empty string with
// no header or +/- lines.
func ComputeUnifiedDiff(candidate, proposed, path string) string {
	return unifiedDiff(candidate, proposed, path, path)
}

// EditDiff is the diff of one edit: ComputeRenameDiff when the edit moves the
// file from renamedFrom to path, ComputeUnifiedDiff of the file at path when
// renamedFrom is empty.
func EditDiff(original, proposed, path, renamedFrom string) string {
	if renamedFrom != "" {
		return ComputeRenameDiff(original, proposed, renamedFrom, path)
	}
	return ComputeUnifiedDiff(original, proposed, path)
}

// ComputeRenameDiff is the diff of a file moved from one path to another, in
// git's form: a "rename from"/"rename to" header, then the unified diff of its
// content labelled with the old path on the "---" line and the new one on the
// "+++" line. A move that leaves the content unchanged is the header alone, so
// the diff of a rename is never empty.
func ComputeRenameDiff(original, proposed, from, to string) string {
	return fmt.Sprintf("rename from %s\nrename to %s\n", from, to) + unifiedDiff(original, proposed, from, to)
}

// unifiedDiff is the unified diff of candidate against proposed, labelled
// fromPath on the "---" line and toPath on the "+++" line, empty when the two
// have the same lines.
func unifiedDiff(candidate, proposed, fromPath, toPath string) string {
	cand := strings.Split(strings.TrimRight(candidate, "\n"), "\n")
	prop := strings.Split(strings.TrimRight(proposed, "\n"), "\n")
	ops := diffLines(cand, prop)

	// Emit headers and diff lines only when changes exist; identical inputs
	// return an empty string so callers can cheaply detect "no diff".
	hasChanges := false
	for _, op := range ops {
		if len(op) > 0 && (op[0] == '+' || op[0] == '-') {
			hasChanges = true
			break
		}
	}
	if !hasChanges {
		return ""
	}

	var b strings.Builder
	fmt.Fprintf(&b, "--- a/%s\n", fromPath)
	fmt.Fprintf(&b, "+++ b/%s\n", toPath)
	for _, op := range ops {
		b.WriteString(op)
		b.WriteString("\n")
	}
	return b.String()
}

// diffLines returns unified-style "-"/"+"/" " prefixed lines via a standard
// LCS table. Kept internal and pure for testability.
func diffLines(a, b []string) []string {
	n, m := len(a), len(b)
	lcs := make([][]int, n+1)
	for i := range lcs {
		lcs[i] = make([]int, m+1)
	}
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			if a[i] == b[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
			} else if lcs[i+1][j] >= lcs[i][j+1] {
				lcs[i][j] = lcs[i+1][j]
			} else {
				lcs[i][j] = lcs[i][j+1]
			}
		}
	}
	var out []string
	i, j := 0, 0
	for i < n && j < m {
		switch {
		case a[i] == b[j]:
			out = append(out, " "+a[i])
			i++
			j++
		case lcs[i+1][j] >= lcs[i][j+1]:
			out = append(out, "-"+a[i])
			i++
		default:
			out = append(out, "+"+b[j])
			j++
		}
	}
	for ; i < n; i++ {
		out = append(out, "-"+a[i])
	}
	for ; j < m; j++ {
		out = append(out, "+"+b[j])
	}
	return out
}
