package proposals

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/event"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// diffByteCap bounds the unified-diff text carried on a ClosedEdit, so a
// pathological diff cannot bloat the pr_closed event payload.
const diffByteCap = 8 << 10

// normalizeContent canonicalizes file text for the amend byte-compare: it folds
// CRLF line endings to LF and strips trailing newlines, so a merge that only
// rewrote line endings or added/removed a trailing newline is not mistaken for
// a human amendment.
func normalizeContent(s string) string {
	return strings.TrimRight(strings.ReplaceAll(s, "\r\n", "\n"), "\n")
}

// resolveClosedEdits fetches each edit's merged content at mergeSHA and its
// proposed content from S3, and returns the pr_closed edit list with an amended
// flag and a capped diff per edit. An edit is amended when the merged file's
// normalized content differs from the proposal's, or when the file is gone at
// the merge commit (ErrSourceNotFound) — either way the merged tree is not what
// the agent proposed. Any OTHER fetch error (a non-404 GitHub read, or an S3
// read of the proposed content or the diff) aborts the whole resolution with
// that error, so the caller records nothing this tick and retries next pass
// rather than emitting a half-computed amend verdict.
func resolveClosedEdits(ctx context.Context, sources ports.SourceReader, evidence ports.EvidenceReader,
	repo, mergeSHA string, edits []proposal.FileEdit) ([]event.ClosedEdit, error) {
	out := make([]event.ClosedEdit, 0, len(edits))
	for _, e := range edits {
		amended := false
		merged, err := sources.ReadFile(ctx, repo, mergeSHA, e.Path)
		switch {
		case errors.Is(err, ports.ErrSourceNotFound):
			amended = true // the edit's file is gone at the merge commit: not as-proposed
		case err != nil:
			return nil, fmt.Errorf("read merged %s@%s: %w", e.Path, mergeSHA, err)
		default:
			proposed, ferr := evidence.Fetch(ctx, e.ContentURI)
			if ferr != nil {
				return nil, fmt.Errorf("fetch proposed %s: %w", e.ContentURI, ferr)
			}
			amended = normalizeContent(merged) != normalizeContent(proposed)
		}
		diff, derr := evidence.Fetch(ctx, e.DiffURI)
		if derr != nil {
			return nil, fmt.Errorf("fetch diff %s: %w", e.DiffURI, derr)
		}
		if len(diff) > diffByteCap {
			diff = diff[:diffByteCap]
		}
		out = append(out, event.ClosedEdit{Path: e.Path, TargetNodeID: e.TargetNodeID, Amended: amended, Diff: diff})
	}
	return out, nil
}
