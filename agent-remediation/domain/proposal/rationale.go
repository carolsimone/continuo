package proposal

import (
	"fmt"
	"strings"
)

// RationaleMember is one failing node a fix addresses, with the error line
// its validation recorded (empty when the failure carried none).
type RationaleMember struct {
	NodeID    string
	Project   string
	ErrorLine string
}

// RationaleUpstream is one node this release changed upstream of the failing
// nodes: what the fix is answering. Depth is the minimum upstream hop
// distance from the failing node.
type RationaleUpstream struct {
	NodeID  string
	Project string
	Depth   int
}

// RationaleFacts is everything the component knows about one cluster's fix.
// The rationale a reviewer reads is composed from these facts alone; the
// model contributes only ModelNote, one sentence describing its edit, taken
// from the same call that produced the edit.
type RationaleFacts struct {
	Members []RationaleMember
	// Upstream is what this release changed upstream of the members, nearest
	// first. UpstreamKnown reports that the trigger carried the release's
	// changed-ancestor facts at all — the analysis is supplied only by a
	// validation rejection; compile, seed_build, and duplicate_table carry
	// none, so nothing can be claimed about it either way for them.
	Upstream      []RationaleUpstream
	UpstreamKnown bool
	// Edits are the repository paths this cluster's fix changes; they all
	// repair TargetNodeID, which for an upstream fix is not a member.
	Edits        []string
	TargetNodeID string
	// TargetProject is the project that owns TargetNodeID — the cluster's
	// edited node. A member living in this same project can still change in
	// this release; only a member in a different project cannot.
	TargetProject string
	// CrossProject marks the cluster as reaching at least one member in a
	// project other than the target's, where that member cannot change in
	// this release. A coalesced cluster can still carry same-project members
	// alongside them; ComposeRationale emits the follow-up line only for the
	// members whose project actually differs from TargetProject.
	CrossProject bool
	ModelNote    string
}

// ComposeRationale renders one cluster's rationale, one fact per line, in a
// fixed order: what failed, what this release changed upstream (or that
// nothing did), what was edited, why the edit is where it is for each member
// whose project differs from TargetProject, and last the model's own note
// under its label.
func ComposeRationale(f RationaleFacts) string {
	var b strings.Builder
	for _, m := range f.Members {
		fmt.Fprintf(&b, "Failed: `%s`", m.NodeID)
		if m.Project != "" {
			fmt.Fprintf(&b, " (project %s)", m.Project)
		}
		if m.ErrorLine != "" {
			fmt.Fprintf(&b, ": %s", m.ErrorLine)
		}
		b.WriteString("\n")
	}
	if f.UpstreamKnown {
		if len(f.Upstream) == 0 {
			fmt.Fprintf(&b, "No upstream of %s changed in this release.\n", quotedNodeIDs(f.Members))
		}
		for _, u := range f.Upstream {
			fmt.Fprintf(&b, "This release changed upstream: `%s` (project %s, %s up)\n", u.NodeID, u.Project, hops(u.Depth))
		}
	}
	for _, e := range f.Edits {
		fmt.Fprintf(&b, "Edited: `%s` (repairs `%s`)\n", e, f.TargetNodeID)
	}
	if f.CrossProject {
		for _, m := range f.Members {
			// An unresolved TargetProject ("") never equals a member's
			// non-empty Project, so this guard degrades to the old
			// per-cluster behavior — every member with a project gets the
			// line — when the target's own project could not be determined.
			if m.Project == "" || m.Project == f.TargetProject {
				continue
			}
			fmt.Fprintf(&b, "`%s` lives in project %s and cannot change in this release; this edit keeps the contract it reads. Moving it to the new shape is a follow-up for project %s.\n", m.NodeID, m.Project, m.Project)
		}
	}
	if f.ModelNote != "" {
		fmt.Fprintf(&b, "Model's note: %s\n", strings.TrimSpace(f.ModelNote))
	}
	return strings.TrimRight(b.String(), "\n")
}

// quotedNodeIDs renders the members' ids for the no-upstream line: one id in
// backticks, or "the failed nodes" when there are several.
func quotedNodeIDs(ms []RationaleMember) string {
	if len(ms) == 1 {
		return "`" + ms[0].NodeID + "`"
	}
	return "the failed nodes"
}

func hops(n int) string {
	if n == 1 {
		return "1 hop"
	}
	return fmt.Sprintf("%d hops", n)
}
