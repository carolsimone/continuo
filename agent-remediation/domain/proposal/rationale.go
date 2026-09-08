package proposal

import (
	"fmt"
	"strings"
)

// RationaleMember is one failing node a fix addresses, with the error line
// its validation recorded (empty when the failure carried none).
type RationaleMember struct {
	NodeID    string
	Service   string
	ErrorLine string
}

// RationaleUpstream is one node this release changed upstream of the failing
// nodes: what the fix is answering. Depth is the minimum upstream hop
// distance from the failing node.
type RationaleUpstream struct {
	NodeID  string
	Service string
	Depth   int
}

// RationaleFacts is everything the service knows about one cluster's fix.
// The rationale a reviewer reads is composed from these facts alone; the
// model contributes only ModelNote, one sentence describing its edit, taken
// from the same call that produced the edit.
type RationaleFacts struct {
	Members []RationaleMember
	// Upstream is what this release changed upstream of the members, nearest
	// first. UpstreamKnown reports that the trigger carried the release's
	// changed-ancestor facts at all — a compile-stage rejection carries
	// none, so nothing can be claimed about it either way.
	Upstream      []RationaleUpstream
	UpstreamKnown bool
	// Edits are the repository paths this cluster's fix changes; they all
	// repair TargetNodeID, which for an upstream fix is not a member.
	Edits        []string
	TargetNodeID string
	// CrossService marks the members as living in a service other than the
	// target's, where they cannot change in this release.
	CrossService bool
	ModelNote    string
}

// ComposeRationale renders one cluster's rationale, one fact per line, in a
// fixed order: what failed, what this release changed upstream (or that
// nothing did), what was edited, why the edit is where it is when the failure
// crossed a service boundary, and last the model's own note under its label.
func ComposeRationale(f RationaleFacts) string {
	var b strings.Builder
	for _, m := range f.Members {
		fmt.Fprintf(&b, "Failed: `%s`", m.NodeID)
		if m.Service != "" {
			fmt.Fprintf(&b, " (service %s)", m.Service)
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
			fmt.Fprintf(&b, "This release changed upstream: `%s` (service %s, %s up)\n", u.NodeID, u.Service, hops(u.Depth))
		}
	}
	for _, e := range f.Edits {
		fmt.Fprintf(&b, "Edited: `%s` (repairs `%s`)\n", e, f.TargetNodeID)
	}
	if f.CrossService {
		for _, m := range f.Members {
			fmt.Fprintf(&b, "`%s` lives in service %s and cannot change in this release; this edit keeps the contract it reads. Moving it to the new shape is a follow-up for service %s.\n", m.NodeID, m.Service, m.Service)
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
