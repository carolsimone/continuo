package fixer

import (
	"context"
	"sort"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
)

// ownChangeDiff is what this release changed in nodeID: the unified diff from
// the node's promoted version to candidateSource, sanitised and capped at
// maxUpstreamDiffBytes. Empty when the node has no promoted version (it is new)
// or the version read fails, which is logged and omits the section rather than
// blocking the fix.
func ownChangeDiff(ctx context.Context, svc Services, nodeID, candidateSource string) string {
	cur, ok, err := svc.Versions.CurrentVersion(ctx, nodeID)
	if err != nil {
		svc.Logger.Warn("current version unavailable; omitting own-change diff", "node", nodeID, "error", err)
		return ""
	}
	if !ok {
		return ""
	}
	return truncateDiff(svc.Sanitizer.Sanitize(proposal.ComputeUnifiedDiff(cur.RawCode, candidateSource, nodeID)), maxUpstreamDiffBytes)
}

// maxReleaseUpstreamAncestors caps how many changed ancestors the single-node
// prompt shows, nearest first.
const maxReleaseUpstreamAncestors = 3

// releaseUpstreamChanges renders what this release changed in the failing
// node's nearest changed ancestors: each ancestor's bundle source diffed
// against its promoted version. An ancestor whose bundle source or promoted
// version is unavailable is listed with an empty diff.
func releaseUpstreamChanges(ctx context.Context, svc Services, in Input) []prompt.ReleaseUpstreamChange {
	ancestors := append([]ChangedAncestorRef(nil), in.ChangedAncestors...)
	sort.Slice(ancestors, func(i, j int) bool {
		if ancestors[i].Depth != ancestors[j].Depth {
			return ancestors[i].Depth < ancestors[j].Depth
		}
		return ancestors[i].NodeID < ancestors[j].NodeID
	})
	if len(ancestors) > maxReleaseUpstreamAncestors {
		ancestors = ancestors[:maxReleaseUpstreamAncestors]
	}
	out := make([]prompt.ReleaseUpstreamChange, 0, len(ancestors))
	for _, a := range ancestors {
		diff := ""
		if src, err := svc.CandidateSource.NodeSource(ctx, in.CodeBundleURI, a.NodeID, in.ReleaseID); err != nil {
			svc.Logger.Warn("changed ancestor source unavailable; listing it without a diff", "node", in.NodeID, "ancestor", a.NodeID, "error", err)
		} else {
			diff = ownChangeDiff(ctx, svc, a.NodeID, src.RawCode)
		}
		out = append(out, prompt.ReleaseUpstreamChange{NodeID: a.NodeID, Service: a.Service, Depth: a.Depth, Diff: diff})
	}
	return out
}
