package runner

import (
	"sort"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
)

// maxRationaleUpstream caps how many changed ancestors the rationale names,
// nearest first.
const maxRationaleUpstream = 3

// rationaleFactsFor projects what the trigger, a checked cluster and its draft
// hold onto the facts the cluster's rationale is composed from. The
// changed-ancestor analysis is supplied only by a validation rejection, so only
// a validation trigger reports the upstream set as known; every other source
// leaves it unknown even when a node carries a ChangedAncestors entry.
func rationaleFactsFor(t remediation.Trigger, c remediation.Cluster, d remediation.Draft) proposal.RationaleFacts {
	f := proposal.RationaleFacts{
		TargetNodeID:  c.TargetNodeID,
		CrossService:  c.Kind == workflow.ClusterCrossService,
		ModelNote:     d.Rationale,
		UpstreamKnown: t.Source == "validation",
	}
	nearest := map[string]proposal.RationaleUpstream{}
	for _, id := range c.Members {
		n, ok := nodeByID(t, id)
		if !ok {
			continue
		}
		f.Members = append(f.Members, proposal.RationaleMember{NodeID: n.NodeID, Service: n.Service, ErrorLine: proposal.RedactDataValues(n.ErrorExcerpt)})
		for _, a := range n.ChangedAncestors {
			if cur, seen := nearest[a.NodeID]; !seen || a.Depth < cur.Depth {
				nearest[a.NodeID] = proposal.RationaleUpstream{NodeID: a.NodeID, Service: a.Service, Depth: a.Depth}
			}
		}
	}
	f.TargetService = nearest[c.TargetNodeID].Service
	for _, u := range nearest {
		f.Upstream = append(f.Upstream, u)
	}
	sort.Slice(f.Upstream, func(i, j int) bool {
		if f.Upstream[i].Depth != f.Upstream[j].Depth {
			return f.Upstream[i].Depth < f.Upstream[j].Depth
		}
		return f.Upstream[i].NodeID < f.Upstream[j].NodeID
	})
	if len(f.Upstream) > maxRationaleUpstream {
		f.Upstream = f.Upstream[:maxRationaleUpstream]
	}
	for _, e := range d.Edits {
		f.Edits = append(f.Edits, e.Path)
	}
	return f
}
