package handlers

import (
	"sort"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/typology"
)

// maxRationaleUpstream caps how many changed ancestors the rationale names,
// nearest first.
const maxRationaleUpstream = 3

// rationaleFactsFor projects what the trigger, the cluster, and its outcome
// hold onto the facts the rationale is composed from. The changed-ancestor
// analysis is supplied only by a validation rejection (release-controller's
// validation-result path computes it); compile, seed_build, and
// duplicate_table rejections carry none, so only a validation-sourced trigger
// reports the upstream set as known — every other source leaves it unknown
// even when a node happens to carry a ChangedAncestors entry.
func rationaleFactsFor(t Trigger, c typology.Cluster, out clusterOutcome) proposal.RationaleFacts {
	f := proposal.RationaleFacts{
		TargetNodeID:  c.TargetNodeID,
		CrossService:  c.Kind == typology.KindCrossServiceUpstream,
		ModelNote:     out.rationale,
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
	for _, e := range out.edits {
		f.Edits = append(f.Edits, e.Path)
	}
	return f
}
