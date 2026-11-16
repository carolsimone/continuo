package contractversion

import (
	"slices"
	"strings"
)

// Plan decides what a promotion writes for a bundle of contracts, given the
// state the graph holds for each node (states is keyed by unique id; a node
// with no entry is new). Every incoming contract becomes a Version. Then, per
// node:
//   - a node a newer promotion already reached (its Watermark is above
//     p.PromotionSeq) is Superseded and changes nothing;
//   - a node whose current contract is the incoming one is Unchanged, which is
//     also what a redelivery of this promotion finds;
//   - any other node takes an Edge numbered one above its newest edge. A
//     revert to an earlier contract takes the next number too, and its edge
//     names the earliest version of that contract in SameAsVersion.
//
// The incoming unique ids are distinct. Versions and Edges are sorted by
// unique id, so the plan does not depend on the order of in; Plan reads in and
// states without changing them.
func Plan(p Promotion, in []Incoming, states map[string]NodeState) WritePlan {
	ordered := slices.Clone(in)
	slices.SortStableFunc(ordered, func(a, b Incoming) int {
		return strings.Compare(a.UniqueID, b.UniqueID)
	})

	var plan WritePlan
	for _, c := range ordered {
		plan.Versions = append(plan.Versions, Version{
			UniqueID:       c.UniqueID,
			ContractHash:   c.ContractHash,
			Family:         c.Family,
			Service:        c.Service,
			FirstReleaseID: p.ReleaseID,
			Document:       c.Document,
		})

		state := states[c.UniqueID]
		switch {
		case state.Watermark > p.PromotionSeq:
			plan.Superseded++
		case state.CurrentHash == c.ContractHash:
			plan.Unchanged++
		default:
			plan.Edges = append(plan.Edges, Edge{
				UniqueID:      c.UniqueID,
				ContractHash:  c.ContractHash,
				ContentHash:   c.ContentHash,
				SourcePath:    c.SourcePath,
				Version:       state.MaxVersion + 1,
				SameAsVersion: state.FirstVersionByHash[c.ContractHash],
			})
		}
	}
	return plan
}
