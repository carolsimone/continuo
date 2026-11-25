package contractversion

import (
	"slices"
	"strings"
)

// Plan decides what a promotion writes for a bundle of contracts, given the
// state the graph holds for each node (states is keyed by unique id; a node
// with no entry is new). Per node:
//   - a node a newer promotion already reached (its Watermark is above
//     p.PromotionSeq) is Superseded: it changes nothing and contributes no
//     Version, so a late promotion never stores a contract nothing points at;
//   - a node whose current contract is the incoming one is Unchanged, which is
//     also what a redelivery of this promotion finds; its contract is a
//     Version, which the write finds stored;
//   - any other node takes a Version and an Edge numbered one above its
//     newest edge. A revert to an earlier contract takes the next number too,
//     and its edge names the earliest version of that contract the graph
//     still holds in SameAsVersion, none when the retention sweep deleted
//     them all.
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
		state := states[c.UniqueID]
		if state.Watermark > p.PromotionSeq {
			plan.Superseded++
			continue
		}
		plan.Versions = append(plan.Versions, Version{
			UniqueID:       c.UniqueID,
			ContractHash:   c.ContractHash,
			Family:         c.Family,
			Project:        c.Project,
			FirstReleaseID: p.ReleaseID,
			Document:       c.Document,
		})

		if state.CurrentHash == c.ContractHash {
			plan.Unchanged++
			continue
		}
		plan.Edges = append(plan.Edges, Edge{
			UniqueID:      c.UniqueID,
			ContractHash:  c.ContractHash,
			ContentHash:   c.ContentHash,
			SourcePath:    c.SourcePath,
			Version:       state.MaxVersion + 1,
			SameAsVersion: state.FirstVersionByHash[c.ContractHash],
		})
	}
	return plan
}
