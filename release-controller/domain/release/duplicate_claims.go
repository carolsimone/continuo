package release

import (
	"fmt"
	"sort"
	"strings"
)

// Claimant is one node's owner, source location, and kind within a duplicate
// claim. NodeType lets a downstream consumer (the remediation fixer) tell a
// dbt claimant from a python one without a second topology lookup: the two
// kinds locate their source differently, and only a dbt claimant's source is
// a single file this system can read.
type Claimant struct {
	ServiceName      string
	OriginalFilePath string
	NodeType         string
}

// DuplicateClaim names one relation claimed by more than one node in a
// candidate topology, with every node that claims it.
//
// unique_id is the physical write target, not merely a graph key: two nodes
// claiming it write the same warehouse table and overwrite each other, and the
// second silently replaces the first in the promoted topology, current_prod,
// and the code bundle. A relation must therefore be produced by exactly one
// node.
type DuplicateClaim struct {
	UniqueID  string
	Claimants []Claimant
}

// DuplicateClaims returns every unique_id claimed by more than one node in the
// candidate topology, each with its claimants sorted by (service, file path).
// Claims are sorted by unique_id. An empty result means every relation has a
// single producer.
//
// The check is service-agnostic: two nodes in the same service collide exactly
// as two nodes in different services do.
func DuplicateClaims(candidate Topology) []DuplicateClaim {
	byID := make(map[string][]Claimant, len(candidate))
	for _, n := range candidate {
		byID[n.UniqueID] = append(byID[n.UniqueID], Claimant{
			ServiceName:      n.ServiceName,
			OriginalFilePath: n.OriginalFilePath,
			NodeType:         n.NodeType,
		})
	}

	var out []DuplicateClaim
	for id, claimants := range byID {
		if len(claimants) < 2 {
			continue
		}
		sort.Slice(claimants, func(i, j int) bool {
			if claimants[i].ServiceName != claimants[j].ServiceName {
				return claimants[i].ServiceName < claimants[j].ServiceName
			}
			return claimants[i].OriginalFilePath < claimants[j].OriginalFilePath
		})
		out = append(out, DuplicateClaim{UniqueID: id, Claimants: claimants})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UniqueID < out[j].UniqueID })
	return out
}

// Target returns the claimant a rename should be proposed against, and one
// competing claimant to name as the relation's other producer.
//
// The changed service's claimant wins when it has one: that is the source
// being changed in this release, so renaming it never touches a service the
// release did not modify. This holds for a bootstrap release too — bootstrap
// still carries a single changedService (the posting service, also used by
// promoteToProduction for the service_prod upsert), so its claimant is
// targeted the same way. Only when neither claimant belongs to the changed
// service (both are already-promoted services colliding) does the choice fall
// back to the first claimant in sorted order, to keep it deterministic; the
// rejection detail names every claimant, so an operator can rename a
// different one instead.
func (c DuplicateClaim) Target(changedService string) (target, other Claimant) {
	idx := 0
	for i, cl := range c.Claimants {
		if cl.ServiceName == changedService {
			idx = i
			break
		}
	}
	target = c.Claimants[idx]
	for i, cl := range c.Claimants {
		if i != idx {
			return target, cl
		}
	}
	return target, Claimant{}
}

// FormatDuplicateClaims renders claims as one operator-facing sentence naming
// every claimant of every duplicated relation, followed by the action that
// resolves it.
func FormatDuplicateClaims(claims []DuplicateClaim) string {
	parts := make([]string, 0, len(claims))
	for _, c := range claims {
		producers := make([]string, 0, len(c.Claimants))
		for _, cl := range c.Claimants {
			producers = append(producers, fmt.Sprintf("%s (%s)", cl.ServiceName, cl.OriginalFilePath))
		}
		parts = append(parts, fmt.Sprintf("%s is produced by %s", c.UniqueID, joinWithAnd(producers)))
	}
	return strings.Join(parts, "; ") +
		"; a relation may be produced by exactly one node — rename one of them"
}

// joinWithAnd renders a list as "a", "a and b", or "a, b and c".
func joinWithAnd(items []string) string {
	switch len(items) {
	case 0:
		return ""
	case 1:
		return items[0]
	default:
		return strings.Join(items[:len(items)-1], ", ") + " and " + items[len(items)-1]
	}
}
