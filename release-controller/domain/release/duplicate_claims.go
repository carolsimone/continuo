package release

import (
	"fmt"
	"sort"
	"strings"
)

// CollisionKind distinguishes what two or more claimants share.
type CollisionKind string

const (
	// CollisionRelation is the zero value, so a DuplicateClaim{...} literal
	// that predates this field (every one in this package's own tests, and
	// any other future caller that only ever meant a relation collision)
	// keeps producing relation-collision formatting unchanged. Under this
	// kind, RelationID is the physical warehouse relation two or more
	// claimants write.
	CollisionRelation CollisionKind = ""
	// CollisionIdentity marks two or more claimants that share a unique_id.
	// unique_id is the identity key for every downstream consumer keyed on
	// it — the code bundle, the candidate object key, NodeRegistry lookups,
	// release-controller's own topology walks, the orchestrator's :Table
	// MERGE — so sharing it silently erases one of the claimants wherever
	// that key is used, independent of whether their resolved relations also
	// collide. Under this kind, UniqueID is the shared identity.
	CollisionIdentity CollisionKind = "identity"
)

// Claimant is one node's identity, owner, source location, and kind within a
// duplicate claim. UniqueID is the claimant's OWN declared identity — a
// relation collision groups by the resolved relation, not by unique_id, so
// claimants of one relation claim can carry different unique_ids (two
// different declared names that resolve to the same alias). NodeType lets a
// downstream consumer (the remediation fixer) tell a dbt claimant from a
// python one without a second topology lookup: the two kinds locate their
// source differently, and only a dbt claimant's source is a single file this
// system can read.
type Claimant struct {
	UniqueID         string
	ServiceName      string
	OriginalFilePath string
	NodeType         string
}

// DuplicateClaim names one thing two or more nodes in a candidate topology
// hold that only one node may hold, with every node that holds it.
//
// Kind selects which field identifies what is shared:
//   - CollisionRelation: RelationID is the physical write target, not merely
//     a graph key. Two nodes claiming it write the same warehouse table and
//     overwrite each other, and the second silently replaces the first in
//     the promoted topology, current_prod, and the code bundle. RelationID
//     is not necessarily any single claimant's own UniqueID — see Claimant.
//   - CollisionIdentity: UniqueID is the declared identity two or more nodes
//     share. It cannot be cleared by renaming a claimant's alias — alias
//     changes ResolvedRelationID, not the declared model name UniqueID is
//     keyed on — so a rename proposal can resolve a relation collision but
//     never an identity one.
type DuplicateClaim struct {
	Kind       CollisionKind
	RelationID string
	UniqueID   string
	Claimants  []Claimant
}

// sortKey is the value claims are ordered by: the physical relation for a
// relation collision, the shared identity for an identity collision.
func (c DuplicateClaim) sortKey() string {
	if c.Kind == CollisionIdentity {
		return c.UniqueID
	}
	return c.RelationID
}

// DuplicateClaims returns every collision in the candidate topology: a
// relation claimed by more than one node (relationCollisions), and a
// unique_id shared by more than one node (identityCollisions), merged and
// sorted together by their key. An empty result means every node's identity
// and physical relation are each held by exactly one node.
//
// The two checks are independent and both run unconditionally. They close
// different holes and neither substitutes for the other: a relation
// collision is two differently-named models racing to write one warehouse
// table; an identity collision is two models that downstream code cannot
// tell apart at all, because every lookup keyed on unique_id sees only one
// of them, whether or not the two also happen to resolve to different
// relations. A single pair of nodes can trip only one of the two, or both at
// once.
func DuplicateClaims(candidate Topology) []DuplicateClaim {
	out := relationCollisions(candidate)
	out = append(out, identityCollisions(candidate)...)
	sort.Slice(out, func(i, j int) bool {
		ki, kj := out[i].sortKey(), out[j].sortKey()
		if ki != kj {
			return ki < kj
		}
		// A relation claim and an identity claim can carry the same string
		// (a resolved relation and a shared unique_id happen to coincide), so
		// the key alone does not order them. Kind breaks the tie, keeping
		// FormatDuplicateClaims's sentence order — and therefore
		// failing_nodes/per_node order — fixed across runs on identical input.
		return out[i].Kind < out[j].Kind
	})
	return out
}

// relationCollisions groups nodes by ResolvedRelationID — the relation a node
// actually writes (its dbt alias, when it has one) — falling back to
// UniqueID when ResolvedRelationID is empty (a payload from before that
// field existed). Two nodes with different declared names but the same
// resolved relation collide even though their unique_ids differ; two nodes
// sharing a declared name but resolving to different relations do NOT
// collide here — see identityCollisions for why they can still collide on
// identity.
//
// The check is service-agnostic: two nodes in the same service collide
// exactly as two nodes in different services do.
func relationCollisions(candidate Topology) []DuplicateClaim {
	byRelation := make(map[string][]Claimant, len(candidate))
	for _, n := range candidate {
		rel := effectiveRelation(n)
		byRelation[rel] = append(byRelation[rel], claimantOf(n))
	}

	var out []DuplicateClaim
	for id, claimants := range byRelation {
		if len(claimants) < 2 {
			continue
		}
		sortClaimants(claimants)
		out = append(out, DuplicateClaim{Kind: CollisionRelation, RelationID: id, Claimants: claimants})
	}
	return out
}

// identityCollisions groups nodes by UniqueID and reports a group only when
// its members do NOT all resolve to the same relation. A relation-homogeneous
// group (same unique_id, same resolved relation, on every member) is already
// reported by relationCollisions for those exact same members — reporting it
// again here would add no information. A group that is not
// relation-homogeneous is exactly the case relationCollisions cannot see: the
// same declared identity claimed under two different physical relations,
// invisible to a check keyed on the relation alone.
func identityCollisions(candidate Topology) []DuplicateClaim {
	byID := make(map[string][]Claimant, len(candidate))
	relationsByID := make(map[string]map[string]bool, len(candidate))
	for _, n := range candidate {
		byID[n.UniqueID] = append(byID[n.UniqueID], claimantOf(n))
		if relationsByID[n.UniqueID] == nil {
			relationsByID[n.UniqueID] = make(map[string]bool, 2)
		}
		relationsByID[n.UniqueID][effectiveRelation(n)] = true
	}

	var out []DuplicateClaim
	for id, claimants := range byID {
		if len(claimants) < 2 || len(relationsByID[id]) < 2 {
			continue
		}
		sortClaimants(claimants)
		out = append(out, DuplicateClaim{Kind: CollisionIdentity, UniqueID: id, Claimants: claimants})
	}
	return out
}

// effectiveRelation is the relation a node actually writes: its
// ResolvedRelationID, falling back to its own UniqueID when that field is
// empty (a payload from before it existed).
func effectiveRelation(n Node) string {
	if n.ResolvedRelationID != "" {
		return n.ResolvedRelationID
	}
	return n.UniqueID
}

func claimantOf(n Node) Claimant {
	return Claimant{
		UniqueID:         n.UniqueID,
		ServiceName:      n.ServiceName,
		OriginalFilePath: n.OriginalFilePath,
		NodeType:         n.NodeType,
	}
}

// sortClaimants orders a claim's claimants by (service, file path), the
// stable presentation order used both in the rejection detail and by Target's
// deterministic fallback.
func sortClaimants(claimants []Claimant) {
	sort.Slice(claimants, func(i, j int) bool {
		if claimants[i].ServiceName != claimants[j].ServiceName {
			return claimants[i].ServiceName < claimants[j].ServiceName
		}
		return claimants[i].OriginalFilePath < claimants[j].OriginalFilePath
	})
}

// Target returns the claimant a rename should be proposed against, and one
// competing claimant to name as the relation's other producer. Meaningful
// only for a relation collision: the caller never invokes it for an identity
// collision, because renaming a claimant's alias changes ResolvedRelationID,
// not the shared UniqueID, so no rename proposal can clear one.
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
// every claimant of every collision, followed by the remedy for each kind of
// collision present. A relation collision reads "<relation> is produced by
// ..."; an identity collision reads "unique_id <id> is declared by ..." — the
// wording itself names which kind it is, because the remedies differ: an
// alias rename clears a relation collision but cannot touch an identity one.
func FormatDuplicateClaims(claims []DuplicateClaim) string {
	parts := make([]string, 0, len(claims))
	hasRelation, hasIdentity := false, false
	for _, c := range claims {
		producers := make([]string, 0, len(c.Claimants))
		for _, cl := range c.Claimants {
			producers = append(producers, fmt.Sprintf("%s (%s)", cl.ServiceName, cl.OriginalFilePath))
		}
		if c.Kind == CollisionIdentity {
			hasIdentity = true
			parts = append(parts, fmt.Sprintf("unique_id %s is declared by %s", c.UniqueID, joinWithAnd(producers)))
		} else {
			hasRelation = true
			parts = append(parts, fmt.Sprintf("%s is produced by %s", c.RelationID, joinWithAnd(producers)))
		}
	}
	var suffix strings.Builder
	if hasRelation {
		suffix.WriteString("; a relation may be produced by exactly one node — rename one of them")
	}
	if hasIdentity {
		suffix.WriteString("; a unique_id must be declared by exactly one node — an alias rename cannot fix this, the declared model itself must be renamed")
	}
	return strings.Join(parts, "; ") + suffix.String()
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
