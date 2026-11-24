package remediation

import (
	"slices"
	"sort"
)

// TryRecord is one earlier try at a release's failing set, as a later try is
// shown it: the remediation round and try it was, its proposal attempt, what
// each of its clusters proposed and why it was refused, what the checks across
// clusters found, and why the verification gate failed it. An attempt that
// belongs to no remediation numbers no try, so it has Try 0 and orders before
// every runner try of its round.
type TryRecord struct {
	Round   int
	Try     int
	Attempt int
	// Clusters are the try's clusters, in cluster order.
	Clusters []ClusterRecord
	// CrossCluster is every finding of the try's cross-cluster checks.
	CrossCluster []Finding
	// Gate is every finding of the verification gate that failed the try.
	Gate []Finding
}

// ClusterRecord is one cluster of an earlier try: its type, the node its fix
// edited and its failing members, the edits of its last pre-flight round, the
// findings of each of its pre-flight rounds (one entry per round, first round
// first), and how it ended when it ended without a checked draft.
type ClusterRecord struct {
	Type         string
	TargetNodeID string
	Members      []string
	Edits        []EditRecord
	Rounds       [][]Finding
	Outcome      string
}

// EditRecord is one file an earlier try changed, with its unified diff.
type EditRecord struct {
	Path string
	Diff string
}

// TryRef names a try by its remediation round and its try number in that round.
type TryRef struct {
	Round int
	Try   int
}

// Before reports whether r is a try made before o: an earlier round, or an
// earlier try of the same round.
func (r TryRef) Before(o TryRef) bool {
	if r.Round != o.Round {
		return r.Round < o.Round
	}
	return r.Try < o.Try
}

// Ref names the try the record is of.
func (t TryRecord) Ref() TryRef {
	return TryRef{Round: t.Round, Try: t.Try}
}

// TryRecords returns the records of the tries made before `before` that hold
// any of nodeIDs as a cluster member, oldest first (by round, try, then
// attempt). Each returned record keeps only its clusters that hold one of
// nodeIDs; its cross-cluster and gate findings are kept whole, since they judge
// the whole try. The records given are left as they were.
func TryRecords(records []TryRecord, nodeIDs []string, before TryRef) []TryRecord {
	out := make([]TryRecord, 0, len(records))
	for _, rec := range records {
		if !rec.Ref().Before(before) {
			continue
		}
		var clusters []ClusterRecord
		for _, c := range rec.Clusters {
			if holdsAny(c.Members, nodeIDs) {
				clusters = append(clusters, c)
			}
		}
		if len(clusters) == 0 {
			continue
		}
		rec.Clusters = clusters
		out = append(out, rec)
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Ref() != b.Ref() {
			return a.Ref().Before(b.Ref())
		}
		return a.Attempt < b.Attempt
	})
	return out
}

func holdsAny(members, nodeIDs []string) bool {
	for _, id := range nodeIDs {
		if slices.Contains(members, id) {
			return true
		}
	}
	return false
}
