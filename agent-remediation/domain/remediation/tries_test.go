package remediation

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTryRecords_KeepsOnlyEarlierTriesOfTheseNodes(t *testing.T) {
	cluster := func(target string, members ...string) ClusterRecord {
		return ClusterRecord{Type: "dbt-validation", TargetNodeID: target, Members: members}
	}
	gate := []Finding{{Code: "verification_failed", Message: "model.a: still broken"}}
	records := []TryRecord{
		// The try being made, and one of a later round: neither is earlier.
		{Round: 2, Try: 2, Attempt: 5, Clusters: []ClusterRecord{cluster("model.a", "model.a")}},
		{Round: 3, Try: 1, Attempt: 6, Clusters: []ClusterRecord{cluster("model.a", "model.a")}},
		// An earlier try of this round, with a cluster of another node that is left out.
		{Round: 2, Try: 1, Attempt: 4, Clusters: []ClusterRecord{cluster("model.z", "model.z"), cluster("model.a", "model.a")}, Gate: gate},
		// An earlier round's try that fixed the node at a shared ancestor.
		{Round: 1, Try: 2, Attempt: 2, Clusters: []ClusterRecord{cluster("model.up", "model.a", "model.b")}},
		// An earlier try that never held the node.
		{Round: 1, Try: 3, Attempt: 3, Clusters: []ClusterRecord{cluster("model.z", "model.z")}},
		// An attempt of the legacy engine, which has no try number.
		{Round: 1, Try: 0, Attempt: 1, Clusters: []ClusterRecord{cluster("model.a", "model.a")}},
	}

	got := TryRecords(records, []string{"model.a"}, TryRef{Round: 2, Try: 2})

	assert.Equal(t, []TryRecord{
		{Round: 1, Try: 0, Attempt: 1, Clusters: []ClusterRecord{cluster("model.a", "model.a")}},
		{Round: 1, Try: 2, Attempt: 2, Clusters: []ClusterRecord{cluster("model.up", "model.a", "model.b")}},
		{Round: 2, Try: 1, Attempt: 4, Clusters: []ClusterRecord{cluster("model.a", "model.a")}, Gate: gate},
	}, got, "the earlier tries holding the node, oldest first, each with only the clusters that hold it")
	assert.Len(t, records[2].Clusters, 2, "the records given are left as they were")
}

func TestTryRecords_NoEarlierTryGivesNone(t *testing.T) {
	records := []TryRecord{{Round: 1, Try: 1, Attempt: 1, Clusters: []ClusterRecord{{Members: []string{"model.a"}}}}}
	assert.Empty(t, TryRecords(records, []string{"model.a"}, TryRef{Round: 1, Try: 1}))
	assert.Empty(t, TryRecords(nil, []string{"model.a"}, TryRef{Round: 1, Try: 1}))
}

func TestTryRef_OrdersByRoundThenTry(t *testing.T) {
	assert.True(t, TryRef{Round: 1, Try: 3}.Before(TryRef{Round: 2, Try: 1}))
	assert.True(t, TryRef{Round: 2, Try: 0}.Before(TryRef{Round: 2, Try: 1}))
	assert.False(t, TryRef{Round: 2, Try: 1}.Before(TryRef{Round: 2, Try: 1}))
	assert.False(t, TryRef{Round: 2, Try: 1}.Before(TryRef{Round: 1, Try: 9}))
}
