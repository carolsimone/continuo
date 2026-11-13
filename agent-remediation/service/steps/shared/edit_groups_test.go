package shared_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
)

var repoPaths = map[string]string{
	"sales":   "dbt/services/sales",
	"finance": "dbt/services/finance",
	"ledger":  "python/ledger",
}

func trigger() remediation.Trigger {
	return remediation.Trigger{
		ReleaseID: "rel-1",
		Nodes: []remediation.TriggerNode{
			{NodeID: "model.sales.orders", NodeType: "dbt-model", Service: "sales"},
			{NodeID: "python.ledger.postings", NodeType: "python-node", Service: "ledger"},
		},
	}
}

func edit(path, target string) remediation.Edit {
	return remediation.Edit{Path: path, Content: "x", TargetNodeID: target}
}

func clusterDraft(i int, contract []byte, edits ...remediation.Edit) step.ClusterDraft {
	ev := remediation.NewEvidence()
	if contract != nil {
		ev = ev.With(remediation.KeyPackagedContract, remediation.Bytes(contract))
	}
	return step.ClusterDraft{
		Cluster:  remediation.Cluster{Cluster: i},
		Draft:    remediation.Draft{Edits: edits},
		Evidence: ev,
	}
}

func TestGroup(t *testing.T) {
	contract := []byte("nodes: []\n")
	groups, err := shared.Group(repoPaths, trigger(), []step.ClusterDraft{
		clusterDraft(1, nil, edit("dbt/services/sales/models/b.sql", "model.sales.orders"), edit("dbt/services/sales/models/a.sql", "model.sales.orders")),
		clusterDraft(2, contract, edit("python/ledger/contracts/postings.yaml", "python.ledger.postings")),
	})
	require.NoError(t, err)
	require.Len(t, groups, 2)
	assert.Equal(t, "ledger", groups[0].Service)
	assert.Equal(t, contract, groups[0].Contract)
	assert.Equal(t, "python/ledger", groups[0].Prefix)
	assert.Equal(t, "sales", groups[1].Service)
	assert.Nil(t, groups[1].Contract)
	assert.Equal(t, "dbt/services/sales", groups[1].Prefix)
	require.Len(t, groups[1].Edits, 2)
	assert.Equal(t, "dbt/services/sales/models/b.sql", groups[1].Edits[0].Path, "edits keep the order of the clusters")

	_, err = shared.Group(repoPaths, trigger(), []step.ClusterDraft{clusterDraft(1, nil, edit("zzz/a.sql", "x"))})
	assert.ErrorIs(t, err, shared.ErrUnmappedEdit)
	_, err = shared.Group(repoPaths, trigger(), []step.ClusterDraft{
		clusterDraft(1, nil, edit("dbt/services/sales/a.sql", "x"), edit("dbt/services/sales/a.sql", "x"))})
	assert.ErrorIs(t, err, shared.ErrDuplicateEditPath)
	_, err = shared.Group(repoPaths, trigger(), []step.ClusterDraft{
		clusterDraft(1, contract, edit("python/ledger/a.sql", "model.sales.orders"))})
	assert.ErrorIs(t, err, shared.ErrMixedManifestKinds)
}

func TestRefusalFinding(t *testing.T) {
	contract := []byte("nodes: []\n")
	cases := map[string]struct {
		drafts []step.ClusterDraft
		code   string
		msg    string
	}{
		"unmapped": {
			[]step.ClusterDraft{clusterDraft(1, nil, edit("zzz/a.sql", "x"))},
			"edit_outside_services", "edit belongs to no configured service: zzz/a.sql",
		},
		"duplicate": {
			[]step.ClusterDraft{clusterDraft(1, nil, edit("dbt/services/sales/a.sql", "x"), edit("dbt/services/sales/a.sql", "x"))},
			"path_edited_twice", "two edits change the same file: dbt/services/sales/a.sql",
		},
		"mixed": {
			[]step.ClusterDraft{clusterDraft(1, contract, edit("python/ledger/a.sql", "model.sales.orders"))},
			"mixed_manifest_kinds", "service ledger mixes a python contract with dbt edits in one attempt; a release verifies one manifest kind",
		},
	}
	for name, c := range cases {
		_, err := shared.Group(repoPaths, trigger(), c.drafts)
		f, ok := shared.RefusalFinding(err)
		require.True(t, ok, name)
		assert.Equal(t, remediation.Finding{Code: c.code, Message: c.msg}, f, name)
	}
	_, ok := shared.RefusalFinding(assert.AnError)
	assert.False(t, ok)
}
