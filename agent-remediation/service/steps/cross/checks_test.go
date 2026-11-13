package cross_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/cross"
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
			{NodeID: "python.ledger.rates", NodeType: "python-csv", Service: "ledger"},
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

func run(t *testing.T, c step.CrossCheck, drafts ...step.ClusterDraft) remediation.CheckResult {
	t.Helper()
	res, err := c.CheckAll(context.Background(), step.CrossInput{Trigger: trigger(), Drafts: drafts})
	require.NoError(t, err)
	return res
}

func TestMeta_PlacesTheThreeChecksAtTheCrossClusterStage(t *testing.T) {
	for name, c := range map[string]step.CrossCheck{
		"edits_map_to_a_service": cross.NewEditsMapToAService(repoPaths, nil),
		"no_path_edited_twice":   cross.NewNoPathEditedTwice(repoPaths, nil),
		"one_manifest_kind":      cross.NewOneManifestKind(repoPaths, nil),
	} {
		m := c.Meta()
		assert.Equal(t, workflow.StepName(name), m.Name)
		assert.Equal(t, step.KindCheck, m.Kind)
		assert.Equal(t, []workflow.Stage{workflow.StageCrossCluster}, m.Stages)
		assert.False(t, m.MayStandAsGateRun)
		assert.Empty(t, m.Needs, "the checks read optional evidence, so a flow need not provide it")
		assert.NotEmpty(t, m.Description)
	}
}

func TestEditsMapToAService(t *testing.T) {
	c := cross.NewEditsMapToAService(repoPaths, nil)

	ok := run(t, c,
		clusterDraft(1, nil, edit("dbt/services/sales/models/orders.sql", "model.sales.orders")),
		clusterDraft(2, nil, edit("dbt/services/finance/models/p.sql", "model.sales.orders")))
	assert.True(t, ok.Passed())
	assert.Equal(t, "edits_map_to_a_service", ok.Check)

	// The first refused edit, in cluster order, is the one reported.
	bad := run(t, c,
		clusterDraft(1, nil, edit("dbt/services/sales/models/orders.sql", "model.sales.orders")),
		clusterDraft(2, nil, edit("docs/readme.md", "model.sales.orders"), edit("other/file.sql", "model.sales.orders")))
	require.Len(t, bad.Findings, 1)
	assert.Equal(t, "edit_outside_services", bad.Findings[0].Code)
	assert.Equal(t, "edit belongs to no configured service: docs/readme.md", bad.Findings[0].Message)
	assert.Equal(t, "docs/readme.md", bad.Findings[0].File)
	assert.False(t, bad.Passed())
}

func TestNoPathEditedTwice(t *testing.T) {
	c := cross.NewNoPathEditedTwice(repoPaths, nil)
	p := "dbt/services/sales/models/orders.sql"

	assert.True(t, run(t, c, clusterDraft(1, nil, edit(p, "a")), clusterDraft(2, nil, edit(p+"2", "a"))).Passed())

	// The same path in two clusters, and twice inside one cluster, are both refused.
	for _, drafts := range [][]step.ClusterDraft{
		{clusterDraft(1, nil, edit(p, "a")), clusterDraft(2, nil, edit(p, "b"))},
		{clusterDraft(1, nil, edit(p, "a"), edit(p, "a"))},
	} {
		res := run(t, c, drafts...)
		require.Len(t, res.Findings, 1)
		assert.Equal(t, "path_edited_twice", res.Findings[0].Code)
		assert.Equal(t, "two edits change the same file: "+p, res.Findings[0].Message)
		assert.Equal(t, "no_path_edited_twice", res.Check)
	}

	// An edit no service owns is not a duplicate: it is refused as unmapped.
	assert.True(t, run(t, c, clusterDraft(1, nil, edit("x/y.sql", "a"), edit("x/y.sql", "a"))).Passed())
}

func TestOneManifestKind(t *testing.T) {
	c := cross.NewOneManifestKind(repoPaths, nil)
	contract := []byte("nodes: []\n")
	yml := "python/ledger/contracts/postings.yaml"

	// A python contract with an edit to a failing python node's contract: fine.
	assert.True(t, run(t, c, clusterDraft(1, contract, edit(yml, "python.ledger.postings"))).Passed())
	// A dbt service never has a packaged contract, so dbt edits alone pass.
	assert.True(t, run(t, c, clusterDraft(1, nil, edit("dbt/services/sales/models/a.sql", "model.sales.orders"))).Passed())
	// A python contract in one service and dbt edits in another: fine.
	assert.True(t, run(t, c,
		clusterDraft(1, contract, edit(yml, "python.ledger.postings")),
		clusterDraft(2, nil, edit("dbt/services/sales/models/a.sql", "model.sales.orders"))).Passed())

	// Edits to a dbt model in the python service next to its contract: mixed.
	mixed := run(t, c,
		clusterDraft(1, contract, edit(yml, "python.ledger.postings")),
		clusterDraft(2, nil, edit("python/ledger/models/m.sql", "model.sales.orders")))
	require.Len(t, mixed.Findings, 1)
	assert.Equal(t, "mixed_manifest_kinds", mixed.Findings[0].Code)
	assert.Equal(t, "service ledger mixes a python contract with dbt edits in one attempt; a release verifies one manifest kind", mixed.Findings[0].Message)
	assert.Equal(t, "one_manifest_kind", mixed.Check)

	// An edit whose target the trigger does not name is not a python contract.
	unnamed := run(t, c, clusterDraft(1, contract, edit(yml, "python.ledger.unknown")))
	require.Len(t, unnamed.Findings, 1)
	assert.Equal(t, "mixed_manifest_kinds", unnamed.Findings[0].Code)

	// An edit to a python node of a non-script kind in a contract service counts as a contract edit.
	assert.True(t, run(t, c, clusterDraft(1, contract, edit(yml, "python.ledger.rates"))).Passed())
}

func TestOneManifestKind_ContractBelongsToTheServiceOfTheClustersFirstEdit(t *testing.T) {
	c := cross.NewOneManifestKind(repoPaths, nil)
	contract := []byte("nodes: []\n")
	// The cluster's first edit is in ledger, so its contract is ledger's; a later
	// edit of the same cluster in sales is a dbt edit of a service with no contract.
	res := run(t, c, clusterDraft(1, contract,
		edit("python/ledger/contracts/postings.yaml", "python.ledger.postings"),
		edit("dbt/services/sales/models/a.sql", "model.sales.orders")))
	assert.True(t, res.Passed())

	// A cluster with a contract and no edit sets none.
	assert.True(t, run(t, c, clusterDraft(1, contract)).Passed())
}

func TestGroup(t *testing.T) {
	contract := []byte("nodes: []\n")
	groups, err := cross.Group(repoPaths, trigger(), []step.ClusterDraft{
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

	_, err = cross.Group(repoPaths, trigger(), []step.ClusterDraft{clusterDraft(1, nil, edit("zzz/a.sql", "x"))})
	assert.ErrorIs(t, err, cross.ErrUnmappedEdit)
	_, err = cross.Group(repoPaths, trigger(), []step.ClusterDraft{
		clusterDraft(1, nil, edit("dbt/services/sales/a.sql", "x"), edit("dbt/services/sales/a.sql", "x"))})
	assert.ErrorIs(t, err, cross.ErrDuplicateEditPath)
	_, err = cross.Group(repoPaths, trigger(), []step.ClusterDraft{
		clusterDraft(1, contract, edit("python/ledger/a.sql", "model.sales.orders"))})
	assert.ErrorIs(t, err, cross.ErrMixedManifestKinds)
}
