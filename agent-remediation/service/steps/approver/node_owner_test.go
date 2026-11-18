package approver_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/adapters/repofs"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/approver"
)

const (
	bundleURI = "s3://bucket/code-bundles/rel-1/bundle.json"
	releaseID = "rel-1"
)

// fakeBundle answers each node from nodes, a node with an error from errs, and
// every other node with ports.ErrNotFound. It records the reads it was asked.
type fakeBundle struct {
	nodes map[string]ports.CandidateSource
	errs  map[string]error
	reads []string
}

func (b *fakeBundle) NodeSource(_ context.Context, uri, nodeID, release string) (ports.CandidateSource, error) {
	b.reads = append(b.reads, uri+"|"+nodeID+"|"+release)
	if err, ok := b.errs[nodeID]; ok {
		return ports.CandidateSource{}, err
	}
	if src, ok := b.nodes[nodeID]; ok {
		return src, nil
	}
	return ports.CandidateSource{}, ports.ErrNotFound
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newNodeOwner(b *fakeBundle) *approver.NodeOwner {
	return approver.NewNodeOwner(b, repofs.NewLocator(discard()), discard())
}

func trigger() remediation.Trigger {
	return remediation.Trigger{TenantID: "default", ReleaseID: releaseID, CodeBundleURI: bundleURI}
}

// odcs is an ODCS contract declaring analytics.kpis, owned by team.
func odcs(team string) string {
	doc := "apiVersion: v3.2.0\nkind: DataContract\nid: analytics.kpis\n" +
		"schema:\n  - name: kpis\n    physicalType: table\n"
	if team != "" {
		doc += "team:\n  name: " + team + "\n"
	}
	return doc
}

// contractCluster is a cluster of the members whose evidence holds the contract
// file of schema.table with text yaml.
func contractCluster(schema, table, yaml string, members ...string) step.ClusterDraft {
	return step.ClusterDraft{
		Cluster: remediation.Cluster{TargetNodeID: members[0], Members: members},
		Evidence: remediation.NewEvidence().With(remediation.KeyContractFile, remediation.ContractFile{
			Schema: schema, Table: table, YAMLPath: "contracts/kpis.odcs.yaml", YAMLText: yaml,
		}),
	}
}

func TestNodeOwner_Meta(t *testing.T) {
	m := newNodeOwner(&fakeBundle{}).Meta()
	assert.Equal(t, workflow.StepName("node_owner"), m.Name)
	assert.Equal(t, step.KindApprover, m.Kind)
	assert.Equal(t, []workflow.Stage{workflow.StageApprover}, m.Stages)
	assert.Empty(t, m.Needs, "the step reads the contract file when a cluster has one, and needs nothing")
	assert.NotEmpty(t, m.Description)
}

// The owner the code bundle records for a node is its approver, for a dbt node
// and for a python node alike, read from the trigger's bundle and release.
func TestNodeOwner_ReadsTheBundleOwner(t *testing.T) {
	b := &fakeBundle{nodes: map[string]ports.CandidateSource{
		"analytics.orders": {Runtime: ports.RuntimeDbt, Owner: "finance"},
		"analytics.kpis":   {Runtime: ports.RuntimePython, Owner: "e2e"},
	}}
	got, err := newNodeOwner(b).Approvers(context.Background(), step.ApproverInput{
		Trigger:  trigger(),
		Clusters: []step.ClusterDraft{contractCluster("analytics", "kpis", odcs("not-this-one"), "analytics.kpis")},
		Nodes:    []string{"analytics.orders", "analytics.kpis"},
	})
	require.NoError(t, err)
	assert.Equal(t, []proposal.Approver{
		{NodeID: "analytics.kpis", Owner: "e2e", Source: proposal.ApproverSourceBundle},
		{NodeID: "analytics.orders", Owner: "finance", Source: proposal.ApproverSourceBundle},
	}, got, "sorted by node id")
	assert.ElementsMatch(t, []string{
		bundleURI + "|analytics.orders|" + releaseID,
		bundleURI + "|analytics.kpis|" + releaseID,
	}, b.reads)
}

// A node the bundle does not hold, or holds with no owner, takes the team.name
// of the ODCS contract file its cluster's evidence carries.
func TestNodeOwner_FallsBackToTheContractFile(t *testing.T) {
	for name, b := range map[string]*fakeBundle{
		"no bundle entry":          {},
		"a bundle entry, no owner": {nodes: map[string]ports.CandidateSource{"analytics.kpis": {Runtime: ports.RuntimePython}}},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := newNodeOwner(b).Approvers(context.Background(), step.ApproverInput{
				Trigger:  trigger(),
				Clusters: []step.ClusterDraft{contractCluster("analytics", "kpis", odcs("analytics-team"), "analytics.kpis")},
				Nodes:    []string{"analytics.kpis"},
			})
			require.NoError(t, err)
			assert.Equal(t, []proposal.Approver{
				{NodeID: "analytics.kpis", Owner: "analytics-team", Source: proposal.ApproverSourceContractFile},
			}, got)
		})
	}
}

// A node with no owner in the bundle and no contract file that declares it, or
// one whose contract names no team or does not parse, has none: an empty owner
// from source none. A contract file of another node is not its declaration.
func TestNodeOwner_ANodeWithNoOwnerHasNone(t *testing.T) {
	for name, clusters := range map[string][]step.ClusterDraft{
		"no cluster":                     {},
		"a cluster with no contract":     {{Cluster: remediation.Cluster{TargetNodeID: "analytics.kpis", Members: []string{"analytics.kpis"}}, Evidence: remediation.NewEvidence()}},
		"another node's contract":        {contractCluster("analytics", "other", odcs("analytics-team"), "analytics.other")},
		"a contract with no team":        {contractCluster("analytics", "kpis", odcs(""), "analytics.kpis")},
		"a contract that does not parse": {contractCluster("analytics", "kpis", "team: [\n", "analytics.kpis")},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := newNodeOwner(&fakeBundle{}).Approvers(context.Background(), step.ApproverInput{
				Trigger:  trigger(),
				Clusters: clusters,
				Nodes:    []string{"analytics.kpis"},
			})
			require.NoError(t, err)
			assert.Equal(t, []proposal.Approver{
				{NodeID: "analytics.kpis", Owner: "", Source: proposal.ApproverSourceNone},
			}, got)
		})
	}
}

// A bundle read that fails for another reason than a permanent miss is
// returned, so the commit that runs the approver fails and runs again.
func TestNodeOwner_ABundleReadErrorIsReturned(t *testing.T) {
	transient := errors.New("s3 answered HTTP 503")
	b := &fakeBundle{errs: map[string]error{"analytics.kpis": transient}}
	_, err := newNodeOwner(b).Approvers(context.Background(), step.ApproverInput{
		Trigger:  trigger(),
		Clusters: []step.ClusterDraft{contractCluster("analytics", "kpis", odcs("analytics-team"), "analytics.kpis")},
		Nodes:    []string{"analytics.kpis"},
	})
	require.ErrorIs(t, err, transient)
	assert.Contains(t, err.Error(), "analytics.kpis")
}

// Each node is named once, whatever order and repeats it is given in.
func TestNodeOwner_NamesEachNodeOnce(t *testing.T) {
	b := &fakeBundle{nodes: map[string]ports.CandidateSource{"a.x": {Owner: "o"}}}
	got, err := newNodeOwner(b).Approvers(context.Background(), step.ApproverInput{
		Trigger: trigger(),
		Nodes:   []string{"b.y", "a.x", "b.y"},
	})
	require.NoError(t, err)
	assert.Equal(t, []proposal.Approver{
		{NodeID: "a.x", Owner: "o", Source: proposal.ApproverSourceBundle},
		{NodeID: "b.y", Owner: "", Source: proposal.ApproverSourceNone},
	}, got)
	assert.Len(t, b.reads, 2)
}
