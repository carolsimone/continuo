package approver_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
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
	"github.com/carolsimone/continuo/pkg/domain/model"
)

const (
	bundleURI = "s3://bucket/code-bundles/rel-1/bundle.json"
	releaseID = "rel-1"
)

// fakeBundle is a code bundle that records owners, by node id. Every read
// answers err while it is set. It records each owners read it was asked.
type fakeBundle struct {
	owners map[string]string
	err    error
	reads  []string
}

func (b *fakeBundle) NodeSource(context.Context, string, string, string) (ports.CandidateSource, error) {
	return ports.CandidateSource{}, errors.New("node_owner reads owners, never a node's source")
}

func (b *fakeBundle) NodeOwners(_ context.Context, uri, release string, nodeIDs []string) (map[string]string, error) {
	b.reads = append(b.reads, uri+"|"+release+"|"+strings.Join(nodeIDs, ","))
	if b.err != nil {
		return nil, b.err
	}
	out := map[string]string{}
	for _, id := range nodeIDs {
		if o, ok := b.owners[id]; ok {
			out[id] = o
		}
	}
	return out, nil
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
	b := &fakeBundle{owners: map[string]string{"analytics.orders": "finance", "analytics.kpis": "e2e"}}
	got, err := newNodeOwner(b).Approvers(context.Background(), step.ApproverInput{
		Trigger:  trigger(),
		Clusters: []step.ClusterDraft{contractCluster("analytics", "kpis", odcs("not-this-one"), "analytics.kpis")},
		Nodes:    []string{"analytics.orders", "analytics.kpis"},
	})
	require.NoError(t, err)
	assert.Equal(t, []proposal.Approver{
		{NodeID: "analytics.kpis", Owner: "e2e", Source: model.ApproverSourceBundle},
		{NodeID: "analytics.orders", Owner: "finance", Source: model.ApproverSourceBundle},
	}, got, "sorted by node id")
	assert.Equal(t, []string{bundleURI + "|" + releaseID + "|analytics.kpis,analytics.orders"}, b.reads)
}

// The approvers of several nodes come from one read of the bundle, asked for
// every node once.
func TestNodeOwner_ReadsTheBundleOnceForEveryNode(t *testing.T) {
	b := &fakeBundle{owners: map[string]string{"a.x": "o1", "c.z": "o3"}}
	_, err := newNodeOwner(b).Approvers(context.Background(), step.ApproverInput{
		Trigger: trigger(),
		Nodes:   []string{"c.z", "a.x", "b.y", "a.x"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{bundleURI + "|" + releaseID + "|a.x,b.y,c.z"}, b.reads)
}

// A node the bundle records no owner for, or whose bundle cannot be read for
// good, takes the team.name of the ODCS contract file its cluster's evidence
// carries.
func TestNodeOwner_FallsBackToTheContractFile(t *testing.T) {
	for name, b := range map[string]*fakeBundle{
		"no owner in the bundle":     {},
		"a bundle that is not there": {err: ports.ErrNotFound},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := newNodeOwner(b).Approvers(context.Background(), step.ApproverInput{
				Trigger:  trigger(),
				Clusters: []step.ClusterDraft{contractCluster("analytics", "kpis", odcs("analytics-team"), "analytics.kpis")},
				Nodes:    []string{"analytics.kpis"},
			})
			require.NoError(t, err)
			assert.Equal(t, []proposal.Approver{
				{NodeID: "analytics.kpis", Owner: "analytics-team", Source: model.ApproverSourceContractFile},
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
				{NodeID: "analytics.kpis", Owner: "", Source: model.ApproverSourceNone},
			}, got)
		})
	}
}

// A bundle read that fails for another reason than a permanent miss is logged
// as a warning and never fails the end: each node falls back to its contract
// file's owner, else none.
func TestNodeOwner_ABundleReadErrorFallsBackAndWarns(t *testing.T) {
	var logs bytes.Buffer
	b := &fakeBundle{err: errors.New("s3 answered HTTP 503")}
	s := approver.NewNodeOwner(b, repofs.NewLocator(discard()), slog.New(slog.NewTextHandler(&logs, nil)))

	got, err := s.Approvers(context.Background(), step.ApproverInput{
		Trigger:  trigger(),
		Clusters: []step.ClusterDraft{contractCluster("analytics", "kpis", odcs("analytics-team"), "analytics.kpis")},
		Nodes:    []string{"analytics.kpis", "analytics.other"},
	})

	require.NoError(t, err)
	assert.Equal(t, []proposal.Approver{
		{NodeID: "analytics.kpis", Owner: "analytics-team", Source: model.ApproverSourceContractFile},
		{NodeID: "analytics.other", Owner: "", Source: model.ApproverSourceNone},
	}, got)
	assert.Contains(t, logs.String(), "level=WARN")
	assert.Contains(t, logs.String(), "s3 answered HTTP 503")
}

// Each node is named once, whatever order and repeats it is given in.
func TestNodeOwner_NamesEachNodeOnce(t *testing.T) {
	b := &fakeBundle{owners: map[string]string{"a.x": "o"}}
	got, err := newNodeOwner(b).Approvers(context.Background(), step.ApproverInput{
		Trigger: trigger(),
		Nodes:   []string{"b.y", "a.x", "b.y"},
	})
	require.NoError(t, err)
	assert.Equal(t, []proposal.Approver{
		{NodeID: "a.x", Owner: "o", Source: model.ApproverSourceBundle},
		{NodeID: "b.y", Owner: "", Source: model.ApproverSourceNone},
	}, got)
}

// With no node, the bundle is not read.
func TestNodeOwner_NoNodeReadsNothing(t *testing.T) {
	b := &fakeBundle{}
	got, err := newNodeOwner(b).Approvers(context.Background(), step.ApproverInput{Trigger: trigger()})
	require.NoError(t, err)
	assert.Empty(t, got)
	assert.Empty(t, b.reads)
}
