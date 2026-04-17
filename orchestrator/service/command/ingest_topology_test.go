package command_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/service/command"
	domainCmd "github.com/carolsimone/continuo/orchestrator/domain/command"
	"github.com/carolsimone/continuo/orchestrator/domain/topology"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── fakes: topology.Repository ───────────────────────────────────────────────

type fakeTopologyRepository struct {
	upsertNodeFn    func(ctx context.Context, node *topology.TopologyNode) error
	upsertNodeCalls []*topology.TopologyNode
}

func (f *fakeTopologyRepository) UpsertNode(ctx context.Context, node *topology.TopologyNode) error {
	f.upsertNodeCalls = append(f.upsertNodeCalls, node)
	if f.upsertNodeFn != nil {
		return f.upsertNodeFn(ctx, node)
	}
	return nil
}

func (f *fakeTopologyRepository) GetScheduleGraph(ctx context.Context, scheduleName string) ([]*topology.Node, []*topology.UpstreamDependency, error) {
	return nil, nil, nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func makeIngestTopologyCmd() domainCmd.IngestTopologyCmd {
	return domainCmd.IngestTopologyCmd{
		Nodes: []domainCmd.TopologyNodePayload{
			{
				ServiceName:     "svc1",
				SchemaName:      "public",
				TableName:       "orders",
				Owner:           "team-a",
				ScheduleName:    "daily",
				Criticality:     "CORE",
				NodeType:        "dbt-model",
				ManifestVersion: "v1",
				Dependencies:    []domainCmd.DependencyPayload{},
			},
			{
				ServiceName:     "svc1",
				SchemaName:      "public",
				TableName:       "customers",
				Owner:           "team-a",
				ScheduleName:    "daily",
				Criticality:     "CORE",
				NodeType:        "dbt-model",
				ManifestVersion: "v1",
				Dependencies: []domainCmd.DependencyPayload{
					{ServiceName: "svc1", SchemaName: "public", TableName: "orders"},
				},
			},
			{
				ServiceName:     "svc2",
				SchemaName:      "analytics",
				TableName:       "revenue",
				Owner:           "team-b",
				ScheduleName:    "hourly",
				Criticality:     "REGULATORY",
				NodeType:        "dbt-model",
				ManifestVersion: "v2",
				Dependencies:    []domainCmd.DependencyPayload{},
			},
		},
	}
}

// ── tests ─────────────────────────────────────────────────────────────────────

// 1. 3 nodes across 2 schedules → UpsertNode called 3x, outbox entry created with both schedule names.
func TestIngestTopology_ThreeNodesTwoSchedules(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	topoRepo := &fakeTopologyRepository{}

	h := command.NewIngestTopologyHandler(uow, topoRepo, newTestLogger())
	cmd := makeIngestTopologyCmd()

	err := h.Handle(ctx, cmd, "msg-ingest-1")
	require.NoError(t, err)

	// UpsertNode should be called once per node
	assert.Len(t, topoRepo.upsertNodeCalls, 3, "UpsertNode should be called 3 times")

	// Transaction should be committed
	assert.True(t, uow.CommittedTx, "transaction should be committed")

	// One outbox entry should be created
	require.Len(t, uow.outboxRepo.CreatedEntries, 1, "one outbox entry should be created")

	entry := uow.outboxRepo.CreatedEntries[0]
	assert.Equal(t, "topology_ingested", entry.EventType)
	assert.Equal(t, "schedules.loaded:v1", entry.StreamName)
	assert.Equal(t, "orchestrator", entry.AggregateType)
	assert.Equal(t, "pending", entry.Status)

	// Parse outbox payload and verify schedule names and manifest versions
	var outboxPayload struct {
		ScheduleNames    []string          `json:"schedule_names"`
		ManifestVersions map[string]string `json:"manifest_versions"`
	}
	require.NoError(t, json.Unmarshal(entry.Payload, &outboxPayload))

	assert.ElementsMatch(t, []string{"daily", "hourly"}, outboxPayload.ScheduleNames,
		"outbox payload should contain both schedule names")
	assert.Equal(t, map[string]string{"svc1": "v1", "svc2": "v2"}, outboxPayload.ManifestVersions,
		"outbox payload should contain manifest versions per service")
}

// 2. Duplicate message → skip processing.
func TestIngestTopology_DuplicateMessage(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	topoRepo := &fakeTopologyRepository{}

	h := command.NewIngestTopologyHandler(uow, topoRepo, newTestLogger())
	cmd := makeIngestTopologyCmd()

	// First call: processes normally
	err := h.Handle(ctx, cmd, "dup-ingest-1")
	require.NoError(t, err)
	assert.Len(t, topoRepo.upsertNodeCalls, 3)

	// Reset call tracking
	topoRepo.upsertNodeCalls = nil
	uow.outboxRepo.CreatedEntries = nil
	uow.CommittedTx = false

	// Second call with same message ID: should be skipped
	err = h.Handle(ctx, cmd, "dup-ingest-1")
	require.NoError(t, err)

	assert.Len(t, topoRepo.upsertNodeCalls, 0, "UpsertNode must NOT be called for duplicate")
	assert.Len(t, uow.outboxRepo.CreatedEntries, 0, "no outbox entries for duplicate")
}
