package command_test

import (
	"context"
	"encoding/json"
	"testing"

	domainCmd "github.com/carolsimone/continuo/orchestrator/domain/command"
	"github.com/carolsimone/continuo/orchestrator/domain/topology"
	"github.com/carolsimone/continuo/orchestrator/service/command"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── fakes: topology.Repository ───────────────────────────────────────────────

type fakeTopologyRepository struct {
	applySnapshotFn    func(ctx context.Context, nodes []*topology.TopologyNode, topologyGeneration int64) error
	applySnapshotCalls [][]*topology.TopologyNode
}

func (f *fakeTopologyRepository) ApplySnapshot(ctx context.Context, nodes []*topology.TopologyNode, topologyGeneration int64) error {
	copied := append([]*topology.TopologyNode(nil), nodes...)
	f.applySnapshotCalls = append(f.applySnapshotCalls, copied)
	if f.applySnapshotFn != nil {
		return f.applySnapshotFn(ctx, nodes, topologyGeneration)
	}
	return nil
}

func (f *fakeTopologyRepository) SetServiceMetadata(ctx context.Context, serviceMetadata map[string]map[string]string, topologyGeneration int64) error {
	return nil
}

func (f *fakeTopologyRepository) GetScheduleGraph(ctx context.Context, scheduleName string) ([]*topology.Node, []*topology.UpstreamDependency, error) {
	return nil, nil, nil
}

// ── fakes: topology.TopologyStateRepository ──────────────────────────────────

type fakeTopologyStateRepository struct {
	generation int64
}

func (f *fakeTopologyStateRepository) IncrementGeneration(ctx context.Context) (int64, error) {
	f.generation++
	return f.generation, nil
}

func (f *fakeTopologyStateRepository) GetGeneration(ctx context.Context) (int64, error) {
	return f.generation, nil
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
				ImageTag:        "sha256:abc",
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
				ImageTag:        "sha256:abc",
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
				ImageTag:        "sha256:def",
				Dependencies:    []domainCmd.DependencyPayload{},
			},
		},
	}
}

// ── tests ─────────────────────────────────────────────────────────────────────

// 1. 3 nodes across 2 schedules → ApplySnapshot called once, outbox entry created with both schedule names.
func TestIngestTopology_ThreeNodesTwoSchedules(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	topoRepo := &fakeTopologyRepository{}
	stateRepo := &fakeTopologyStateRepository{}

	h := command.NewIngestTopologyHandler(uow, topoRepo, stateRepo, newTestLogger())
	cmd := makeIngestTopologyCmd()

	err := h.Handle(ctx, cmd, "msg-ingest-1")
	require.NoError(t, err)

	require.Len(t, topoRepo.applySnapshotCalls, 1, "ApplySnapshot should be called once per manifest message")
	assert.Len(t, topoRepo.applySnapshotCalls[0], 3, "ApplySnapshot should receive all nodes from the manifest payload")

	// Transaction should be committed
	assert.True(t, uow.CommittedTx, "transaction should be committed")

	// One outbox entry should be created
	require.Len(t, uow.outboxRepo.CreatedEntries, 1, "one outbox entry should be created")

	entry := uow.outboxRepo.CreatedEntries[0]
	assert.Equal(t, "topology_ingested", entry.EventType)
	assert.Equal(t, "schedules.loaded:v1", entry.StreamName)
	assert.Equal(t, "orchestrator", entry.AggregateType)
	assert.Equal(t, "pending", entry.Status)

	// Parse outbox payload and verify schedule names and service_metadata
	var outboxPayload struct {
		ScheduleNames   []string                       `json:"schedule_names"`
		ServiceMetadata map[string]map[string]string   `json:"service_metadata"`
	}
	require.NoError(t, json.Unmarshal(entry.Payload, &outboxPayload))

	assert.ElementsMatch(t, []string{"daily", "hourly"}, outboxPayload.ScheduleNames,
		"outbox payload should contain both schedule names")
	assert.Equal(t, map[string]map[string]string{
		"svc1": {"manifest_version": "v1", "image_tag": "sha256:abc"},
		"svc2": {"manifest_version": "v2", "image_tag": "sha256:def"},
	}, outboxPayload.ServiceMetadata,
		"outbox payload should contain service_metadata per service")

	// topology_generation should be 1 after one ingestion
	assert.Equal(t, int64(1), stateRepo.generation)
}

// 2. Duplicate message → skip processing.
func TestIngestTopology_DuplicateMessage(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	topoRepo := &fakeTopologyRepository{}
	stateRepo := &fakeTopologyStateRepository{}

	h := command.NewIngestTopologyHandler(uow, topoRepo, stateRepo, newTestLogger())
	cmd := makeIngestTopologyCmd()

	// First call: processes normally
	err := h.Handle(ctx, cmd, "dup-ingest-1")
	require.NoError(t, err)
	require.Len(t, topoRepo.applySnapshotCalls, 1)
	assert.Len(t, topoRepo.applySnapshotCalls[0], 3)

	// Reset call tracking
	topoRepo.applySnapshotCalls = nil
	uow.outboxRepo.CreatedEntries = nil
	uow.CommittedTx = false

	// Second call with same message ID: should be skipped
	err = h.Handle(ctx, cmd, "dup-ingest-1")
	require.NoError(t, err)

	assert.Len(t, topoRepo.applySnapshotCalls, 0, "ApplySnapshot must NOT be called for duplicate")
	assert.Len(t, uow.outboxRepo.CreatedEntries, 0, "no outbox entries for duplicate")
}
