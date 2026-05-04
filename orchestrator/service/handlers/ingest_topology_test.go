package handlers_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	postgresadapter "github.com/carolsimone/continuo/orchestrator/adapters/postgres"
	domainCmd "github.com/carolsimone/continuo/orchestrator/domain/command"
	"github.com/carolsimone/continuo/orchestrator/domain/topology"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	"github.com/carolsimone/continuo/pkg/events"
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

// ── fakes: RejectedTopologyRepository ────────────────────────────────────────

type fakeRejectedTopologyRepo struct {
	InsertCalls []rejectedInsertCall
	InsertErr   error
}

type rejectedInsertCall struct {
	MessageID string
	Reason    string
	Payload   json.RawMessage
}

func (f *fakeRejectedTopologyRepo) Insert(_ context.Context, messageID, reason string, payload json.RawMessage) error {
	f.InsertCalls = append(f.InsertCalls, rejectedInsertCall{MessageID: messageID, Reason: reason, Payload: payload})
	return f.InsertErr
}

var _ postgresadapter.RejectedTopologyRepository = (*fakeRejectedTopologyRepo)(nil)

// ── tests ─────────────────────────────────────────────────────────────────────

// 1. 3 nodes across 2 schedules → ApplySnapshot called once, outbox entry created with both schedule names.
func TestIngestTopology_ThreeNodesTwoSchedules(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	topoRepo := &fakeTopologyRepository{}
	stateRepo := &fakeTopologyStateRepository{}
	rejectedRepo := &fakeRejectedTopologyRepo{}

	h := handlers.NewIngestTopologyHandler(uow, topoRepo, stateRepo, rejectedRepo, newTestLogger())
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
	rejectedRepo := &fakeRejectedTopologyRepo{}

	h := handlers.NewIngestTopologyHandler(uow, topoRepo, stateRepo, rejectedRepo, newTestLogger())
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

// ── rejection-path tests (Task A2.4) ─────────────────────────────────────────

func TestIngestTopology_BadPayload_ReturnsWrappedPermanent(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	topoRepo := &fakeTopologyRepository{}
	stateRepo := &fakeTopologyStateRepository{}
	rejectedRepo := &fakeRejectedTopologyRepo{}

	h := handlers.NewIngestTopologyHandler(uow, topoRepo, stateRepo, rejectedRepo, newTestLogger())
	cmd := domainCmd.IngestTopologyCmd{Nodes: []domainCmd.TopologyNodePayload{
		{ServiceName: "svc-1", SchemaName: "raw", TableName: "users", ImageTag: ""},
	}}

	err := h.Handle(ctx, cmd, "msg-bad-1")
	if !errors.Is(err, events.ErrPermanent) {
		t.Fatalf("expected wrapped ErrPermanent, got %v", err)
	}
}

func TestIngestTopology_BadPayload_SkipsUnitOfWork(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	topoRepo := &fakeTopologyRepository{}
	stateRepo := &fakeTopologyStateRepository{}
	rejectedRepo := &fakeRejectedTopologyRepo{}

	h := handlers.NewIngestTopologyHandler(uow, topoRepo, stateRepo, rejectedRepo, newTestLogger())
	cmd := domainCmd.IngestTopologyCmd{Nodes: []domainCmd.TopologyNodePayload{
		{ServiceName: "svc-1", SchemaName: "raw", TableName: "users", ImageTag: ""},
	}}

	_ = h.Handle(ctx, cmd, "msg-bad-2")

	if uow.BegunTx {
		t.Fatalf("expected uow.Begin not called, got BegunTx=true")
	}
	if uow.CommittedTx {
		t.Fatalf("expected uow.Commit not called, got CommittedTx=true")
	}
}

func TestIngestTopology_BadPayload_WritesForensicsRow(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	topoRepo := &fakeTopologyRepository{}
	stateRepo := &fakeTopologyStateRepository{}
	rejectedRepo := &fakeRejectedTopologyRepo{}

	h := handlers.NewIngestTopologyHandler(uow, topoRepo, stateRepo, rejectedRepo, newTestLogger())
	cmd := domainCmd.IngestTopologyCmd{Nodes: []domainCmd.TopologyNodePayload{
		{ServiceName: "svc-1", SchemaName: "raw", TableName: "users", ImageTag: ""},
	}}

	_ = h.Handle(ctx, cmd, "msg-bad-3")

	if got := len(rejectedRepo.InsertCalls); got != 1 {
		t.Fatalf("expected exactly 1 forensics insert, got %d", got)
	}
	call := rejectedRepo.InsertCalls[0]
	if call.MessageID != "msg-bad-3" {
		t.Fatalf("expected message_id=msg-bad-3, got %q", call.MessageID)
	}
	if !strings.Contains(call.Reason, "svc-1/raw/users") {
		t.Fatalf("expected offender in reason, got %q", call.Reason)
	}
	// Sanity: payload should be valid JSON containing the original cmd shape.
	if len(call.Payload) == 0 {
		t.Fatal("expected non-empty payload")
	}
}

func TestIngestTopology_BadPayload_SkipsApplySnapshot(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	topoRepo := &fakeTopologyRepository{}
	stateRepo := &fakeTopologyStateRepository{}
	rejectedRepo := &fakeRejectedTopologyRepo{}

	h := handlers.NewIngestTopologyHandler(uow, topoRepo, stateRepo, rejectedRepo, newTestLogger())
	cmd := domainCmd.IngestTopologyCmd{Nodes: []domainCmd.TopologyNodePayload{
		{ServiceName: "svc-1", SchemaName: "raw", TableName: "users", ImageTag: ""},
	}}

	_ = h.Handle(ctx, cmd, "msg-bad-4")

	if got := len(topoRepo.applySnapshotCalls); got != 0 {
		t.Fatalf("expected ApplySnapshot not called, got %d call(s)", got)
	}
}

func TestIngestTopology_BadPayload_ForensicsFailureStillReturnsPermanent(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	topoRepo := &fakeTopologyRepository{}
	stateRepo := &fakeTopologyStateRepository{}
	rejectedRepo := &fakeRejectedTopologyRepo{InsertErr: errors.New("postgres blip")}

	h := handlers.NewIngestTopologyHandler(uow, topoRepo, stateRepo, rejectedRepo, newTestLogger())
	cmd := domainCmd.IngestTopologyCmd{Nodes: []domainCmd.TopologyNodePayload{
		{ServiceName: "svc-1", SchemaName: "raw", TableName: "users", ImageTag: ""},
	}}

	err := h.Handle(ctx, cmd, "msg-bad-5")
	if !errors.Is(err, events.ErrPermanent) {
		t.Fatalf("expected wrapped ErrPermanent (forensics is best-effort), got %v", err)
	}
}

func TestIngestTopology_HappyPath_DoesNotInsertForensics(t *testing.T) {
	ctx := context.Background()
	uow := newFakeUnitOfWork()
	topoRepo := &fakeTopologyRepository{}
	stateRepo := &fakeTopologyStateRepository{}
	rejectedRepo := &fakeRejectedTopologyRepo{}

	h := handlers.NewIngestTopologyHandler(uow, topoRepo, stateRepo, rejectedRepo, newTestLogger())
	cmd := makeIngestTopologyCmd() // helper at line 56 — all nodes have non-empty image_tag

	if err := h.Handle(ctx, cmd, "msg-ok"); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if got := len(rejectedRepo.InsertCalls); got != 0 {
		t.Fatalf("expected no forensics inserts on happy path, got %d", got)
	}
}
