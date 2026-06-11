package handlers_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// fakeOutboxRepository captures created outbox entries in memory.
// Set createErr to inject an error from Create for failure-path tests.
type fakeOutboxRepository struct {
	CreatedEntries []*pkgoutbox.Entry
	createErr      error
}

func (f *fakeOutboxRepository) Create(ctx context.Context, entry *pkgoutbox.Entry) error {
	if f.createErr != nil {
		return f.createErr
	}
	f.CreatedEntries = append(f.CreatedEntries, entry)
	return nil
}
func (f *fakeOutboxRepository) GetPendingBatch(ctx context.Context, limit int) ([]*pkgoutbox.Entry, error) {
	return nil, nil
}
func (f *fakeOutboxRepository) MarkProcessed(ctx context.Context, id uuid.UUID) error { return nil }
func (f *fakeOutboxRepository) MarkProcessedBatch(ctx context.Context, ids []uuid.UUID) error {
	return nil
}
func (f *fakeOutboxRepository) MarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error {
	return nil
}
func (f *fakeOutboxRepository) IncrementRetry(ctx context.Context, id uuid.UUID) error { return nil }

// fakeMessageProcessingRepository is an in-memory message_processing store
// keyed by messageID.
type fakeMessageProcessingRepository struct {
	messages map[string]*messageprocessing.MessageProcessing
}

func newFakeMessageProcessingRepository() *fakeMessageProcessingRepository {
	return &fakeMessageProcessingRepository{
		messages: make(map[string]*messageprocessing.MessageProcessing),
	}
}

func fakeMsgProcKey(messageID, streamName string) string {
	return messageID + "|" + streamName
}

func (f *fakeMessageProcessingRepository) InsertIfNotExists(ctx context.Context, msgProc *messageprocessing.MessageProcessing) (uuid.UUID, bool, error) {
	k := fakeMsgProcKey(msgProc.MessageID, msgProc.StreamName)
	if existing, ok := f.messages[k]; ok {
		return existing.ID, false, nil
	}
	msgProc.ID = uuid.New()
	f.messages[k] = msgProc
	return msgProc.ID, true, nil
}

func (f *fakeMessageProcessingRepository) GetByMessageIDAndStream(ctx context.Context, messageID, streamName string) (*messageprocessing.MessageProcessing, error) {
	msg, ok := f.messages[fakeMsgProcKey(messageID, streamName)]
	if !ok {
		return nil, nil
	}
	return msg, nil
}

func (f *fakeMessageProcessingRepository) GetByID(ctx context.Context, id uuid.UUID) (*messageprocessing.MessageProcessing, error) {
	for _, msg := range f.messages {
		if msg.ID == id {
			return msg, nil
		}
	}
	return nil, nil
}

func (f *fakeMessageProcessingRepository) UpdateState(ctx context.Context, id uuid.UUID, state string) error {
	for _, msg := range f.messages {
		if msg.ID == id {
			msg.State = state
			return nil
		}
	}
	return nil
}

func (f *fakeMessageProcessingRepository) DeleteTerminalOlderThan(_ context.Context, _ time.Duration, _ int) (int64, error) {
	return 0, nil
}

// fakeUnitOfWork is a no-op UnitOfWork backed by the in-memory outbox and
// message-processing repos.
type fakeUnitOfWork struct {
	outboxRepo   *fakeOutboxRepository
	msgProcRepo  *fakeMessageProcessingRepository
	BegunTx      bool
	CommittedTx  bool
	RolledBackTx bool
}

func newFakeUnitOfWork() *fakeUnitOfWork {
	return &fakeUnitOfWork{
		outboxRepo:  &fakeOutboxRepository{},
		msgProcRepo: newFakeMessageProcessingRepository(),
	}
}

func (f *fakeUnitOfWork) OutboxRepo() pkgoutbox.Repository { return f.outboxRepo }
func (f *fakeUnitOfWork) MessageProcessingRepo() messageprocessing.Repository {
	return f.msgProcRepo
}
func (f *fakeUnitOfWork) Begin(ctx context.Context) error { f.BegunTx = true; return nil }
func (f *fakeUnitOfWork) Commit() error                   { f.CommittedTx = true; return nil }
func (f *fakeUnitOfWork) Rollback() error                 { f.RolledBackTx = true; return nil }

// newTestLogger returns a debug-level text logger writing to stdout.
func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// ── fakes: repository.TopologyRepository ─────────────────────────────────────

// fakeTopologyRepository is an in-memory stub for repository.TopologyRepository.
type fakeTopologyRepository struct {
	setServiceMetadataCalls []setServiceMetadataCall
	setServiceMetadataErr   error
}

type setServiceMetadataCall struct {
	ServiceMetadata    map[string]map[string]string
	TopologyGeneration int64
}

func (f *fakeTopologyRepository) SetServiceMetadata(_ context.Context, serviceMetadata map[string]map[string]string, topologyGeneration int64) error {
	f.setServiceMetadataCalls = append(f.setServiceMetadataCalls, setServiceMetadataCall{
		ServiceMetadata:    serviceMetadata,
		TopologyGeneration: topologyGeneration,
	})
	return f.setServiceMetadataErr
}

var _ repository.TopologyRepository = (*fakeTopologyRepository)(nil)

// ── fakes: repository.TopologyStateRepository ────────────────────────────────

type fakeTopologyStateRepository struct {
	generation             int64
	incrementGenerationErr error
	getGenerationErr       error
	incrementCalls         int
	getCalls               int
}

func (f *fakeTopologyStateRepository) IncrementGeneration(_ context.Context) (int64, error) {
	f.incrementCalls++
	if f.incrementGenerationErr != nil {
		return 0, f.incrementGenerationErr
	}
	f.generation++
	return f.generation, nil
}

func (f *fakeTopologyStateRepository) GetGeneration(_ context.Context) (int64, error) {
	f.getCalls++
	if f.getGenerationErr != nil {
		return 0, f.getGenerationErr
	}
	return f.generation, nil
}

var _ repository.TopologyStateRepository = (*fakeTopologyStateRepository)(nil)

// ── integration test infrastructure ──────────────────────────────────────────

func commandTestNeo4jURI() string {
	if u := os.Getenv("NEO4J_URI"); u != "" {
		return u
	}
	return "bolt://neo4j:7687"
}

func commandTestNeo4jUser() string {
	if u := os.Getenv("NEO4J_USER"); u != "" {
		return u
	}
	return "neo4j"
}

func commandTestNeo4jPassword() string {
	if p := os.Getenv("NEO4J_PASSWORD"); p != "" {
		return p
	}
	return "neo4j"
}

// newCommandTestNeo4jClient creates a Neo4j client for integration tests,
// skipping if Neo4j is unavailable.
func newCommandTestNeo4jClient(t *testing.T) neo4jinfra.Neo4jClient {
	t.Helper()
	client, err := neo4jinfra.NewNeo4jClient(
		commandTestNeo4jURI(),
		commandTestNeo4jUser(),
		commandTestNeo4jPassword(),
		newTestLogger(),
	)
	if err != nil {
		t.Skipf("Neo4j unavailable, skipping integration test: %v", err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return client
}

// newCommandTestDB creates a Postgres connection for integration tests,
// skipping if Postgres is unavailable.
func newCommandTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	host := os.Getenv("POSTGRES_HOST")
	if host == "" {
		host = "postgres"
	}
	port := os.Getenv("POSTGRES_PORT")
	if port == "" {
		port = "5432"
	}
	db, err := sqlx.Connect("postgres", fmt.Sprintf(
		"host=%s port=%s dbname=continuo_orchestrator user=continuo_svc password=continuo sslmode=disable",
		host, port,
	))
	if err != nil {
		t.Skipf("Postgres unavailable: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}
