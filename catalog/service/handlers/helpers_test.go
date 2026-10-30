package handlers_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/repository"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/catalog/service/uow"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/google/uuid"
)

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

func (f *fakeMessageProcessingRepository) AlreadyProcessed(_ context.Context, _, _ string, _ *uuid.UUID) (bool, error) {
	return false, nil
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

func (f *fakeMessageProcessingRepository) DeleteOlderThan(_ context.Context, _ time.Duration, _ int) (int64, error) {
	return 0, nil
}

// fakeUnitOfWork is a no-op PostgresUnitOfWork backed by the in-memory
// message-processing repo.
type fakeUnitOfWork struct {
	msgProcRepo  *fakeMessageProcessingRepository
	BegunTx      bool
	CommittedTx  bool
	RolledBackTx bool
}

func newFakeUnitOfWork() *fakeUnitOfWork {
	return &fakeUnitOfWork{
		msgProcRepo: newFakeMessageProcessingRepository(),
	}
}

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

// ── fakes: ports.TopologyArtifactReader ──────────────────────────────────────

// fakeArtifactReader serves topology artifacts from memory, keyed by URI. err,
// when set, is returned for every read.
type fakeArtifactReader struct {
	docs  map[string]topologyartifact.Document
	err   error
	calls []string
}

func (f *fakeArtifactReader) Load(_ context.Context, uri, _ string) (topologyartifact.Document, error) {
	f.calls = append(f.calls, uri)
	if f.err != nil {
		return topologyartifact.Document{}, f.err
	}
	doc, ok := f.docs[uri]
	if !ok {
		return topologyartifact.Document{}, fmt.Errorf("%w: %s", ports.ErrTopologyArtifactNotFound, uri)
	}
	return doc, nil
}

var _ ports.TopologyArtifactReader = (*fakeArtifactReader)(nil)

// artifactFor registers doc under the URI its release's artifact has and
// returns that URI, so a test can build a promotion that names it.
func (f *fakeArtifactReader) artifactFor(doc topologyartifact.Document) string {
	if f.docs == nil {
		f.docs = map[string]topologyartifact.Document{}
	}
	uri := "s3://continuo/" + topologyartifact.Key(doc.TenantID, doc.ReleaseID)
	f.docs[uri] = doc
	return uri
}

// ── fakes: uow.UnitOfWork ─────────────────────────────────────────────────────

// fakeGraphUnitOfWork is an in-memory uow.UnitOfWork. Write runs fn on tx
// unless the message's marker is recorded, and records the marker when the
// write is final and fn succeeds, as the Neo4j unit of work does in one
// transaction. processedErr and writeErr, when set, are what Processed and
// Write return before they do anything else.
type fakeGraphUnitOfWork struct {
	tx           *fakeGraphTx
	recorded     map[string]bool
	processedErr error
	writeErr     error
	processed    []uow.Marker
	writes       []fakeGraphWrite
}

// fakeGraphWrite is one Write call.
type fakeGraphWrite struct {
	Marker uow.Marker
	Final  bool
}

func newFakeGraphUnitOfWork(topology repository.ReleasePromotionRepository) *fakeGraphUnitOfWork {
	return &fakeGraphUnitOfWork{tx: &fakeGraphTx{topology: topology}, recorded: map[string]bool{}}
}

// markerKeys are the identities a marker is found by: its message id and,
// when it carries one, its outbox entry id, each within its tenant and group.
func markerKeys(m uow.Marker) []string {
	keys := []string{m.TenantID + "|" + m.Group + "|message|" + m.MessageID}
	if m.OutboxEntryID != nil {
		keys = append(keys, m.TenantID+"|"+m.Group+"|outbox|"+m.OutboxEntryID.String())
	}
	return keys
}

// record stores m as processed.
func (f *fakeGraphUnitOfWork) record(m uow.Marker) {
	for _, k := range markerKeys(m) {
		f.recorded[k] = true
	}
}

func (f *fakeGraphUnitOfWork) isRecorded(m uow.Marker) bool {
	for _, k := range markerKeys(m) {
		if f.recorded[k] {
			return true
		}
	}
	return false
}

func (f *fakeGraphUnitOfWork) Processed(_ context.Context, m uow.Marker) (bool, error) {
	f.processed = append(f.processed, m)
	if f.processedErr != nil {
		return false, f.processedErr
	}
	return f.isRecorded(m), nil
}

func (f *fakeGraphUnitOfWork) Write(ctx context.Context, m uow.Marker, final bool, fn func(ctx context.Context, tx uow.Tx) error) error {
	f.writes = append(f.writes, fakeGraphWrite{Marker: m, Final: final})
	if f.writeErr != nil {
		return f.writeErr
	}
	if f.isRecorded(m) {
		return nil
	}
	if err := fn(ctx, f.tx); err != nil {
		return err
	}
	if final {
		f.record(m)
	}
	return nil
}

var _ uow.UnitOfWork = (*fakeGraphUnitOfWork)(nil)

// fakeGraphTx hands out the fake repositories a test wires in.
type fakeGraphTx struct {
	topology repository.ReleasePromotionRepository
}

func (t *fakeGraphTx) Topology() repository.ReleasePromotionRepository { return t.topology }
func (t *fakeGraphTx) CodeVersions() repository.CodeVersionRepository  { return nil }
func (t *fakeGraphTx) CaseBase() repository.CaseBaseRepository         { return nil }

var _ uow.Tx = (*fakeGraphTx)(nil)
