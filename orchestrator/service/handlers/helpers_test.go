package handlers_test

import (
	"context"
	"log/slog"
	"os"

	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/google/uuid"
)

// fakeOutboxRepository captures created outbox entries in memory.
type fakeOutboxRepository struct {
	CreatedEntries []*pkgoutbox.Entry
}

func (f *fakeOutboxRepository) Create(ctx context.Context, entry *pkgoutbox.Entry) error {
	f.CreatedEntries = append(f.CreatedEntries, entry)
	return nil
}
func (f *fakeOutboxRepository) GetPendingBatch(ctx context.Context, limit int) ([]*pkgoutbox.Entry, error) {
	return nil, nil
}
func (f *fakeOutboxRepository) MarkProcessed(ctx context.Context, id uuid.UUID) error { return nil }
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
