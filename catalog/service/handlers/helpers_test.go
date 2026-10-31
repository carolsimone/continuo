package handlers_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/carolsimone/continuo/catalog/domain/repository"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
)

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
	topology     repository.ReleasePromotionRepository
	codeVersions repository.CodeVersionRepository
	caseBase     repository.CaseBaseRepository
}

func (t *fakeGraphTx) Topology() repository.ReleasePromotionRepository { return t.topology }
func (t *fakeGraphTx) CodeVersions() repository.CodeVersionRepository  { return t.codeVersions }
func (t *fakeGraphTx) CaseBase() repository.CaseBaseRepository         { return t.caseBase }

var _ uow.Tx = (*fakeGraphTx)(nil)
