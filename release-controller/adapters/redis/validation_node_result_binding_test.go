package redis

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/carolsimone/continuo/release-controller/service/uow"
	goredis "github.com/redis/go-redis/v9"
)

// TestDecodeValidationNodeResult verifies that the validation.node.result:v1
// "payload" field decodes into handlers.NodeValidationResultInput.
func TestDecodeValidationNodeResult(t *testing.T) {
	rawPayload := `{
		"release_id":      "rel-1",
		"stage":            "validate",
		"node_id":          "core",
		"status":           "passed",
		"dbt_log_uri":      "s3://log",
		"run_results_uri":  "s3://results"
	}`

	msg := goredis.XMessage{
		ID: "0-1",
		Values: map[string]any{
			"payload": rawPayload,
		},
	}

	var in handlers.NodeValidationResultInput
	if err := decodePayload(msg, &in); err != nil {
		t.Fatalf("decodePayload: %v", err)
	}

	if in.ReleaseID != "rel-1" {
		t.Errorf("ReleaseID: want %q got %q", "rel-1", in.ReleaseID)
	}
	if in.Stage != "validate" {
		t.Errorf("Stage: want %q got %q", "validate", in.Stage)
	}
	if in.NodeID != "core" {
		t.Errorf("NodeID: want %q got %q", "core", in.NodeID)
	}
	if in.Status != "passed" {
		t.Errorf("Status: want %q got %q", "passed", in.Status)
	}
	if in.DBTLogURI != "s3://log" {
		t.Errorf("DBTLogURI: want %q got %q", "s3://log", in.DBTLogURI)
	}
	if in.RunResultsURI != "s3://results" {
		t.Errorf("RunResultsURI: want %q got %q", "s3://results", in.RunResultsURI)
	}
}

// TestValidationNodeResultHandlerAcksMalformedMessage verifies that a message
// with an undecodable payload is treated as a permanent failure: the handler
// returns nil so the stream consumer ACKs and drops it instead of retrying
// forever. deps is nil because decode failure short-circuits before deps is
// ever touched.
func TestValidationNodeResultHandlerAcksMalformedMessage(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := newValidationNodeResultHandler(nil, logger)

	msg := goredis.XMessage{
		ID: "0-1",
		Values: map[string]any{
			"payload": "not valid json",
		},
	}

	if err := handler(context.Background(), msg); err != nil {
		t.Fatalf("want nil (ack) for malformed payload, got %v", err)
	}
}

// TestValidationNodeResultHandlerDispatchesDecodedInput verifies that a
// well-formed message is decoded and handed to
// handlers.HandleNodeValidationResult with the fields intact — proven by a
// fake UnitOfWork whose ReleaseRepo.Load captures the release ID it was
// called with.
func TestValidationNodeResultHandlerDispatchesDecodedInput(t *testing.T) {
	repo := &fakeReleaseRepo{}
	u := &fakeUoW{releaseRepo: repo}
	deps := &handlers.Deps{
		NewUoW: func() uow.UnitOfWork { return u },
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := newValidationNodeResultHandler(deps, logger)

	payload := handlers.NodeValidationResultInput{
		ReleaseID: "rel-42",
		Stage:     "validate",
		NodeID:    "core",
		Status:    "passed",
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	msg := goredis.XMessage{
		ID: "0-2",
		Values: map[string]any{
			"payload": string(raw),
		},
	}

	if err := handler(context.Background(), msg); err != nil {
		t.Fatalf("want nil (unknown release is dropped, not an error), got %v", err)
	}
	if repo.loadedID != "rel-42" {
		t.Errorf("ReleaseRepo().Load called with %q, want %q — decoded input never reached the handler", repo.loadedID, "rel-42")
	}
}

// fakeUoW is a minimal uow.UnitOfWork stub. Only the methods exercised by
// HandleNodeValidationResult's unknown-release path (Begin, ReleaseRepo,
// Rollback) are implemented; the rest panic if ever called, which would
// indicate the test exercised more of the handler than intended.
type fakeUoW struct {
	releaseRepo repository.ReleaseRepository
}

func (f *fakeUoW) ReleaseRepo() repository.ReleaseRepository         { return f.releaseRepo }
func (f *fakeUoW) CurrentProdRepo() repository.CurrentProdRepository { panic("not implemented") }
func (f *fakeUoW) ServiceProdRepo() repository.ServiceProdRepository { panic("not implemented") }
func (f *fakeUoW) OutboxRepo() pkgoutbox.Repository                  { panic("not implemented") }
func (f *fakeUoW) MessageProcessingRepo() messageprocessing.Repository {
	panic("not implemented")
}
func (f *fakeUoW) Begin(ctx context.Context) error            { return nil }
func (f *fakeUoW) Commit() error                              { panic("not implemented") }
func (f *fakeUoW) Rollback() error                            { return nil }
func (f *fakeUoW) LockReleaseQueue(ctx context.Context) error { panic("not implemented") }

// fakeReleaseRepo is a minimal repository.ReleaseRepository stub. Only Load
// is implemented; it records the requested ID and reports the release as
// absent, which is enough to reach and verify the handler's dispatch without
// standing up a real release aggregate.
type fakeReleaseRepo struct {
	loadedID string
}

func (r *fakeReleaseRepo) Get(ctx context.Context, id string) (*release.Release, error) {
	panic("not implemented")
}
func (r *fakeReleaseRepo) Load(ctx context.Context, id string) (*release.Release, error) {
	r.loadedID = id
	return nil, nil
}
func (r *fakeReleaseRepo) Save(ctx context.Context, rel *release.Release) error {
	panic("not implemented")
}
func (r *fakeReleaseRepo) NextQueuedRelease(ctx context.Context) (*release.Release, error) {
	panic("not implemented")
}
func (r *fakeReleaseRepo) ActiveRelease(ctx context.Context) (*release.Release, error) {
	panic("not implemented")
}
func (r *fakeReleaseRepo) List(ctx context.Context, f repository.ListFilter) ([]*release.Release, *repository.ListCursor, error) {
	panic("not implemented")
}
func (r *fakeReleaseRepo) DeleteResolvedBefore(ctx context.Context, cutoff time.Time, keepReleaseIDs []string) (int, error) {
	panic("not implemented")
}
