package grpc

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	deadletterv1 "github.com/carolsimone/continuo/dead-letter-controller/api/deadletter/v1"
	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/dead-letter-controller/domain/repository"
	"github.com/carolsimone/continuo/dead-letter-controller/service/handlers"
	"github.com/carolsimone/continuo/dead-letter-controller/service/uow"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/pkg/maintenance"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// fakeRepo is an in-memory DeadLetterRepository keyed by id.
type fakeRepo struct {
	rows map[uuid.UUID]deadletter.DeadLetter
}

var _ repository.DeadLetterRepository = (*fakeRepo)(nil)

// putOpen stores an open, redrivable consumer dead letter whose original
// message was produced at originalAt, and returns it.
func (r *fakeRepo) putOpen(originalAt time.Time) deadletter.DeadLetter {
	dl := deadletter.DeadLetter{
		ID: uuid.New(), DedupKey: uuid.NewString(), Source: deadletter.SourceConsumer,
		FailureKind: model.DeadLetterKind("permanent"), Stream: "node.updated:v1", Group: "orchestrator-group",
		OriginalMessageID: "1-0", Producer: "state", Error: "boom", DeliveryCount: 3,
		Fields: map[string]string{"k": "v"}, Redrivable: true,
		OriginalAt: originalAt, RecordedAt: originalAt.Add(time.Minute), Status: deadletter.StatusOpen,
	}
	r.rows[dl.ID] = dl
	return dl
}

func (r *fakeRepo) Insert(_ context.Context, dl deadletter.DeadLetter) (bool, error) {
	r.rows[dl.ID] = dl
	return true, nil
}

func (r *fakeRepo) InsertBatch(ctx context.Context, dls []deadletter.DeadLetter) (int, error) {
	n := 0
	for _, dl := range dls {
		if ok, _ := r.Insert(ctx, dl); ok {
			n++
		}
	}
	return n, nil
}

func (r *fakeRepo) Get(_ context.Context, id uuid.UUID) (deadletter.DeadLetter, error) {
	dl, ok := r.rows[id]
	if !ok {
		return deadletter.DeadLetter{}, deadletter.ErrNotFound
	}
	return dl, nil
}

func (r *fakeRepo) List(_ context.Context, f deadletter.Filter) ([]deadletter.DeadLetter, error) {
	var out []deadletter.DeadLetter
	for _, dl := range r.rows {
		if dl.Status != f.Status || (f.Source != "" && dl.Source != f.Source) || (f.Stream != "" && dl.Stream != f.Stream) {
			continue
		}
		out = append(out, dl)
	}
	return out, nil
}

func (r *fakeRepo) CountOpen(_ context.Context) (int64, error) {
	var n int64
	for _, dl := range r.rows {
		if dl.Status == deadletter.StatusOpen {
			n++
		}
	}
	return n, nil
}

func (r *fakeRepo) LockForRedrive(_ context.Context, ids []uuid.UUID) ([]deadletter.DeadLetter, error) {
	var out []deadletter.DeadLetter
	for _, id := range ids {
		if dl, ok := r.rows[id]; ok {
			out = append(out, dl)
		}
	}
	return out, nil
}

func (r *fakeRepo) SaveRedrive(_ context.Context, dl deadletter.DeadLetter) error {
	r.rows[dl.ID] = dl
	return nil
}

func (r *fakeRepo) DeleteExpired(context.Context, time.Time, int) ([]deadletter.DeadLetter, error) {
	return nil, nil
}

func (r *fakeRepo) Backlog(context.Context) ([]deadletter.BacklogRow, error) { return nil, nil }

// fakeUoW commits straight into the fake repository.
type fakeUoW struct{ repo *fakeRepo }

var _ uow.UnitOfWork = (*fakeUoW)(nil)

func (u *fakeUoW) Begin(context.Context) error                  { return nil }
func (u *fakeUoW) Commit() error                                { return nil }
func (u *fakeUoW) Rollback() error                              { return nil }
func (u *fakeUoW) DeadLetters() repository.DeadLetterRepository { return u.repo }
func (u *fakeUoW) Outbox() outbox.Repository                    { return noopOutbox{} }

type noopOutbox struct{}

func (noopOutbox) Create(context.Context, *outbox.Entry) error                   { return nil }
func (noopOutbox) GetPendingBatch(context.Context, int) ([]*outbox.Entry, error) { return nil, nil }
func (noopOutbox) MarkProcessed(context.Context, uuid.UUID) error                { return nil }
func (noopOutbox) MarkProcessedBatch(context.Context, []uuid.UUID) error         { return nil }
func (noopOutbox) MarkFailed(context.Context, uuid.UUID, string) error           { return nil }
func (noopOutbox) IncrementRetry(context.Context, uuid.UUID) error               { return nil }

type clock struct{}

func (clock) Now() time.Time { return time.Now() }

type noopObserver struct{}

func (noopObserver) Recorded(deadletter.DeadLetter) {}
func (noopObserver) Redriven(deadletter.DeadLetter) {}
func (noopObserver) Expired(deadletter.DeadLetter)  {}

func newTestClient(t *testing.T) (deadletterv1.DeadLetterServiceClient, *fakeRepo) {
	return newTestClientWithMaintenance(t, false)
}

// newTestClientWithMaintenance serves the real handlers over the fakes through
// an in-memory listener, with the production interceptors and maintenance mode
// set to maintenanceOn, and returns a connected client and the fake repository.
func newTestClientWithMaintenance(t *testing.T, maintenanceOn bool) (deadletterv1.DeadLetterServiceClient, *fakeRepo) {
	t.Helper()
	repo := &fakeRepo{rows: map[uuid.UUID]deadletter.DeadLetter{}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	query := handlers.NewQuery(repo)
	redriver := handlers.NewRedriver(func() uow.UnitOfWork { return &fakeUoW{repo: repo} }, clock{}, noopObserver{}, logger)

	lis := bufconn.Listen(1 << 20)
	srv := newServer(lis, query, redriver, logger, maintenanceOn)
	go func() { _ = srv.Start() }()
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return deadletterv1.NewDeadLetterServiceClient(conn), repo
}

// During maintenance a redrive could re-inject a trigger or a release past
// every API gate, so it is refused; listing still works.
func TestMaintenance_RedriveRefusedListAllowed(t *testing.T) {
	c, repo := newTestClientWithMaintenance(t, true)
	open := repo.putOpen(time.Now().Add(-time.Hour))

	var trailer metadata.MD
	_, err := c.RedriveDeadLetters(ctxWithUser("alice"),
		&deadletterv1.RedriveDeadLettersRequest{Ids: []string{open.ID.String()}, Reason: "r"}, grpc.Trailer(&trailer))
	require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
	require.Equal(t, maintenance.Message, status.Convert(err).Message())
	require.Equal(t, []string{"true"}, trailer.Get(maintenance.TrailerKey))
	require.Equal(t, deadletter.StatusOpen, repo.rows[open.ID].Status, "the dead letter stays open")

	_, err = c.ListDeadLetters(context.Background(), &deadletterv1.ListDeadLettersRequest{})
	require.NoError(t, err)
}

func ctxWithUser(user string) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), identity.MetadataKey, user)
}

func TestRedrive_ConflictAndNotFoundCodes(t *testing.T) {
	c, repo := newTestClient(t)
	expired := repo.putOpen(time.Now().Add(-model.ReplayHorizon - time.Hour))
	unreadable := repo.putOpen(time.Now().Add(-time.Hour))
	unreadable.Redrivable = false
	repo.rows[unreadable.ID] = unreadable

	cases := []struct {
		name string
		req  *deadletterv1.RedriveDeadLettersRequest
		want codes.Code
	}{
		{"expired", &deadletterv1.RedriveDeadLettersRequest{Ids: []string{expired.ID.String()}, Reason: "r"}, codes.FailedPrecondition},
		{"not redrivable", &deadletterv1.RedriveDeadLettersRequest{Ids: []string{unreadable.ID.String()}, Reason: "r"}, codes.FailedPrecondition},
		{"unknown id", &deadletterv1.RedriveDeadLettersRequest{Ids: []string{uuid.NewString()}, Reason: "r"}, codes.NotFound},
		{"malformed id", &deadletterv1.RedriveDeadLettersRequest{Ids: []string{"not-a-uuid"}, Reason: "r"}, codes.InvalidArgument},
		{"blank reason", &deadletterv1.RedriveDeadLettersRequest{Ids: []string{expired.ID.String()}, Reason: " "}, codes.InvalidArgument},
		{"no ids", &deadletterv1.RedriveDeadLettersRequest{Reason: "r"}, codes.InvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := c.RedriveDeadLetters(ctxWithUser("alice"), tc.req)
			assert.Equal(t, tc.want, status.Code(err), "%v", err)
		})
	}
}

func TestRedrive_RecordsActorFromMetadata(t *testing.T) {
	c, repo := newTestClient(t)
	dl := repo.putOpen(time.Now().Add(-time.Hour))
	resp, err := c.RedriveDeadLetters(ctxWithUser("alice@example.com"), &deadletterv1.RedriveDeadLettersRequest{Ids: []string{dl.ID.String()}, Reason: "fixed"})
	require.NoError(t, err)
	require.Len(t, resp.DeadLetters, 1)
	assert.Equal(t, "alice@example.com", resp.DeadLetters[0].Redrive.Actor)
	assert.Equal(t, "fixed", resp.DeadLetters[0].Redrive.Reason)
	assert.Equal(t, "redriven", resp.DeadLetters[0].Status)
}

func TestRedrive_WithoutMetadataRecordsSystemActor(t *testing.T) {
	c, repo := newTestClient(t)
	dl := repo.putOpen(time.Now().Add(-time.Hour))
	resp, err := c.RedriveDeadLetters(context.Background(), &deadletterv1.RedriveDeadLettersRequest{Ids: []string{dl.ID.String()}, Reason: "fixed"})
	require.NoError(t, err)
	assert.Equal(t, identity.SystemUserID, resp.DeadLetters[0].Redrive.Actor)
}

func TestList_InvalidSourceIsInvalidArgument(t *testing.T) {
	c, _ := newTestClient(t)
	_, err := c.ListDeadLetters(context.Background(), &deadletterv1.ListDeadLettersRequest{Source: "bogus"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestList_InvalidStatusIsInvalidArgument(t *testing.T) {
	c, _ := newTestClient(t)
	_, err := c.ListDeadLetters(context.Background(), &deadletterv1.ListDeadLettersRequest{Status: "bogus"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestList_ReturnsOpenRowsAndTotal(t *testing.T) {
	c, repo := newTestClient(t)
	dl := repo.putOpen(time.Now().Add(-time.Hour))
	resp, err := c.ListDeadLetters(context.Background(), &deadletterv1.ListDeadLettersRequest{Source: "consumer"})
	require.NoError(t, err)
	require.Len(t, resp.DeadLetters, 1)
	assert.Equal(t, dl.ID.String(), resp.DeadLetters[0].Id)
	assert.Equal(t, "orchestrator-group", resp.DeadLetters[0].ConsumerGroup)
	assert.Equal(t, int64(1), resp.TotalOpen)
}

func TestGet_ReturnsFieldsAndExpiry(t *testing.T) {
	c, repo := newTestClient(t)
	dl := repo.putOpen(time.Now().Add(-time.Hour))
	resp, err := c.GetDeadLetter(context.Background(), &deadletterv1.GetDeadLetterRequest{Id: dl.ID.String()})
	require.NoError(t, err)
	assert.Equal(t, "v", resp.Fields["k"])
	assert.Equal(t, dl.ExpiresAt().UTC().Format(time.RFC3339), resp.DeadLetter.ExpiresAt)
}

func TestGet_UnknownIDIsNotFound(t *testing.T) {
	c, _ := newTestClient(t)
	_, err := c.GetDeadLetter(context.Background(), &deadletterv1.GetDeadLetterRequest{Id: uuid.NewString()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestGet_MalformedIDIsInvalidArgument(t *testing.T) {
	c, _ := newTestClient(t)
	_, err := c.GetDeadLetter(context.Background(), &deadletterv1.GetDeadLetterRequest{Id: "nope"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestToStatus_MapsEveryDomainError(t *testing.T) {
	cases := []struct {
		err  error
		want codes.Code
	}{
		{deadletter.ErrNotFound, codes.NotFound},
		{deadletter.ErrExpired, codes.FailedPrecondition},
		{deadletter.ErrNotRedrivable, codes.FailedPrecondition},
		{deadletter.ErrReasonRequired, codes.InvalidArgument},
		{deadletter.ErrNoIDs, codes.InvalidArgument},
		{deadletter.ErrInvalidFilter, codes.InvalidArgument},
		{io.ErrUnexpectedEOF, codes.Internal},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, status.Code(toStatus(tc.err)), "%v", tc.err)
	}
}

func TestNewServer_ListensAndShutsDown(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	repo := &fakeRepo{rows: map[uuid.UUID]deadletter.DeadLetter{}}
	srv, err := NewServer(0, handlers.NewQuery(repo), nil, logger, false)
	require.NoError(t, err)
	assert.NotEmpty(t, srv.Addr())

	served := make(chan error, 1)
	go func() { served <- srv.Start() }()

	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = deadletterv1.NewDeadLetterServiceClient(conn).ListDeadLetters(ctx, &deadletterv1.ListDeadLettersRequest{}, grpc.WaitForReady(true))
	require.NoError(t, err)

	require.NoError(t, srv.Shutdown(context.Background()))
	select {
	case err := <-served:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after Shutdown")
	}
}
