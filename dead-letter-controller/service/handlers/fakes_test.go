package handlers

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/dead-letter-controller/domain/repository"
	"github.com/carolsimone/continuo/dead-letter-controller/service/uow"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
)

// fakeRepo is an in-memory DeadLetterRepository: a map keyed by id, plus a
// dedup-key index so Insert keeps the first row per DedupKey. It doubles as the
// committed store the fake unit of work writes into on Commit.
type fakeRepo struct {
	rows       map[uuid.UUID]deadletter.DeadLetter
	byDedup    map[string]uuid.UUID
	insertErr  error
	lastFilter deadletter.Filter
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{rows: map[uuid.UUID]deadletter.DeadLetter{}, byDedup: map[string]uuid.UUID{}}
}

var _ repository.DeadLetterRepository = (*fakeRepo)(nil)

// put stores dl unconditionally (test helper, not part of the port).
func (r *fakeRepo) put(dl deadletter.DeadLetter) {
	r.rows[dl.ID] = dl
	if dl.DedupKey != "" {
		r.byDedup[dl.DedupKey] = dl.ID
	}
}

func (r *fakeRepo) get(id uuid.UUID) deadletter.DeadLetter { return r.rows[id] }
func (r *fakeRepo) has(id uuid.UUID) bool                  { _, ok := r.rows[id]; return ok }
func (r *fakeRepo) count() int                             { return len(r.rows) }

func (r *fakeRepo) Insert(_ context.Context, dl deadletter.DeadLetter) (bool, error) {
	if r.insertErr != nil {
		return false, r.insertErr
	}
	if _, ok := r.byDedup[dl.DedupKey]; ok {
		return false, nil
	}
	r.put(dl)
	return true, nil
}

func (r *fakeRepo) InsertBatch(ctx context.Context, dls []deadletter.DeadLetter) (int, error) {
	n := 0
	for _, dl := range dls {
		ok, err := r.Insert(ctx, dl)
		if err != nil {
			return n, err
		}
		if ok {
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
	r.lastFilter = f
	var out []deadletter.DeadLetter
	for _, dl := range r.rows {
		if f.Status != "" && dl.Status != f.Status {
			continue
		}
		if f.Source != "" && dl.Source != f.Source {
			continue
		}
		if f.Stream != "" && dl.Stream != f.Stream {
			continue
		}
		if f.Limit > 0 && len(out) >= f.Limit {
			break
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
	r.put(dl)
	return nil
}

func (r *fakeRepo) DeleteExpired(_ context.Context, originalBefore time.Time, limit int) ([]deadletter.DeadLetter, error) {
	var gone []deadletter.DeadLetter
	for id, dl := range r.rows {
		if len(gone) >= limit {
			break
		}
		if dl.OriginalAt.Before(originalBefore) {
			gone = append(gone, dl)
			delete(r.rows, id)
			delete(r.byDedup, dl.DedupKey)
		}
	}
	return gone, nil
}

func (r *fakeRepo) Backlog(_ context.Context) ([]deadletter.BacklogRow, error) { return nil, nil }

// fakeOutbox is the committed outbox store: the entries Commit has flushed.
type fakeOutbox struct {
	created []*outbox.Entry
}

// txState buffers the writes of one in-flight transaction.
type txState struct {
	saved   []deadletter.DeadLetter
	created []*outbox.Entry
}

// fakeUoW is one reusable unit of work over the committed fakeRepo and
// fakeOutbox. Begin opens a fresh buffer, Commit flushes it into the committed
// stores, Rollback discards it. Each is counted.
type fakeUoW struct {
	repo   *fakeRepo
	outbox *fakeOutbox

	begins    int
	commits   int
	rollbacks int

	tx *txState
}

var _ uow.UnitOfWork = (*fakeUoW)(nil)

func (u *fakeUoW) Begin(_ context.Context) error {
	u.begins++
	u.tx = &txState{}
	return nil
}

func (u *fakeUoW) Commit() error {
	u.commits++
	for _, dl := range u.tx.saved {
		u.repo.put(dl)
	}
	u.outbox.created = append(u.outbox.created, u.tx.created...)
	u.tx = nil
	return nil
}

func (u *fakeUoW) Rollback() error {
	u.rollbacks++
	u.tx = nil
	return nil
}

func (u *fakeUoW) DeadLetters() repository.DeadLetterRepository {
	return &txRepo{repo: u.repo, tx: u.tx}
}
func (u *fakeUoW) Outbox() outbox.Repository { return &txOutbox{tx: u.tx} }

// txRepo reads committed rows but buffers SaveRedrive in the transaction.
type txRepo struct {
	repo *fakeRepo
	tx   *txState
}

var _ repository.DeadLetterRepository = (*txRepo)(nil)

func (r *txRepo) Insert(ctx context.Context, dl deadletter.DeadLetter) (bool, error) {
	return r.repo.Insert(ctx, dl)
}
func (r *txRepo) InsertBatch(ctx context.Context, dls []deadletter.DeadLetter) (int, error) {
	return r.repo.InsertBatch(ctx, dls)
}
func (r *txRepo) Get(ctx context.Context, id uuid.UUID) (deadletter.DeadLetter, error) {
	return r.repo.Get(ctx, id)
}
func (r *txRepo) List(ctx context.Context, f deadletter.Filter) ([]deadletter.DeadLetter, error) {
	return r.repo.List(ctx, f)
}
func (r *txRepo) CountOpen(ctx context.Context) (int64, error) { return r.repo.CountOpen(ctx) }
func (r *txRepo) LockForRedrive(ctx context.Context, ids []uuid.UUID) ([]deadletter.DeadLetter, error) {
	return r.repo.LockForRedrive(ctx, ids)
}
func (r *txRepo) SaveRedrive(_ context.Context, dl deadletter.DeadLetter) error {
	r.tx.saved = append(r.tx.saved, dl)
	return nil
}
func (r *txRepo) DeleteExpired(ctx context.Context, originalBefore time.Time, limit int) ([]deadletter.DeadLetter, error) {
	return r.repo.DeleteExpired(ctx, originalBefore, limit)
}
func (r *txRepo) Backlog(ctx context.Context) ([]deadletter.BacklogRow, error) {
	return r.repo.Backlog(ctx)
}

// txOutbox buffers the entries a transaction creates.
type txOutbox struct {
	tx *txState
}

var _ outbox.Repository = (*txOutbox)(nil)

func (o *txOutbox) Create(_ context.Context, entry *outbox.Entry) error {
	o.tx.created = append(o.tx.created, entry)
	return nil
}
func (o *txOutbox) GetPendingBatch(context.Context, int) ([]*outbox.Entry, error) { return nil, nil }
func (o *txOutbox) MarkProcessed(context.Context, uuid.UUID) error                { return nil }
func (o *txOutbox) MarkProcessedBatch(context.Context, []uuid.UUID) error         { return nil }
func (o *txOutbox) MarkFailed(context.Context, uuid.UUID, string) error           { return nil }
func (o *txOutbox) IncrementRetry(context.Context, uuid.UUID) error               { return nil }

// fixedClock returns a constant time.
type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

// recordingObserver keeps every notification for assertions.
type recordingObserver struct {
	recorded []deadletter.DeadLetter
	redriven []deadletter.DeadLetter
	expired  []deadletter.DeadLetter
}

func (o *recordingObserver) Recorded(dl deadletter.DeadLetter) { o.recorded = append(o.recorded, dl) }
func (o *recordingObserver) Redriven(dl deadletter.DeadLetter) { o.redriven = append(o.redriven, dl) }
func (o *recordingObserver) Expired(dl deadletter.DeadLetter)  { o.expired = append(o.expired, dl) }

// harness wires the fakes to the four handlers, with the clock fixed at now.
type harness struct {
	repo     *fakeRepo
	outbox   *fakeOutbox
	uow      *fakeUoW
	obs      *recordingObserver
	recorder *Recorder
	query    *Query
	redriver *Redriver
	expirer  *Expirer
}

func newHarness() *harness {
	repo := newFakeRepo()
	ob := &fakeOutbox{}
	obs := &recordingObserver{}
	unit := &fakeUoW{repo: repo, outbox: ob}
	clock := fixedClock{t: now}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &harness{
		repo:     repo,
		outbox:   ob,
		uow:      unit,
		obs:      obs,
		recorder: NewRecorder(repo, obs, logger),
		query:    NewQuery(repo),
		redriver: NewRedriver(func() uow.UnitOfWork { return unit }, clock, obs, logger),
		expirer:  NewExpirer(repo, clock, obs, logger),
	}
}
