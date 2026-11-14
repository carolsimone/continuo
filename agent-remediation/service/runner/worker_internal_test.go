package runner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// leaseLog is a remediation repository that hands out one remediation and
// records the lease calls made on it.
type leaseLog struct {
	repository.RemediationRepository
	mu        sync.Mutex
	id        uuid.UUID
	claims    []time.Time
	pending   int
	holds     []time.Duration
	holdOwner []string
	holdErr   func(n int) error
	released  int
}

func (l *leaseLog) ClaimNext(_ context.Context, owner string, _ time.Time, lease time.Duration) (uuid.UUID, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.claims = append(l.claims, time.Now())
	if l.pending == 0 {
		return uuid.Nil, false, nil
	}
	l.pending--
	l.holdOwner = append(l.holdOwner, owner)
	l.holds = append(l.holds, lease)
	return l.id, true, nil
}

func (l *leaseLog) Hold(_ context.Context, _ uuid.UUID, owner string, _ time.Time, lease time.Duration) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.holdOwner = append(l.holdOwner, owner)
	l.holds = append(l.holds, lease)
	if l.holdErr != nil {
		return l.holdErr(len(l.holds))
	}
	return nil
}

func (l *leaseLog) ReleaseLease(context.Context, uuid.UUID, string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.released++
	return nil
}

func (l *leaseLog) snapshot() (holds int, released int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.holds), l.released
}

type fixedClock struct{}

func (fixedClock) Now() time.Time { return time.Date(2026, 11, 13, 9, 0, 0, 0, time.UTC) }

var _ ports.Clock = fixedClock{}

func testWorker(repo *leaseLog, drive func(context.Context, uuid.UUID) error) *Worker {
	w := NewWorker(&Runner{}, func() repository.RemediationRepository { return repo }, "owner-1", fixedClock{},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.drive = drive
	w.stuck = func(context.Context, uuid.UUID) (bool, error) { return false, nil }
	w.heartbeat = 5 * time.Millisecond
	w.idleWait = 5 * time.Millisecond
	return w
}

func runUntil(t *testing.T, w *Worker, done func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		w.Run(ctx)
	}()
	require.Eventually(t, done, 5*time.Second, time.Millisecond)
	cancel()
	<-finished
}

// While a drive runs, the worker renews its lease every third of the lease
// duration, so a model call that outlasts one lease cannot let a second worker
// claim the remediation and call the model again.
func TestWorker_RenewsTheLeaseWhileADriveRuns(t *testing.T) {
	repo := &leaseLog{id: uuid.New(), pending: 1}
	renewed := make(chan struct{})
	var once sync.Once
	w := testWorker(repo, func(ctx context.Context, _ uuid.UUID) error {
		for {
			if h, _ := repo.snapshot(); h >= 4 {
				once.Do(func() { close(renewed) })
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Millisecond):
			}
		}
	})
	runUntil(t, w, func() bool {
		select {
		case <-renewed:
			_, released := repo.snapshot()
			return released == 1
		default:
			return false
		}
	})

	repo.mu.Lock()
	defer repo.mu.Unlock()
	assert.Equal(t, leaseDuration, repo.holds[1], "each renewal asks for a whole lease")
	for _, o := range repo.holdOwner {
		assert.Equal(t, "owner-1", o)
	}
}

func TestWorker_RenewsAtAThirdOfTheLease(t *testing.T) {
	w := NewWorker(&Runner{}, nil, "o", fixedClock{}, slog.Default())
	assert.Equal(t, leaseDuration/3, w.heartbeat)
	assert.Equal(t, 2*time.Second, w.idleWait)
}

// The gate poller holds the lease of the remediation it polls at the worker's
// pace.
func TestGatePoller_RenewsAtAThirdOfTheLease(t *testing.T) {
	p := NewGatePoller(&Runner{}, nil, "o", time.Second, nil, fixedClock{}, slog.Default())
	assert.Equal(t, leaseDuration/3, p.heartbeat)
	assert.Equal(t, leaseDuration/3, p.lease().heartbeat)
}

// A lease another worker took stops the drive at once: its context is
// cancelled, and the worker does not renew again.
func TestWorker_StopsTheDriveWhenTheLeaseIsLost(t *testing.T) {
	repo := &leaseLog{id: uuid.New(), pending: 1}
	repo.holdErr = func(n int) error {
		if n >= 3 {
			return repository.ErrLeaseLost
		}
		return nil
	}
	var cancelled bool
	var mu sync.Mutex
	w := testWorker(repo, func(ctx context.Context, _ uuid.UUID) error {
		select {
		case <-ctx.Done():
			mu.Lock()
			cancelled = true
			mu.Unlock()
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return errors.New("the drive was never stopped")
		}
	})
	runUntil(t, w, func() bool { mu.Lock(); defer mu.Unlock(); return cancelled })

	holds, _ := repo.snapshot()
	time.Sleep(30 * time.Millisecond)
	after, _ := repo.snapshot()
	assert.Equal(t, holds, after, "no renewal after the lease was lost")
}

// A renewal that fails for another reason is retried at the next tick, with the
// drive left running.
func TestWorker_KeepsDrivingThroughAFailedRenewal(t *testing.T) {
	repo := &leaseLog{id: uuid.New(), pending: 1}
	repo.holdErr = func(n int) error {
		if n == 2 {
			return errors.New("database blip")
		}
		return nil
	}
	finished := make(chan struct{})
	w := testWorker(repo, func(ctx context.Context, _ uuid.UUID) error {
		for {
			if h, _ := repo.snapshot(); h >= 5 {
				close(finished)
				return nil
			}
			select {
			case <-ctx.Done():
				return errors.New("the drive was stopped by a failed renewal")
			case <-time.After(time.Millisecond):
			}
		}
	})
	runUntil(t, w, func() bool {
		select {
		case <-finished:
			return true
		default:
			return false
		}
	})
}

// After a drive that errors, the worker still holds the lease and checks
// whether the remediation can go no further. If it can, the remediation is
// ended and the lease let go.
func TestWorker_EndsARemediationWhoseDriveFailedAndCanGoNoFurther(t *testing.T) {
	repo := &leaseLog{id: uuid.New(), pending: 1}
	w := testWorker(repo, func(context.Context, uuid.UUID) error { return errors.New("s3 is down") })
	var checked []uuid.UUID
	var mu sync.Mutex
	w.stuck = func(_ context.Context, id uuid.UUID) (bool, error) {
		mu.Lock()
		defer mu.Unlock()
		checked = append(checked, id)
		return true, nil
	}
	runUntil(t, w, func() bool { _, released := repo.snapshot(); return released == 1 })

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []uuid.UUID{repo.id}, checked)
	repo.mu.Lock()
	defer repo.mu.Unlock()
	for _, d := range repo.holds {
		assert.NotEqual(t, ParkDuration, d, "an ended remediation is not parked")
	}
}

// A remediation whose drive failed and that can still go on is parked: the
// worker holds its lease for two more minutes instead of letting it go, so no
// worker claims it before then.
func TestWorker_ParksARemediationWhoseDriveFailed(t *testing.T) {
	repo := &leaseLog{id: uuid.New(), pending: 1}
	w := testWorker(repo, func(context.Context, uuid.UUID) error { return errors.New("s3 is down") })
	runUntil(t, w, func() bool {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		return slices.Contains(repo.holds, ParkDuration)
	})

	_, released := repo.snapshot()
	assert.Zero(t, released, "a parked remediation keeps its lease")
	assert.Equal(t, 2*time.Minute, ParkDuration)
}

// A failed drive is parked as well when the check itself failed.
func TestWorker_ParksARemediationWhenTheStuckCheckFails(t *testing.T) {
	repo := &leaseLog{id: uuid.New(), pending: 1}
	w := testWorker(repo, func(context.Context, uuid.UUID) error { return errors.New("s3 is down") })
	w.stuck = func(context.Context, uuid.UUID) (bool, error) { return false, errors.New("database down") }
	runUntil(t, w, func() bool {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		return slices.Contains(repo.holds, ParkDuration)
	})
}

// A drive that ends because the lease was lost says nothing about the
// remediation: it is neither checked nor parked.
func TestWorker_DoesNotCheckADriveStoppedByALostLease(t *testing.T) {
	repo := &leaseLog{id: uuid.New(), pending: 1}
	repo.holdErr = func(int) error { return repository.ErrLeaseLost }
	checked := make(chan struct{}, 1)
	w := testWorker(repo, func(ctx context.Context, _ uuid.UUID) error {
		<-ctx.Done()
		return ctx.Err()
	})
	w.stuck = func(context.Context, uuid.UUID) (bool, error) { checked <- struct{}{}; return false, nil }
	runUntil(t, w, func() bool { _, released := repo.snapshot(); return released == 1 })
	assert.Empty(t, checked)
}

// The worker beats at every iteration and every renewal.
func TestWorker_Beats(t *testing.T) {
	repo := &leaseLog{id: uuid.New()}
	w := testWorker(repo, func(context.Context, uuid.UUID) error { return nil })
	var beats atomic.Int64
	w.SetHeartbeat(func() { beats.Add(1) })
	runUntil(t, w, func() bool { return beats.Load() >= 3 })
}

// An idle worker claims again after Wake, without waiting out its idle time.
func TestWorker_WakeEndsTheIdleWait(t *testing.T) {
	repo := &leaseLog{id: uuid.New()}
	w := testWorker(repo, func(context.Context, uuid.UUID) error { return nil })
	w.idleWait = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() { defer close(finished); w.Run(ctx) }()
	defer func() { cancel(); <-finished }()

	require.Eventually(t, func() bool { repo.mu.Lock(); defer repo.mu.Unlock(); return len(repo.claims) == 1 }, 5*time.Second, time.Millisecond)
	w.Wake()
	w.Wake()
	require.Eventually(t, func() bool { repo.mu.Lock(); defer repo.mu.Unlock(); return len(repo.claims) >= 2 }, 5*time.Second, time.Millisecond)
}
