package outbox_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgresWaker_SignalsOnlyAfterCommit(t *testing.T) {
	db := dbForTest(t)
	w, err := outbox.NewPostgresWaker(context.Background(), testPostgresConfig().DSN(), testOutboxTable, newTestLogger())
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	tx := db.MustBegin()
	defer func() { _ = tx.Rollback() }()
	repo := outbox.NewPostgresRepository(tx, testOutboxTable, newTestLogger())
	require.NoError(t, repo.Create(context.Background(), &outbox.Entry{
		AggregateType: "pkg-outbox-test", AggregateID: uuid.New(), EventType: "e", Payload: []byte(`{}`), StreamName: "s"}))
	select {
	case <-w.Wake():
		t.Fatal("woke before the inserting transaction committed")
	case <-time.After(300 * time.Millisecond):
	}
	require.NoError(t, tx.Commit())
	select {
	case <-w.Wake():
	case <-time.After(5 * time.Second):
		t.Fatal("no wake after commit")
	}
}

// When the listener's connection drops, the waker re-establishes it, signals
// once for the notifications it may have missed, and keeps waking on rows
// committed afterwards.
func TestPostgresWaker_WakesAfterAReconnectAndKeepsListening(t *testing.T) {
	db := dbForTest(t)
	const appName = "pkg-outbox-waker-test"
	w, err := outbox.NewPostgresWaker(context.Background(), testPostgresConfig().DSN()+" application_name="+appName, testOutboxTable, newTestLogger())
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	var terminated int
	require.NoError(t, db.Get(&terminated, `
		SELECT count(*) FROM (
			SELECT pg_terminate_backend(pid) AS ok FROM pg_stat_activity WHERE application_name = $1
		) t WHERE ok`, appName))
	require.Equal(t, 1, terminated, "the listener's backend is terminated")

	select {
	case <-w.Wake():
	case <-time.After(10 * time.Second):
		t.Fatal("no wake after the listener reconnected")
	}

	createRows(t, db, uuid.New(), 1)
	select {
	case <-w.Wake():
	case <-time.After(5 * time.Second):
		t.Fatal("no wake for a row committed after the reconnect")
	}
}

// With an hour-long tick, a row committed while the relay is idle publishes
// within seconds only because its NOTIFY wakes the relay.
func TestProcessor_PublishesOnNotifyLongBeforeTheTick(t *testing.T) {
	db := dbForTest(t)
	w, err := outbox.NewPostgresWaker(context.Background(), testPostgresConfig().DSN(), testOutboxTable, newTestLogger())
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	pub := &orderPublisher{}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(), outbox.ProcessorConfig{
		Tick: time.Hour, Waker: w})
	drained := make(chan struct{}, 16)
	p.SetAfterDrainHookForTest(func() {
		select {
		case drained <- struct{}{}:
		default:
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() { defer close(stopped); _ = p.Run(ctx) }()
	defer func() { cancel(); <-stopped }()

	// Run drains once when it starts. After that drain ends, the next drain
	// comes from a wake or from the hour-long tick, so a row committed from
	// here on reaches the publisher only through a drain that a wake starts.
	awaitDrainEnd(t, drained, "the initial drain ends")

	insertedAt := time.Now()
	ids := createRows(t, db, uuid.New(), 1)
	require.Eventually(t, func() bool {
		pub.mu.Lock()
		defer pub.mu.Unlock()
		return len(pub.ids) == 1 && pub.ids[0] == ids[0]
	}, 3*time.Second, 5*time.Millisecond, "the committed row publishes on its notification")
	t.Logf("published %s after the insert, with a %s tick", time.Since(insertedAt).Round(time.Millisecond), time.Hour)
	awaitDrainEnd(t, drained, "the drain the wake started ends")
}

// awaitDrainEnd waits for the processor's after-drain hook to signal on
// drained.
func awaitDrainEnd(t *testing.T, drained <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal(what + ": no drain ended within 5s")
	}
}

// A shutdown during boot does not wait for Postgres: when ctx ends before the
// LISTEN is established, NewPostgresWaker returns ctx's error.
func TestNewPostgresWaker_ReturnsWhenItsContextEndsBeforeTheListen(t *testing.T) {
	// Nothing listens on port 1, so every connection attempt is refused and
	// the listener keeps retrying.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	w, err := outbox.NewPostgresWaker(ctx, "host=127.0.0.1 port=1 user=u dbname=d sslmode=disable", "t", newTestLogger())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Nil(t, w)
	assert.Less(t, time.Since(start), 2*time.Second)
}
