package neo4jinfra

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func connErr() error {
	return &neo4j.ConnectivityError{Inner: errors.New("dial tcp: lookup continuo-neo4j: no such host")}
}

// recordingSleep records the requested pauses instead of sleeping.
type recordingSleep struct{ delays []time.Duration }

func (r *recordingSleep) sleep(_ context.Context, d time.Duration) error {
	r.delays = append(r.delays, d)
	return nil
}

var testPolicy = StartupPolicy{Timeout: time.Minute, InitialDelay: time.Second, MaxDelay: 4 * time.Second}

func TestWaitForConnectivity_SucceedsOnceNeo4jComesUp(t *testing.T) {
	rec := &recordingSleep{}
	calls := 0
	verify := func(context.Context) error {
		calls++
		if calls <= 4 {
			return connErr()
		}
		return nil
	}

	if err := waitForConnectivity(context.Background(), verify, testPolicy, rec.sleep, quietLogger()); err != nil {
		t.Fatalf("expected success after neo4j came up, got %v", err)
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second}
	if len(rec.delays) != len(want) {
		t.Fatalf("delays = %v, want %v", rec.delays, want)
	}
	for i := range want {
		if rec.delays[i] != want[i] {
			t.Fatalf("delays = %v, want doubling capped at MaxDelay: %v", rec.delays, want)
		}
	}
}

func TestWaitForConnectivity_GivesUpAtTimeoutWithTheLastError(t *testing.T) {
	rec := &recordingSleep{}
	verify := func(context.Context) error { return connErr() }
	policy := StartupPolicy{Timeout: 7 * time.Second, InitialDelay: time.Second, MaxDelay: 4 * time.Second}

	err := waitForConnectivity(context.Background(), verify, policy, rec.sleep, quietLogger())

	if err == nil {
		t.Fatal("expected an error once the timeout elapsed")
	}
	var ce *neo4j.ConnectivityError
	if !errors.As(err, &ce) {
		t.Fatalf("the last connectivity error must stay inspectable, got %v", err)
	}
	var total time.Duration
	for _, d := range rec.delays {
		total += d
	}
	if total > policy.Timeout {
		t.Fatalf("waited %s, more than the %s budget", total, policy.Timeout)
	}
}

func TestWaitForConnectivity_FailsFastOnAValueRetryingCannotFix(t *testing.T) {
	rec := &recordingSleep{}
	calls := 0
	bad := &neo4j.Neo4jError{Code: "Neo.ClientError.Security.Unauthorized", Msg: "invalid credentials"}
	verify := func(context.Context) error { calls++; return bad }

	err := waitForConnectivity(context.Background(), verify, testPolicy, rec.sleep, quietLogger())

	if !errors.Is(err, bad) {
		t.Fatalf("expected the credentials error back, got %v", err)
	}
	if calls != 1 || len(rec.delays) != 0 {
		t.Fatalf("bad credentials must not be retried: calls=%d delays=%v", calls, rec.delays)
	}
}

func TestWaitForConnectivity_StopsWhenTheContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	verify := func(context.Context) error { calls++; return connErr() }
	cancelling := func(context.Context, time.Duration) error { cancel(); return ctx.Err() }

	err := waitForConnectivity(ctx, verify, testPolicy, cancelling, quietLogger())

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected a single attempt before the cancel, got %d", calls)
	}
}

// A real driver dial failure must be classified as retryable; the fakes above
// cannot prove the classifier recognises what the driver actually returns.
func TestNewNeo4jClientWithRetry_RetriesARealDialFailureThenGivesUp(t *testing.T) {
	policy := StartupPolicy{Timeout: 300 * time.Millisecond, InitialDelay: 50 * time.Millisecond, MaxDelay: 100 * time.Millisecond}
	start := time.Now()

	client, err := NewNeo4jClientWithRetry(context.Background(), "bolt://127.0.0.1:1", "neo4j", "x", quietLogger(), policy)

	if err == nil {
		_ = client.Close(context.Background())
		t.Fatal("expected an error for an unreachable neo4j")
	}
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("returned after %s: the dial failure was not retried", elapsed)
	}
}
