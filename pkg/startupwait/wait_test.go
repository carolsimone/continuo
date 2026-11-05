package startupwait

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

var testPolicy = Policy{Timeout: time.Minute, InitialDelay: time.Second, MaxDelay: 4 * time.Second}

// recordingSleep records the requested pauses instead of sleeping.
type recordingSleep struct{ delays []time.Duration }

func (r *recordingSleep) sleep(_ context.Context, d time.Duration) error {
	r.delays = append(r.delays, d)
	return nil
}

func TestWait_RetriesWithCappedBackoffUntilThePingSucceeds(t *testing.T) {
	rec := &recordingSleep{}
	attempts := 0
	dep := Dependency{
		Name: "Postgres",
		Ping: func(context.Context) error {
			attempts++
			if attempts < 5 {
				return errors.New("connection refused")
			}
			return nil
		},
		Retryable: func(error) bool { return true },
		Sleep:     rec.sleep,
	}

	require.NoError(t, Wait(context.Background(), dep, testPolicy, quietLogger()))

	assert.Equal(t, 5, attempts)
	assert.Equal(t, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second}, rec.delays)
}

func TestWait_ReturnsWithoutPausingWhenTheFirstPingSucceeds(t *testing.T) {
	rec := &recordingSleep{}
	dep := Dependency{
		Name:      "Postgres",
		Ping:      func(context.Context) error { return nil },
		Retryable: func(error) bool { return true },
		Sleep:     rec.sleep,
	}

	require.NoError(t, Wait(context.Background(), dep, testPolicy, quietLogger()))

	assert.Empty(t, rec.delays)
}

func TestWait_FailsAtOnceAndNamesTheDependencyOnAnErrorThatIsNotRetryable(t *testing.T) {
	rec := &recordingSleep{}
	permanent := errors.New("password authentication failed")
	dep := Dependency{
		Name:      "Postgres",
		Ping:      func(context.Context) error { return permanent },
		Retryable: func(error) bool { return false },
		Sleep:     rec.sleep,
	}

	err := Wait(context.Background(), dep, testPolicy, quietLogger())

	require.ErrorIs(t, err, permanent)
	assert.Contains(t, err.Error(), "postgres rejected the connection")
	assert.Empty(t, rec.delays)
}

func TestWait_GivesUpWhenTheBudgetIsSpentAndNamesTheDependency(t *testing.T) {
	rec := &recordingSleep{}
	last := errors.New("connection refused")
	dep := Dependency{
		Name:      "Postgres",
		Ping:      func(context.Context) error { return last },
		Retryable: func(error) bool { return true },
		Sleep:     rec.sleep,
	}
	policy := Policy{Timeout: 7 * time.Second, InitialDelay: time.Second, MaxDelay: 4 * time.Second}

	err := Wait(context.Background(), dep, policy, quietLogger())

	require.ErrorIs(t, err, last)
	assert.Contains(t, err.Error(), "postgres still unreachable")
	assert.NotErrorIs(t, err, context.Canceled)
	assert.Equal(t, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}, rec.delays)
}

func TestWait_ReportsACancelledContextAsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dep := Dependency{
		Name: "Postgres",
		Ping: func(context.Context) error {
			cancel()
			return errors.New("connection refused")
		},
		Retryable: func(error) bool { return true },
	}

	err := Wait(ctx, dep, testPolicy, quietLogger())

	require.ErrorIs(t, err, context.Canceled)
	assert.Contains(t, err.Error(), "waiting for postgres interrupted")
}
