package neo4jinfra

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// deadlock is the error two catalog pods migrating together raise: each holds
// the schema locks the other needs.
func deadlock() error {
	return fmt.Errorf("step: %w", &neo4j.Neo4jError{Code: "Neo.TransientError.Transaction.DeadlockDetected"})
}

func TestRetryMigration_RetriesAnErrorAConcurrentMigrationCauses(t *testing.T) {
	calls := 0
	err := retryMigration(context.Background(), 5, time.Millisecond, quietLogger(), func(context.Context) error {
		calls++
		if calls == 1 {
			return deadlock()
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 2, calls)
}

func TestRetryMigration_FailsAtOnceOnAnyOtherError(t *testing.T) {
	boom := errors.New("boom")
	constraint := &neo4j.Neo4jError{Code: "Neo.ClientError.Schema.ConstraintValidationFailed"}
	for name, failure := range map[string]error{"plain": boom, "client error": constraint} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			err := retryMigration(context.Background(), 5, time.Millisecond, quietLogger(), func(context.Context) error {
				calls++
				return failure
			})
			require.ErrorIs(t, err, failure)
			assert.Equal(t, 1, calls)
		})
	}
}

func TestRetryMigration_GivesUpAfterItsAttempts(t *testing.T) {
	calls := 0
	err := retryMigration(context.Background(), 3, time.Millisecond, quietLogger(), func(context.Context) error {
		calls++
		return deadlock()
	})
	require.Error(t, err)
	assert.Equal(t, 3, calls)
	assert.True(t, concurrentMigrationError(err), "the last failure is returned as it was")
}

func TestRetryMigration_StopsWhenItsContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	err := retryMigration(ctx, 5, time.Hour, quietLogger(), func(context.Context) error {
		cancel()
		return deadlock()
	})
	require.ErrorIs(t, err, context.Canceled)
}
