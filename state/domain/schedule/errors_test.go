package schedule_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/state/domain/schedule"
)

// A stored cron that no longer parses is ErrStoredCronInvalid, names the row
// and the parser's reason, and carries the version the row is at, through any
// wrapping.
func TestStoredCronInvalidError_IsErrStoredCronInvalidAndCarriesTheVersion(t *testing.T) {
	reason := errors.New("unknown timezone \"Mars/Olympus\"")
	err := fmt.Errorf("get: %w", &schedule.StoredCronInvalidError{
		TenantID: "acme", Name: "daily", Cron: "0 1 * * *", Timezone: "Mars/Olympus", Version: 7, Reason: reason,
	})

	assert.ErrorIs(t, err, schedule.ErrStoredCronInvalid)
	assert.ErrorIs(t, err, reason)
	var invalid *schedule.StoredCronInvalidError
	require.ErrorAs(t, err, &invalid)
	assert.Equal(t, int64(7), invalid.Version)
	for _, part := range []string{"acme", "daily", "0 1 * * *", "Mars/Olympus", reason.Error()} {
		assert.Contains(t, err.Error(), part)
	}
}
