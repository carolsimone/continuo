package handlers_test

import (
	"context"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeReleaseSchema reports whether the release schema is built, or fails
// with err.
type fakeReleaseSchema struct {
	built bool
	err   error
}

func (f fakeReleaseSchema) Initialized(context.Context) (bool, error) { return f.built, f.err }

var _ ports.ReleaseSchema = fakeReleaseSchema{}

// A database the migrations have built passes.
func TestCheckReleaseSchemaInitialized_PassesOnABuiltSchema(t *testing.T) {
	require.NoError(t, handlers.CheckReleaseSchemaInitialized(context.Background(), fakeReleaseSchema{built: true}))
}

// A reachable database with no release schema is refused, naming the Job that
// builds it, so release-controller never serves from a database it cannot use.
func TestCheckReleaseSchemaInitialized_RefusesADatabaseTheMigrationsHaveNotBuilt(t *testing.T) {
	err := handlers.CheckReleaseSchemaInitialized(context.Background(), fakeReleaseSchema{built: false})
	require.Error(t, err)
	assert.ErrorIs(t, err, handlers.ErrReleaseSchemaNotReady)
	assert.Contains(t, err.Error(), "db-init-migrate")
	for _, table := range []string{"release_checks", "live_release", "production_pointers"} {
		assert.Contains(t, err.Error(), table, "the refusal names every table it needs")
	}
}

// A schema read that fails is surfaced as a failed read, not as the refusal.
func TestCheckReleaseSchemaInitialized_ReportsAFailedRead(t *testing.T) {
	boom := errors.New("connection refused")
	err := handlers.CheckReleaseSchemaInitialized(context.Background(), fakeReleaseSchema{err: boom})
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
	assert.NotErrorIs(t, err, handlers.ErrReleaseSchemaNotReady)
}
