package handlers

import (
	"context"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/release-controller/service/ports"
)

// ErrReleaseSchemaNotReady reports that the release schema is not built: a
// table of the release checks, the live release or the production pointers is
// absent, so the database has not been migrated.
var ErrReleaseSchemaNotReady = errors.New("release schema not initialized")

// CheckReleaseSchemaInitialized refuses a release database whose base tables
// are absent. release-controller runs it at startup, so it never serves from a
// database the migrations have not built.
func CheckReleaseSchemaInitialized(ctx context.Context, schema ports.ReleaseSchema) error {
	initialized, err := schema.Initialized(ctx)
	if err != nil {
		return fmt.Errorf("read the release database schema: %w", err)
	}
	if !initialized {
		return fmt.Errorf("%w: a table that holds the release checks (release_checks), the live release (live_release) "+
			"or the production pointers (production_pointers) is absent; release-controller starts once the release "+
			"database migration (the chart's db-init-migrate Job) has built the schema",
			ErrReleaseSchemaNotReady)
	}
	return nil
}
