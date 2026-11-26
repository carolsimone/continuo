package handlers

import (
	"context"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/release-controller/service/ports"
)

// ErrReleaseSchemaNotReady reports that the release schema is not built: its
// base tables are absent, so the database has not been migrated.
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
		return fmt.Errorf("%w: release_checks, current_prod or service_prod is absent; release-controller "+
			"starts once the release database migration (the chart's db-init-migrate Job) has built the schema",
			ErrReleaseSchemaNotReady)
	}
	return nil
}
