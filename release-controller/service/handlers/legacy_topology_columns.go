package handlers

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/carolsimone/continuo/release-controller/service/ports"
)

// upgradeThroughRelease is the continuo release whose release-controller moves
// every topology stored inline into object storage at startup. An install runs
// it at least once before it reaches a release whose database drops those
// columns.
const upgradeThroughRelease = "0.10.0"

// ErrLegacyTopologyColumnsPresent reports that the release database still has a
// column release migration V26 drops: the migration has not applied yet, or it
// refused because rows still need the columns.
var ErrLegacyTopologyColumnsPresent = errors.New("release migration V26 not applied")

// ErrReleaseSchemaNotReady reports that the release schema is not initialized:
// its base tables are absent, so the database has not been migrated yet.
var ErrReleaseSchemaNotReady = errors.New("release schema not initialized")

// CheckLegacyTopologyColumnsDropped refuses a release database that still has
// the columns that held topologies inline. This release-controller never reads
// them, so on such a database it would treat a run or current_prod whose
// topology is only inline as having none.
func CheckLegacyTopologyColumnsDropped(ctx context.Context, cols ports.LegacyTopologyColumns) error {
	initialized, err := cols.SchemaInitialized(ctx)
	if err != nil {
		return fmt.Errorf("read the release database schema: %w", err)
	}
	if !initialized {
		return fmt.Errorf("%w: release_pipeline_runs or current_prod is absent; release-controller starts once the "+
			"release database migration (the chart's db-init-migrate Job) has created and migrated the schema. If an "+
			"upgrade left it this way, upgrade to continuo %s and let release-controller start once before upgrading to this release",
			ErrReleaseSchemaNotReady, upgradeThroughRelease)
	}

	present, err := cols.Present(ctx)
	if err != nil {
		return fmt.Errorf("read the release database schema: %w", err)
	}
	if len(present) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %s still exist; release-controller starts once the release database migration "+
		"(the chart's db-init-migrate Job) has applied it. If the Job failed because rows still need the columns, "+
		"upgrade to continuo %s and let release-controller start once before upgrading to this release",
		ErrLegacyTopologyColumnsPresent, strings.Join(present, ", "), upgradeThroughRelease)
}
