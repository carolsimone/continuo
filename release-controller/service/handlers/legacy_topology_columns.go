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

// CheckLegacyTopologyColumnsDropped refuses a release database that still has
// the columns that held topologies inline. This release-controller never reads
// them, so on such a database it would treat a run or current_prod whose
// topology is only inline as having none.
func CheckLegacyTopologyColumnsDropped(ctx context.Context, cols ports.LegacyTopologyColumns) error {
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
