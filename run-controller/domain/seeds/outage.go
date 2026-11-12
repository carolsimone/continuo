package seeds

import (
	"time"

	"github.com/carolsimone/continuo/pkg/warehouse"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/google/uuid"
)

// OutageState is how a view stands after a seed load dropped the table it
// reads, until a task of the view succeeds.
type OutageState string

const (
	// OutageMissing: the load dropped the view with the table, so it does not
	// exist until its rebuild succeeds.
	OutageMissing OutageState = "missing"
	// OutageStale: the view still exists but was defined over the table the
	// load dropped, so it may answer wrongly until its rebuild succeeds.
	OutageStale OutageState = "stale"
)

// OutageStates are the outage states, in the order they are reported.
func OutageStates() []OutageState { return []OutageState{OutageMissing, OutageStale} }

// OutageStateFor is the state a seed load that drops its table leaves the
// views the run rebuilds after it in: a schema_bound engine drops them with the
// table, so they are missing; a late_binding engine leaves them in place,
// stale.
func OutageStateFor(f warehouse.Family) OutageState {
	if f == warehouse.LateBinding {
		return OutageStale
	}
	return OutageMissing
}

// ViewOutage is a view a seed load left missing or stale: since when, the seed
// whose load did it and the run of that load.
type ViewOutage struct {
	Key     topology.NodeKey
	State   OutageState
	Since   time.Time
	SeedKey topology.NodeKey
	RunID   uuid.UUID
}
