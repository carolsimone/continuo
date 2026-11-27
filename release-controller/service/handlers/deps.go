package handlers

import (
	"log/slog"

	"github.com/carolsimone/continuo/pkg/warehouse"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/carolsimone/continuo/release-controller/service/uow"
)

// Deps bundles the collaborators every handler shares.
// Constructed once in main.go; handlers are stateless functions over it.
// NewUoW is a factory called per-handler-invocation so that each request
// owns an independent UnitOfWork with isolated tx state. The underlying
// *sqlx.DB pool is goroutine-safe; UoW instances are not.
type Deps struct {
	NewUoW    func() uow.UnitOfWork
	Clock     ports.Clock
	Telemetry ports.Telemetry
	Logger    *slog.Logger
	Bucket    string
	Fixes     ports.FixReader // lists a release's remediation attempts for the retry decision

	// Topologies reads and writes topology artifacts: every handler that needs
	// a check's candidate topology loads it through here by the check's reference.
	Topologies ports.TopologyArtifactStore

	// Rejections renders the release.rejected:v1 body of whichever leg ended
	// the candidate, so the handlers pass values and never wire keys.
	Rejections ports.ReleaseRejectedEncoder

	// Maintenance is MAINTENANCE_ENABLED. While true, release intake and
	// remediation retries are refused and the queue activates only the
	// release checks of fixes, so candidates wait and fixes already being
	// verified finish.
	Maintenance bool

	// OntologyEnabled is ONTOLOGY_ENABLED, the install's ontology switch.
	// While false, a release of kind ontology is refused.
	OntologyEnabled bool

	// Family is the warehouse family of WAREHOUSE_ENGINE and Engine its name:
	// each release's seed reload notices predict what a seed's load drops by
	// the family, and name the engine in their reasons.
	Family warehouse.Family
	Engine string
}
