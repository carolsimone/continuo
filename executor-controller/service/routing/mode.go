// Package routing decides how a production record reaches dbt: as its own
// Kubernetes Job, or as a task claimed from a pool of reusable worker pods.
package routing

import (
	"fmt"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
)

// Policy routes production records by service. defaultMode applies to every
// service the overrides do not name, so enabling workers for one service is a
// single entry in the override map.
type Policy struct {
	defaultMode model.ExecutionMode
	overrides   map[string]model.ExecutionMode
}

// NewPolicy builds a Policy from the configured default mode and the per-service
// overrides. A nil or empty override map means every service takes defaultMode.
func NewPolicy(defaultMode model.ExecutionMode, overrides map[string]model.ExecutionMode) Policy {
	return Policy{defaultMode: defaultMode, overrides: overrides}
}

// Resolve selects the execution mode for one record.
//
// A worker runs exactly one named dbt node against a pinned runtime manifest, so
// a record can only take the worker path when it identifies that node:
//   - dbtUniqueID empty — a message produced before nodes carried their dbt
//     identity. It predates workers entirely and runs as a Kubernetes Job.
//   - dispatchMode promote_seed — seeds promoted into production are built by
//     their own Job, not claimed from a pool.
//
// Everything else takes the service's configured mode. A migrated node routed to
// workers without a complete runtime manifest reference resolves to workers here
// and is rejected by Validate: there is no full-project-parse fallback, so the
// record fails rather than silently running down a path it was moved off.
func (p Policy) Resolve(serviceName, dispatchMode, dbtUniqueID string, ref pkgmodel.RuntimeManifestRef) model.ExecutionMode {
	if dbtUniqueID == "" || dispatchMode == pkgevents.ModePromoteSeed {
		return model.ExecutionModeJobs
	}
	return p.modeFor(serviceName)
}

// Validate reports whether a record Resolve routes to workers carries what a
// worker needs to run it. Records that take the Jobs path are always accepted:
// the runtime manifest requirement belongs to the worker path alone.
func (p Policy) Validate(serviceName, dispatchMode, dbtUniqueID string, ref pkgmodel.RuntimeManifestRef) error {
	if p.Resolve(serviceName, dispatchMode, dbtUniqueID, ref) != model.ExecutionModeWorkers {
		return nil
	}
	if !ref.Complete() {
		return fmt.Errorf("node %q is routed to workers but its runtime manifest reference is incomplete", dbtUniqueID)
	}
	return ref.Validate()
}

// modeFor returns the mode configured for serviceName.
func (p Policy) modeFor(serviceName string) model.ExecutionMode {
	if mode, ok := p.overrides[serviceName]; ok {
		return mode
	}
	return p.defaultMode
}
