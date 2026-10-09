package handlers

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/uow"
)

// loadProdTopology returns the topology current_prod points at. Nothing
// promoted yet — no release and no artifact — is an empty production topology.
// A release without an artifact is a current_prod written before topology
// artifacts existed; release-controller's startup step writes that artifact
// before any handler runs, so meeting one here is an error.
func loadProdTopology(ctx context.Context, d *Deps, cp *release.CurrentProd) (release.Topology, error) {
	if cp == nil || cp.Topology().IsZero() {
		if cp != nil && cp.ReleaseID() != "" {
			return nil, fmt.Errorf("current_prod %s references no topology artifact", cp.ReleaseID())
		}
		return nil, nil
	}
	topo, err := d.Topologies.Load(ctx, cp.Topology())
	if err != nil {
		return nil, fmt.Errorf("load current_prod topology %s: %w", cp.ReleaseID(), err)
	}
	return topo, nil
}

// currentProdTopology reads current_prod and loads the topology it points at.
func currentProdTopology(ctx context.Context, d *Deps, u uow.UnitOfWork) (release.Topology, error) {
	cp, err := u.CurrentProdRepo().Get(ctx)
	if err != nil {
		return nil, fmt.Errorf("get current prod: %w", err)
	}
	return loadProdTopology(ctx, d, cp)
}
