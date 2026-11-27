package handlers

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/uow"
)

// loadProdTopology returns the live release's topology. Nothing
// promoted yet — no release and no artifact — is an empty production topology.
// A release without an artifact has no topology to read, so meeting one is an
// error rather than an empty production topology.
func loadProdTopology(ctx context.Context, d *Deps, cp *release.LiveRelease) (release.Topology, error) {
	if cp == nil || cp.Topology().IsZero() {
		if cp != nil && cp.ReleaseID() != "" {
			return nil, fmt.Errorf("live release %s references no topology artifact", cp.ReleaseID())
		}
		return nil, nil
	}
	topo, err := d.Topologies.Load(ctx, cp.Topology())
	if err != nil {
		return nil, fmt.Errorf("load the live release's topology %s: %w", cp.ReleaseID(), err)
	}
	return topo, nil
}

// liveReleaseTopology reads the live release and loads its topology.
func liveReleaseTopology(ctx context.Context, d *Deps, u uow.UnitOfWork) (release.Topology, error) {
	cp, err := u.LiveReleaseRepo().Get(ctx)
	if err != nil {
		return nil, fmt.Errorf("get live release: %w", err)
	}
	return loadProdTopology(ctx, d, cp)
}
