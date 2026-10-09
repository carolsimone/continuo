package handlers

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// candidateTopology loads r's candidate topology from its artifact. A run
// without a reference — not parsed yet, or rejected before its topology was
// recorded — has none. A load error is returned wrapped (errors.Is still
// matches): the caller's transaction rolls back, and the binding dead-letters a
// corrupt artifact while an unreachable store pauses the consumer.
func candidateTopology(ctx context.Context, d *Deps, r *pipeline.Run) (release.Topology, error) {
	ref := r.CandidateTopologyRef()
	if ref.IsZero() {
		return nil, nil
	}
	topo, err := d.Topologies.Load(ctx, ref)
	if err != nil {
		return nil, fmt.Errorf("load candidate topology of %s: %w", r.ID(), err)
	}
	return topo, nil
}
