package repository

import (
	"context"

	"github.com/carolsimone/continuo/run-controller/domain/topology"
)

// ScheduleGraphRepository reads a tenant's live release as schedules. Every
// read names the tenant and sees only that tenant's live release.
type ScheduleGraphRepository interface {
	// Graph returns the live release's nodes of scheduleName, the nodes of other
	// schedules they read directly, the edges from the schedule's nodes to what
	// they read, and the live promotion seq. A schedule the live release holds
	// no node of yields no nodes and no edges; a tenant with no live release
	// yields the zero graph.
	Graph(ctx context.Context, tenantID, scheduleName string) (topology.ScheduleGraph, error)

	// Summaries returns each schedule of the live release with its node count
	// and the time the release was loaded, ordered by schedule name. A node that
	// belongs to no schedule is not counted; a tenant with no live release
	// yields none.
	Summaries(ctx context.Context, tenantID string) ([]topology.ScheduleSummary, error)
}
