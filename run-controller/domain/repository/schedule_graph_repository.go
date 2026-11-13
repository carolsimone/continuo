package repository

import (
	"context"

	"github.com/carolsimone/continuo/run-controller/domain/topology"
)

// ScheduleGraphRepository reads a tenant's live release as schedules. Every
// read names the tenant and sees only that tenant's live release.
type ScheduleGraphRepository interface {
	// Graph returns the live release's nodes of scheduleName, the nodes they
	// read directly (of other schedules, dormant or unknown-external), the edges
	// from the schedule's nodes to what they read, and the live promotion seq.
	// Each dormant node carries the creation time of the run of its newest run
	// or full_refresh task in the tenant, none when it has no such task (a test
	// task is not a run of it); no other node carries one. A
	// schedule the live release holds no node of yields no nodes and no edges;
	// a tenant with no live release yields the zero graph.
	Graph(ctx context.Context, tenantID, scheduleName string) (topology.ScheduleGraph, error)

	// Summaries returns each schedule of the live release with its node count
	// and the time the release was loaded, ordered by schedule name, and the
	// number of dormant nodes the release holds. A node that belongs to no
	// schedule is not counted in any schedule; a tenant with no live release
	// yields the zero ScheduleSummaries.
	Summaries(ctx context.Context, tenantID string) (topology.ScheduleSummaries, error)
}
