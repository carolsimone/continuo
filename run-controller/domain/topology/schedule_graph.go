package topology

import "time"

// ScheduleGraph is the view of one schedule in a tenant's live release: the
// schedule's nodes, the nodes of other schedules they read directly, and the
// edges from the schedule's nodes to what they read. A node read directly can
// be a dormant node or an unknown-external one, which belong to no schedule.
//
// Nodes are in key order. Every edge has its downstream among the schedule's
// nodes and both ends among Nodes; edges are in (upstream, downstream) order.
// PromotionSeq is the promotion seq of the live release, 0 when none is live.
// The zero ScheduleGraph is the view of a tenant with no live release.
type ScheduleGraph struct {
	PromotionSeq int64
	Nodes        []ScheduleGraphNode
	Edges        []Edge
}

// ScheduleGraphNode is one node of a ScheduleGraph. LastRunAt is, for a
// dormant node (Node.IsDormant), the creation time of the run of its newest
// run or full_refresh task in the tenant: a test task of the node is not a run
// of it. It is nil for a dormant node with no such task, one only tested
// included, and for every node that is not dormant.
type ScheduleGraphNode struct {
	Node
	LastRunAt *time.Time
}

// ScheduleSummary is one schedule of the live release: how many nodes it holds
// and when that release was loaded.
type ScheduleSummary struct {
	ScheduleName  string
	NodeCount     int
	LastUpdatedAt time.Time
}

// ScheduleSummaries is a tenant's live release seen as schedules: each
// schedule, in name order, and how many dormant nodes the release holds. The
// zero ScheduleSummaries is the view of a tenant with no live release.
type ScheduleSummaries struct {
	Schedules        []ScheduleSummary
	DormantNodeCount int
}
