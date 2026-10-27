package topology

import "time"

// ScheduleGraph is the view of one schedule in a tenant's live release: the
// schedule's nodes, the nodes of other schedules they read directly, and the
// edges from the schedule's nodes to what they read.
//
// Nodes are in key order. Every edge has its downstream among the schedule's
// nodes and both ends among Nodes; edges are in (upstream, downstream) order.
// PromotionSeq is the promotion seq of the live release, 0 when none is live.
// The zero ScheduleGraph is the view of a tenant with no live release.
type ScheduleGraph struct {
	PromotionSeq int64
	Nodes        []Node
	Edges        []Edge
}

// ScheduleSummary is one schedule of the live release: how many nodes it holds
// and when that release was loaded.
type ScheduleSummary struct {
	ScheduleName  string
	NodeCount     int
	LastUpdatedAt time.Time
}
