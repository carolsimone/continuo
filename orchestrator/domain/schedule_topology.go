package domain

import "time"

// ScheduleTopologySummary is the per-schedule aggregate emitted by
// ListScheduleTopologies. It is computed against the current :Table state in
// Neo4j and carries the information needed to render a "DAG Latest Snapshot"
// homepage tile: how many active nodes the schedule owns, and when any of
// them was last touched.
type ScheduleTopologySummary struct {
	ScheduleName  string
	NodeCount     int
	LastUpdatedAt time.Time
}
