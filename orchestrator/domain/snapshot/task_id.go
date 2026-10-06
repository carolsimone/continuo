package snapshot

import "github.com/google/uuid"

// taskIDNamespace seeds the task ids TaskIDFor derives. Never change it: every
// task id already written to an :EXECUTES edge, a task_tracker row and a Job
// label was derived from it, and a redelivered snapshot of an existing run must
// derive those same ids again.
var taskIDNamespace = uuid.MustParse("1e94c4ff-326b-43dc-8d7f-036d0d0bf7c7")

// TaskIDFor derives the task id of table f in run runID as a UUIDv5 over the
// run id and the :Table identity. Selecting the same run's projection twice —
// a trigger redelivered after the snapshot's Neo4j transaction committed but
// before the Postgres unit of work did — therefore yields the task ids already
// stamped on the run's :EXECUTES edges, which the snapshot writer never
// overwrites. The NUL separators keep the five components unambiguous.
func TaskIDFor(runID string, f FQN) uuid.UUID {
	return uuid.NewSHA1(taskIDNamespace, []byte("task:"+runID+"\x00"+f.Service+"\x00"+f.Schema+"\x00"+f.Table+"\x00"+f.ScheduleName))
}

// rowTaskID is TaskIDFor keyed by the schedule name a projection row carries,
// which is the schedule_name the snapshot writer matches the row's :Table on.
func rowTaskID(runID string, f FQN, scheduleName string) uuid.UUID {
	f.ScheduleName = scheduleName
	return TaskIDFor(runID, f)
}
