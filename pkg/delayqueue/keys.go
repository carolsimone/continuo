// Package delayqueue names the Redis keys of the status-check delay queue an
// execution-controller of the previous version writes: a ZSET clock and a
// HASH of pending-check tickets. execution-controller deletes them on every
// resync, since it observes every Job through its watch.
package delayqueue

const (
	// PendingKey is the ZSET acting as the clock: member = a Job's stable
	// identity (JobName), score = the check's due time (unix seconds).
	PendingKey = "checkk8s:pending"

	// TicketsKey is the HASH holding each pending check's ticket: field =
	// JobName, value = the ticket JSON. Keyed by the same JobName as PendingKey.
	TicketsKey = "checkk8s:tickets"
)
