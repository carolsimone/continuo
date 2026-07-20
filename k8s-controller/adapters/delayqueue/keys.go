// Package delayqueue implements the check.k8s scheduled-set delay queue: a Redis
// ZSET (the clock) + HASH (the payload) that replace the self-recirculating
// check.k8s:v1 stream timer removed in Phase 2 of issue #282. The stream stays
// the reliable executor; this package only answers "run this check at time T".
package delayqueue

const (
	// PendingKey is the ZSET acting as the clock: member = a Job's stable
	// identity (JobName, unique per k8s Job), score = check_after (unix seconds).
	// Because a ZSET member is keyed by value, re-scheduling a job is an in-place
	// score update — one entry per in-flight job, forever. The #282 pile-up is
	// physically impossible here.
	PendingKey = "checkk8s:pending"

	// TicketsKey is the HASH holding each pending check's payload: field =
	// JobName, value = the CheckK8s payload JSON. Keyed by the same JobName as
	// PendingKey, so the two structures stay one-entry-per-job in lockstep.
	TicketsKey = "checkk8s:tickets"
)
