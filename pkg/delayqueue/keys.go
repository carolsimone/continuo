// Package delayqueue holds the Redis key contract for k8s-controller's check.k8s
// delay queue: a ZSET clock and a HASH of pending-check tickets. The keys live in
// a shared package — not a service's internals — so both k8s-controller and the
// end-to-end teardown that must purge them reference one definition.
package delayqueue

const (
	// PendingKey is the ZSET acting as the clock: member = a Job's stable
	// identity (JobName), score = the check's due time (unix seconds).
	PendingKey = "checkk8s:pending"

	// TicketsKey is the HASH holding each pending check's ticket: field =
	// JobName, value = the ticket JSON. Keyed by the same JobName as PendingKey.
	TicketsKey = "checkk8s:tickets"
)
