// Package delayqueue implements the check.k8s delay queue: a Redis ZSET (the
// clock) + HASH (the payload) that hold a status re-check until its due time.
// Keeping a not-yet-due check here — one entry per in-flight Job — rather than on
// the check.k8s:v1 stream bounds the stream's size. The stream stays the reliable
// executor; this package only answers "run this check at time T".
package delayqueue

import contract "github.com/carolsimone/continuo/pkg/delayqueue"

// PendingKey and TicketsKey are the delay queue's Redis keys, both keyed by
// JobName (a K8s Job's stable identity) so the ZSET clock and the HASH of tickets
// stay one-entry-per-job in lockstep. They come from the shared contract package
// so the end-to-end teardown purges the same keys this adapter writes.
const (
	PendingKey = contract.PendingKey
	TicketsKey = contract.TicketsKey
)
