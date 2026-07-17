package model

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ReserveForDispatch takes an execution slot for a due pending Deployment and
// parks it in 'dispatching' while its Kubernetes Job is created. The slot stays
// held through 'deployed' until the Job reports a terminal status, which
// releases it via the repository's ReleaseSlot — the Jobs path has no aggregate
// transition of its own at that point.
func (d *Deployment) ReserveForDispatch(now time.Time) error {
	if d.status != StatusPending {
		return fmt.Errorf("cannot reserve an execution slot from status %q", d.status)
	}
	if now.Before(d.nextAttemptAt) {
		return fmt.Errorf("deployment %s is not due until %s", d.id, d.nextAttemptAt)
	}
	d.status = StatusDispatching
	d.reserve(now)
	return nil
}

// Claim gives a worker an exclusive, expiring hold on a due pending worker-mode
// task and takes its execution slot. tokenSHA256 is the digest of the raw lease
// token; the raw token stays with the claiming worker and is never stored. Each
// claim counts one attempt against the task, so a requeued task's next lease
// carries the higher attempt number.
func (d *Deployment) Claim(
	leaseID uuid.UUID,
	tokenSHA256, owner, podName, podUID string,
	now, expiresAt time.Time,
	argv []string,
	path ExecutionPath,
) error {
	if d.executionMode != ExecutionModeWorkers {
		return fmt.Errorf("cannot claim deployment %s: execution mode is %q", d.id, d.executionMode)
	}
	if d.status != StatusPending {
		return fmt.Errorf("cannot claim deployment %s from status %q", d.id, d.status)
	}
	if now.Before(d.nextAttemptAt) {
		return fmt.Errorf("deployment %s is not due until %s", d.id, d.nextAttemptAt)
	}
	if !path.Valid() || path == "" {
		return fmt.Errorf("invalid execution path %q", path)
	}
	d.attempt++
	d.status = StatusLeased
	d.resolvedArgv = argv
	d.executionPath = path
	d.lease = &Lease{
		ID:          leaseID,
		TokenSHA256: tokenSHA256,
		Owner:       owner,
		PodName:     podName,
		PodUID:      podUID,
		Attempt:     d.attempt,
		ExpiresAt:   expiresAt,
		HeartbeatAt: now,
	}
	d.reserve(now)
	return nil
}

// AcknowledgeStart records that the lease holder's dbt process has started.
// It reports changed=false for a duplicate start so a retrying worker neither
// errors nor moves started_at.
func (d *Deployment) AcknowledgeStart(leaseID uuid.UUID, tokenSHA256 string, now time.Time) (bool, error) {
	if !d.lease.Authorizes(leaseID, tokenSHA256) {
		return false, ErrStaleLease
	}
	if d.lease.StartedAt != nil {
		return false, nil
	}
	if d.status != StatusLeased {
		return false, fmt.Errorf("cannot start deployment %s from status %q", d.id, d.status)
	}
	ts := now
	d.lease.StartedAt = &ts
	d.lease.HeartbeatAt = now
	d.status = StatusRunning
	return true, nil
}

// Heartbeat extends the current lease's deadline for the worker holding it. A
// caller whose lease no longer holds the task is fenced with ErrStaleLease, so a
// superseded worker cannot keep a reassigned task's lease alive. An expiry that
// would pull the deadline in is rejected so a delayed heartbeat cannot shorten a
// live lease.
func (d *Deployment) Heartbeat(leaseID uuid.UUID, tokenSHA256 string, now, expiresAt time.Time) error {
	if !d.lease.Authorizes(leaseID, tokenSHA256) {
		return ErrStaleLease
	}
	if d.status != StatusLeased && d.status != StatusRunning {
		return fmt.Errorf("cannot heartbeat deployment %s in status %q", d.id, d.status)
	}
	if expiresAt.Before(d.lease.ExpiresAt) {
		return fmt.Errorf("heartbeat for deployment %s would shorten its lease", d.id)
	}
	d.lease.ExpiresAt = expiresAt
	d.lease.HeartbeatAt = now
	return nil
}

// Complete records the lease holder's successful terminal report and releases
// the task's execution slot. It is idempotent for a repeat of the same result
// from the same lease; a conflicting second result is rejected. A failed result
// travels through MarkRetryPending or MarkFailed, which the lease service picks
// after classifying the failure.
func (d *Deployment) Complete(leaseID uuid.UUID, tokenSHA256 string, result WorkerResult, now time.Time) error {
	if !d.lease.Authorizes(leaseID, tokenSHA256) {
		return ErrStaleLease
	}
	if d.lease.FinishedAt != nil {
		if d.terminalResult != nil && *d.terminalResult == result {
			return nil
		}
		return fmt.Errorf("deployment %s already reported a different terminal result", d.id)
	}
	if !result.Succeeded {
		return fmt.Errorf("Complete requires a succeeded result for deployment %s", d.id)
	}
	if d.status != StatusLeased && d.status != StatusRunning {
		return fmt.Errorf("cannot complete deployment %s from status %q", d.id, d.status)
	}
	ts := now
	d.lease.FinishedAt = &ts
	res := result
	d.terminalResult = &res
	d.status = StatusSucceeded
	d.releaseSlot(now)
	return nil
}

// ReportFailure applies the lease holder's failed terminal report. Both
// release the task's execution slot. A caller whose lease no longer holds the
// task is fenced with ErrStaleLease, so a superseded worker cannot drop the
// current holder's lease or free the slot that holder occupies.
//
// A permanent failure finishes the lease that reported it, so a redelivery of
// the same report is absorbed and a conflicting second result is rejected —
// the same idempotency Complete gives a successful report. A retryable failure
// drops the lease instead, so its redelivery is fenced with ErrStaleLease: once
// the lease is gone the report is indistinguishable from one sent by a worker
// whose task has been reassigned.
//
// result.Retryable and the retryable parameter are distinct and may disagree.
// result.Retryable is the worker's own hint, stored verbatim as part of the
// terminal result for audit. The retryable parameter is the caller's decision
// and alone selects the transition: true parks the task for requeue after
// backoff, false fails it permanently and records the report. The caller
// narrows the worker's hint against an error-class denylist and the task's
// retry budget, so an exhausted budget fails a task permanently while the
// stored report still shows the worker considered the failure retryable.
func (d *Deployment) ReportFailure(
	leaseID uuid.UUID,
	tokenSHA256 string,
	result WorkerResult,
	retryable bool,
	now time.Time,
	backoff time.Duration,
) error {
	if !d.lease.Authorizes(leaseID, tokenSHA256) {
		return ErrStaleLease
	}
	if result.Succeeded {
		return fmt.Errorf("ReportFailure requires a failed result for deployment %s", d.id)
	}
	if d.lease.FinishedAt != nil {
		if d.terminalResult != nil && *d.terminalResult == result {
			return nil
		}
		return fmt.Errorf("deployment %s already reported a different terminal result", d.id)
	}
	if retryable {
		return d.MarkRetryPending(now, backoff)
	}
	if err := d.MarkFailed(now, result.ErrorMessage); err != nil {
		return err
	}
	ts := now
	d.lease.FinishedAt = &ts
	res := result
	d.terminalResult = &res
	return nil
}

// ExpireLease drops the lease whose deadline has passed, fencing the worker that
// held it: with no lease on the task, a report that worker sends afterwards
// authorizes nobody and is answered ErrStaleLease instead of driving a
// transition. leaseID names the lease being expired, so a caller acting on a
// lease the task no longer holds is rejected rather than dropping the lease a
// live worker is running under.
//
// It settles nothing else. The caller applies the transition that decides the
// task's fate and releases its execution slot, so slot release stays where every
// other worker path keeps it.
func (d *Deployment) ExpireLease(leaseID uuid.UUID) error {
	if d.lease == nil || d.lease.ID != leaseID {
		return ErrStaleLease
	}
	d.lease = nil
	return nil
}

// MarkRetryPending parks a worker task that failed retryably, or whose lease
// expired, for requeue after backoff and releases its execution slot in the same
// transition. The lease is dropped so the next attempt claims a fresh one. It is
// unfenced: the lease reaper and cancellation drive it holding no worker token.
func (d *Deployment) MarkRetryPending(now time.Time, backoff time.Duration) error {
	if d.status.terminal() {
		return fmt.Errorf("cannot retry deployment %s from terminal status %q", d.id, d.status)
	}
	d.status = StatusRetryPending
	d.nextAttemptAt = now.Add(backoff)
	d.lease = nil
	d.releaseSlot(now)
	return nil
}

// MarkFailed drives a worker task to a permanent failure and releases its
// execution slot in the same transition. It is unfenced: the lease reaper and
// cancellation drive it holding no worker token.
func (d *Deployment) MarkFailed(now time.Time, reason string) error {
	if d.status.terminal() {
		return fmt.Errorf("cannot fail deployment %s from terminal status %q", d.id, d.status)
	}
	msg := reason
	d.errorMessage = &msg
	d.status = StatusFailed
	d.releaseSlot(now)
	return nil
}

// Cancel drives a Deployment to a terminal cancelled state and releases any
// execution slot it holds in the same transition.
func (d *Deployment) Cancel(reason string, now time.Time) error {
	if d.status.terminal() {
		return fmt.Errorf("cannot cancel deployment %s from terminal status %q", d.id, d.status)
	}
	msg := reason
	d.errorMessage = &msg
	d.status = StatusCancelled
	d.releaseSlot(now)
	return nil
}

// reserve takes an execution slot for this Deployment.
func (d *Deployment) reserve(now time.Time) {
	ts := now
	d.reservation.ReservedAt = &ts
	d.reservation.ReleasedAt = nil
}

// releaseSlot frees a held execution slot. A Deployment that never reserved one
// stays unreserved, keeping released-implies-reserved true.
func (d *Deployment) releaseSlot(now time.Time) {
	if !d.reservation.held() {
		return
	}
	ts := now
	d.reservation.ReleasedAt = &ts
}

// Accessors for the worker-execution state, used by adapters and the
// application services.
func (d *Deployment) ExecutionMode() ExecutionMode  { return d.executionMode }
func (d *Deployment) PoolKey() string               { return d.poolKey }
func (d *Deployment) ResolvedArgv() []string        { return d.resolvedArgv }
func (d *Deployment) ExecutionPath() ExecutionPath  { return d.executionPath }
func (d *Deployment) Reservation() Reservation      { return d.reservation }

// Attempt reports how many times a worker has claimed this task. It survives the
// lease being dropped between attempts, so a requeued task keeps counting up.
func (d *Deployment) Attempt() int { return d.attempt }
func (d *Deployment) TerminalResult() *WorkerResult { return d.terminalResult }

// ActiveLease returns the lease currently held on this Deployment, or nil when
// no worker holds it.
func (d *Deployment) ActiveLease() *Lease { return d.lease }

// SlotReleasedAt reports when this Deployment released its execution slot, or
// nil while it still holds one (or never reserved one).
func (d *Deployment) SlotReleasedAt() *time.Time { return d.reservation.ReleasedAt }
