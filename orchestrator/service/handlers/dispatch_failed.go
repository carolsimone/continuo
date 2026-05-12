package handlers

import (
	"errors"

	pkgEvents "github.com/carolsimone/continuo/pkg/events"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
)

// dispatchFailedReason maps an error returned by SnapshotService.Snapshot
// to a DispatchFailedReason for the run.entries.dispatch_failed:v1 stream.
//
// Returns (reason, true) only for the two sentinels that express an
// expected "no work to dispatch" outcome:
//   - snapshot.ErrTargetNotFound  -> DispatchFailedReasonTargetNotFound
//   - snapshot.ErrEmptyProjection -> DispatchFailedReasonEmptyProjection
//
// Returns ("", false) for every other error. When false is returned the
// caller MUST propagate err unchanged (typically wrapped with
// fmt.Errorf) so the Redis consumer's existing classification still
// applies: a transient error gets NACKed and reclaimed via XCLAIM; an
// error wrapping events.ErrPermanent gets ACKed and dropped (see
// docs/arch/05-error-classification.md). Synthesising a dispatch_failed
// event for a transient error would terminally fail a recoverable run;
// doing so for a permanent error would be redundant and would mask the
// upstream operator signal.
func dispatchFailedReason(err error) (pkgEvents.DispatchFailedReason, bool) {
	switch {
	case errors.Is(err, snapshot.ErrTargetNotFound):
		return pkgEvents.DispatchFailedReasonTargetNotFound, true
	case errors.Is(err, snapshot.ErrEmptyProjection):
		return pkgEvents.DispatchFailedReasonEmptyProjection, true
	default:
		return "", false
	}
}
