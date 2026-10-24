package model

import (
	"time"

	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
)

// Verdict is how an attempt ended: succeeded, stopped on request, or failed
// for Reason.
type Verdict struct {
	Succeeded bool
	// Stopped is true when the attempt was asked to stop and neither succeeded
	// nor failed on its own first; it carries no Reason.
	Stopped bool
	Reason  pkgmodel.FailureReason
}

// Kubernetes reasons Classify reads.
const (
	reasonOOMKilled        = "OOMKilled"
	reasonEvicted          = "Evicted"
	reasonDeadlineExceeded = "DeadlineExceeded"
)

// Classify decides how an attempt that has ended went, from everything
// observed of it. A reason execution-controller already decided (it ended the
// attempt itself, or found its Job missing) stands; otherwise the Job's
// deadline, a memory kill, an eviction and the exit code are read in that
// order. An attempt whose pods are gone without the main container having
// terminated, and without the Job reporting success, was lost with its node.
// An attempt that was asked to stop and did not succeed is stopped, unless it
// had already failed before the stop was asked for: that failure is how it
// ended (failedBeforeStop). A reason decided before the stop stands.
func Classify(f AttemptFacts) Verdict {
	observed := classifyObserved(f)
	if f.Reason == "" && f.StopRequestedAt != nil && !observed.Succeeded && !failedBeforeStop(f, observed) {
		return Verdict{Stopped: true}
	}
	return observed
}

// Exit codes of a container ended by a signal: 143 is SIGTERM, which the
// Job's deletion sends, and 137 SIGKILL, which follows once the pod's grace
// period ends.
const (
	exitSIGTERM = 143
	exitSIGKILL = 137
)

// failedBeforeStop reports whether v, the failure an attempt that was asked to
// stop shows, ended it before the stop request. It is judged by FinishedAt,
// when the main container (or the init container that failed in its place)
// terminated. FinishedAt is a Kubernetes container timestamp: the API keeps
// it to the second, dropping the fraction, and the node's clock wrote it.
// StopRequestedAt is execution-controller's clock, to the nanosecond, and the
// Job is deleted only after it is recorded, so an ending the deletion causes
// comes after it. The two are compared by second:
//   - a finish in an earlier second than the request's came before it: the
//     failure is the attempt's own;
//   - a finish in the request's own second is ambiguous, since the dropped
//     fraction may hide a finish just after the request: it counts as the
//     stop's doing only when it shows the signature of a signal kill, exit 143
//     or 137 read as a plain exit code (no memory kill, eviction or deadline
//     explains it), and as the attempt's own failure otherwise;
//   - a later finish, or none recorded (the pods were gone before a
//     terminated state was seen), is the stop's doing.
//
// No allowance is made for skew between the node's clock and
// execution-controller's beyond that second: clocks synchronised by NTP
// differ by milliseconds, and a misjudged ending changes only how one task
// of a run that ends cancelled is recorded (failed or cancelled); either way
// no pod of the attempt runs again.
func failedBeforeStop(f AttemptFacts, v Verdict) bool {
	if f.FinishedAt == nil {
		return false
	}
	finished := f.FinishedAt.Truncate(time.Second)
	requested := f.StopRequestedAt.Truncate(time.Second)
	switch {
	case finished.Before(requested):
		return true
	case finished.Equal(requested):
		return !killedBySignal(f, v)
	}
	return false
}

// killedBySignal reports whether v is a plain exit-code failure whose exit
// code is a signal kill's.
func killedBySignal(f AttemptFacts, v Verdict) bool {
	return v.Reason == pkgmodel.FailureReasonExitCode && f.ExitCode != nil &&
		(*f.ExitCode == exitSIGTERM || *f.ExitCode == exitSIGKILL)
}

// classifyObserved reads the verdict off the facts, ignoring any stop request.
func classifyObserved(f AttemptFacts) Verdict {
	switch {
	case f.Reason != "":
		return Verdict{Reason: f.Reason}
	case f.JobFailureReason == reasonDeadlineExceeded:
		return Verdict{Reason: pkgmodel.FailureReasonDeadlineExceeded}
	case f.TerminationReason == reasonOOMKilled:
		return Verdict{Reason: pkgmodel.FailureReasonOOMKilled}
	case f.PodReason == reasonEvicted:
		return Verdict{Reason: pkgmodel.FailureReasonEvicted}
	case f.ExitCode != nil && *f.ExitCode == 0 && f.FailedContainer == "":
		if f.NothingToDo {
			return Verdict{Reason: pkgmodel.FailureReasonDbtNothingToDo}
		}
		return Verdict{Succeeded: true}
	case f.ExitCode != nil:
		return Verdict{Reason: pkgmodel.FailureReasonExitCode}
	case f.JobSucceeded:
		return Verdict{Succeeded: true}
	default:
		return Verdict{Reason: pkgmodel.FailureReasonNodeLost}
	}
}
