package model

import pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"

// Verdict is how an attempt ended: succeeded, or failed for Reason.
type Verdict struct {
	Succeeded bool
	Reason    pkgmodel.FailureReason
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
func Classify(f AttemptFacts) Verdict {
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
