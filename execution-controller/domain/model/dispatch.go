package model

import (
	"fmt"
	"regexp"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/command"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

// DispatchAction is what execution-controller does with one dispatch of an
// attempt.
type DispatchAction int

const (
	// ResendReport: the attempt is accepted already; its last report is sent
	// again.
	ResendReport DispatchAction = iota + 1
	// IgnoreOlder: a later attempt of the task is accepted, so this one is
	// never created.
	IgnoreOlder
	// RefuseInvalid: the dispatch cannot run; the attempt is recorded failed
	// with the problem's reason.
	RefuseInvalid
	// AdoptJob: no record of the attempt is held, but its Job exists; the
	// record is rebuilt from the dispatch and the Job.
	AdoptJob
	// CreateAttempt: the dispatch is new; the attempt is queued for its Job.
	CreateAttempt
	// ReportLost: state knows the attempt as acknowledged, yet neither its
	// record nor its Job is held, and no pod of it is left; it is reported
	// failed as attempt_lost.
	ReportLost
	// AwaitOrphanPod: state knows the attempt as acknowledged and neither its
	// record nor its Job is held, but a pod of it has not finished (its Job
	// was deleted without its pods). Nothing is written: the pod may still
	// write the table, and state asks again.
	AwaitOrphanPod
)

// DecideDispatch decides what a dispatch leads to, from whether the attempt's
// record is held, whether a later attempt's is, the problem (if any) that
// stops the dispatch from running, whether a Job carrying the task and
// attempt exists, whether a pod of the attempt has not finished, and state's
// known status. Only a dispatch state still knows as dispatched creates an
// attempt: an acknowledged attempt is never run again because of a question,
// and is reported lost only once no pod of it can run.
func DecideDispatch(held, laterHeld bool, problem pkgmodel.FailureReason, jobOfAttempt, podOfAttemptRunning bool,
	knownStatus pkgmodel.AttemptStatus) DispatchAction {
	switch {
	case held:
		return ResendReport
	case laterHeld:
		return IgnoreOlder
	case problem != "":
		return RefuseInvalid
	case jobOfAttempt:
		return AdoptJob
	case knownStatus == pkgmodel.AttemptStatusDispatched:
		return CreateAttempt
	case podOfAttemptRunning:
		return AwaitOrphanPod
	}
	return ReportLost
}

// FencedAction is what execution-controller does with an attempt of a fenced
// run that it holds no record of: one a dispatch of the run carries, or one
// the run's cancel names. No such attempt starts.
type FencedAction int

const (
	// StopAdopted: a Job of the attempt exists; its record is rebuilt from
	// the command and moved to terminating with the stop recorded, so the Job
	// is deleted and the attempt reported stopped once its pods are gone.
	StopAdopted FencedAction = iota + 1
	// AwaitFencedPod: no Job of the attempt exists, but a pod of it has not
	// finished (its Job was deleted without its pods). Nothing is written
	// while the pod can still run; state's next cancel names the attempt
	// again.
	AwaitFencedPod
	// StoreStopped: neither a Job nor an unfinished pod of the attempt
	// exists; the attempt is stored done with the outcome stopped and
	// reported stopped.
	StoreStopped
)

// DecideFenced decides what an attempt of a fenced run with no record leads
// to, from whether a Job carrying its task and attempt exists and whether a
// pod of it has not finished.
func DecideFenced(jobOfAttempt, podOfAttemptUnfinished bool) FencedAction {
	switch {
	case jobOfAttempt:
		return StopAdopted
	case podOfAttemptUnfinished:
		return AwaitFencedPod
	}
	return StoreStopped
}

// maxJobNameLen is the longest name of a Kubernetes Job: the Job controller
// copies it into a label value, which holds at most 63 characters.
const maxJobNameLen = 63

// jobNamePattern is a DNS-1123 label: lowercase alphanumerics and hyphens,
// starting and ending alphanumeric. It is the form of every job_name state
// computes, and the only one the Kubernetes API can look up by name or by the
// Job-name label selector.
var jobNamePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// DispatchProblem returns the reason and message of what stops cmd, the
// command of a dispatch state knows in knownStatus (dispatched, queued or
// running; any other value cannot run), from running; an empty
// reason when nothing does. A job_name that is not a DNS-1123 label of 1 to 63
// characters stops it: Kubernetes can never read such a Job, so no lookup of
// the attempt's Job could ever answer. The Job itself may still be refused
// when it is built (deploy.ErrMalformedDispatch).
func DispatchProblem(cmd command.DeployTask, knownStatus pkgmodel.AttemptStatus) (pkgmodel.FailureReason, string) {
	if _, err := pkgmodel.ParseNodeType(cmd.NodeType); err != nil {
		return pkgmodel.FailureReasonInvalidNodeType, err.Error()
	}
	problem := ""
	switch {
	case knownStatus != pkgmodel.AttemptStatusDispatched && knownStatus != pkgmodel.AttemptStatusQueued &&
		knownStatus != pkgmodel.AttemptStatusRunning:
		problem = fmt.Sprintf("known_status %q is not dispatched, queued or running", knownStatus)
	case len(cmd.JobName) > maxJobNameLen || !jobNamePattern.MatchString(cmd.JobName):
		problem = fmt.Sprintf("job_name %q is not a Kubernetes name: 1 to %d lowercase letters, digits or dashes, starting and ending with a letter or digit",
			cmd.JobName, maxJobNameLen)
	case cmd.ImageTag == "":
		problem = "image_tag is empty"
	case cmd.TimeoutSeconds < 1:
		problem = fmt.Sprintf("timeout_seconds %d is below 1", cmd.TimeoutSeconds)
	default:
		if _, err := pkgmodel.ParseOperation(cmd.Operation); err != nil {
			problem = err.Error()
		}
	}
	if problem != "" {
		return pkgmodel.FailureReasonMalformedDispatch, problem
	}
	return "", ""
}

// NewAdoptedDeployment rebuilds the record of an attempt whose Job exists:
// it holds a slot and is starting, as if just created, so the observer
// follows the Job to its end.
func NewAdoptedDeployment(cmd command.DeployTask, msgProcID *uuid.UUID, now time.Time) *Deployment {
	d := NewDeployment(cmd, msgProcID, now)
	d.status = StatusStarting
	d.deployedAt = &now
	return d
}

// NewStoppedDeployment records an attempt dispatched after it was asked to
// stop: done at once with the outcome stopped and no Job, so a repeated
// dispatch finds it and is answered with its report.
func NewStoppedDeployment(cmd command.DeployTask, msgProcID *uuid.UUID, now time.Time) *Deployment {
	d := NewDeployment(cmd, msgProcID, now)
	d.status = StatusDone
	d.outcome = OutcomeStopped
	d.outcomeAt = &now
	d.recordStopRequest(now)
	return d
}

// NewRefusedDeployment records an attempt that cannot run as dispatched:
// failed at once with reason and message, its outcome recorded, so a repeated
// dispatch finds it and is answered with its report.
func NewRefusedDeployment(cmd command.DeployTask, msgProcID *uuid.UUID, reason pkgmodel.FailureReason, message string, now time.Time) *Deployment {
	d := NewDeployment(cmd, msgProcID, now)
	d.status = StatusFailed
	d.setError(message)
	d.outcome = "failed"
	d.outcomeAt = &now
	d.facts, _ = d.facts.Merge(AttemptFacts{Reason: reason, Message: message})
	return d
}
