package model

import (
	"strings"
	"time"

	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/parsecache"
)

// PodPhase is a pod's phase as Kubernetes reports it.
type PodPhase string

const (
	PodPending   PodPhase = "Pending"
	PodRunning   PodPhase = "Running"
	PodSucceeded PodPhase = "Succeeded"
	PodFailed    PodPhase = "Failed"
	PodUnknown   PodPhase = "Unknown"
)

// ContainerObservation is what Kubernetes shows of one container of a pod.
type ContainerObservation struct {
	Name string
	// StartedAt is when the container started running; nil until it has.
	StartedAt *time.Time
	// Terminated is true once the container exited; the other Terminated*
	// fields, ExitCode and FinishedAt say how.
	Terminated        bool
	ExitCode          int32
	TerminatedReason  string
	TerminatedMessage string
	FinishedAt        *time.Time
	// WaitingReason and WaitingMessage say why the container is not running.
	WaitingReason  string
	WaitingMessage string
}

// PodObservation is what Kubernetes shows of one pod of an attempt's Job.
type PodObservation struct {
	Name    string
	Phase   PodPhase
	Reason  string
	Message string
	// DeletingSince is set once the pod carries a deletion timestamp.
	DeletingSince *time.Time
	// Main is the pod's main container: every Job pod has exactly one.
	Main  ContainerObservation
	Inits []ContainerObservation
}

// Finished reports whether the pod can never run again: its phase is
// Succeeded or Failed. A deletion timestamp alone does not finish a pod.
func (p PodObservation) Finished() bool { return p.Phase == PodSucceeded || p.Phase == PodFailed }

// JobObservation is what Kubernetes shows of one attempt: its Job, when the
// cache holds it, and every pod the Job created that the cache holds.
type JobObservation struct {
	JobPresent bool
	// JobDeleting is true once the Job carries a deletion timestamp.
	JobDeleting bool
	// JobSucceeded and JobFailed report the Job's success and failure
	// conditions; JobFailureReason is the failure condition's reason.
	JobSucceeded     bool
	JobFailed        bool
	JobFailureReason string
	Pods             []PodObservation
}

// MainStarted reports whether the main container of a pod has started.
func (o JobObservation) MainStarted() bool {
	for _, p := range o.Pods {
		if p.Main.StartedAt != nil {
			return true
		}
	}
	return false
}

// Ended reports whether the attempt has ended: no pod of it can run again, and
// the Job accounts for that — it finished, or it is being deleted or gone. A
// Job that holds no pod and has not finished is still creating one.
func (o JobObservation) Ended() bool {
	for _, p := range o.Pods {
		if !p.Finished() {
			return false
		}
	}
	return !o.JobPresent || o.JobDeleting || o.JobSucceeded || o.JobFailed
}

// AwaitingStop reports whether a pod that has not finished is being deleted,
// or sits on a node that stopped reporting (phase Unknown). Its container may
// still be running, so the attempt waits for it, and its log is read at once
// because the pod may vanish.
func (o JobObservation) AwaitingStop() bool {
	for _, p := range o.Pods {
		if !p.Finished() && (p.DeletingSince != nil || p.Phase == PodUnknown) {
			return true
		}
	}
	return false
}

// startErrors are the waiting reasons Kubernetes never resolves by itself,
// with the failure reason each one ends the attempt with.
var startErrors = map[string]pkgmodel.FailureReason{
	"ErrImagePull":               pkgmodel.FailureReasonImagePull,
	"ImagePullBackOff":           pkgmodel.FailureReasonImagePull,
	"InvalidImageName":           pkgmodel.FailureReasonInvalidImage,
	"CreateContainerConfigError": pkgmodel.FailureReasonMissingConfig,
	"CreateContainerError":       pkgmodel.FailureReasonCreateContainer,
}

// StartError returns the failure reason of a container, init or main, waiting
// on a start condition that never resolves by itself, with Kubernetes'
// message; ok is false when there is none.
func (o JobObservation) StartError() (reason pkgmodel.FailureReason, message string, ok bool) {
	for _, p := range o.Pods {
		for _, c := range append(append([]ContainerObservation{}, p.Inits...), p.Main) {
			if r, found := startErrors[c.WaitingReason]; found {
				return r, joinReason(c.WaitingReason, c.WaitingMessage), true
			}
		}
	}
	return "", "", false
}

// Facts returns the facts this observation shows; now dates a pod first seen
// awaiting its stop.
func (o JobObservation) Facts(now time.Time) AttemptFacts {
	f := AttemptFacts{JobSucceeded: o.JobSucceeded}
	if o.JobFailed {
		f.JobFailureReason = o.JobFailureReason
	}
	if o.AwaitingStop() {
		f.AwaitingStopSince = &now
	}
	for _, p := range o.Pods {
		f, _ = f.Merge(podFacts(p))
	}
	return f
}

// podFacts returns the facts one pod shows.
func podFacts(p PodObservation) AttemptFacts {
	f := AttemptFacts{StartedAt: p.Main.StartedAt, PodReason: p.Reason}
	if p.Main.Terminated {
		exit := p.Main.ExitCode
		f.ExitCode, f.FinishedAt = &exit, p.Main.FinishedAt
		f.TerminationReason = p.Main.TerminatedReason
		f.Message = joinReason(p.Main.TerminatedReason, p.Main.TerminatedMessage)
	}
	for _, c := range p.Inits {
		if !c.Terminated {
			continue
		}
		if c.Name == parsecache.ContainerName {
			f.ParseCache, f.ParseCacheReason = parseCacheState(c.TerminatedMessage)
		}
		if c.ExitCode != 0 && f.FailedContainer == "" {
			f.FailedContainer = c.Name
			if !p.Main.Terminated {
				exit := c.ExitCode
				f.ExitCode, f.FinishedAt = &exit, c.FinishedAt
				f.Message = joinReason(c.TerminatedReason, c.TerminatedMessage)
			}
		}
	}
	if f.FailedContainer == "" && p.Main.Terminated && p.Main.ExitCode != 0 {
		f.FailedContainer = p.Main.Name
	}
	if f.Message == "" {
		f.Message = p.Message
	}
	return f
}

// parseCacheState reads the parse-cache init container's termination message:
// hydrated, degraded with its reason, or unknown for any other message.
func parseCacheState(msg string) (state, reason string) {
	switch {
	case msg == parsecache.Hydrated:
		return parsecache.Hydrated, ""
	case strings.HasPrefix(msg, parsecache.DegradedPrefix):
		return "degraded", strings.TrimPrefix(msg, parsecache.DegradedPrefix)
	default:
		return "unknown", ""
	}
}

// joinReason renders a Kubernetes reason and message as "reason: message",
// or whichever of the two is set.
func joinReason(reason, message string) string {
	switch {
	case reason == "":
		return message
	case message == "":
		return reason
	default:
		return reason + ": " + message
	}
}
