package model

import (
	"time"

	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
)

// AttemptFacts is what execution-controller has observed of one attempt of a
// Job so far. Observations only add to it: a fact once known is never
// replaced, so a fact seen before the pod that showed it disappeared survives
// it.
type AttemptFacts struct {
	// StartedAt is when the main container started.
	StartedAt *time.Time
	// FinishedAt is when the main container terminated, or the init container
	// that failed in its place.
	FinishedAt *time.Time
	// ExitCode is the main container's exit code, or the exit code of the first
	// init container that exited non-zero when the main container never ran.
	ExitCode *int32
	// FailedContainer names the first init or main container that exited
	// non-zero.
	FailedContainer string
	// TerminationReason is the main container's termination reason, for
	// example OOMKilled.
	TerminationReason string
	// PodReason is the pod's status reason, for example Evicted.
	PodReason string
	// JobSucceeded is true once the Job reported success. JobFailureReason is
	// the reason of its failure condition, for example DeadlineExceeded.
	JobSucceeded     bool
	JobFailureReason string
	// Message is what Kubernetes said about a failure: a container's
	// termination or waiting message, or the pod's status message.
	Message string
	// NothingToDo is true when the pod's log shows dbt matched no node.
	NothingToDo bool
	// LogKey and RunResultsKey are the object-storage keys of the pod's log
	// and of the structured result block it printed. LogMessage is the failure
	// message the log carried: the result block's message, else the log tail.
	LogKey        string
	RunResultsKey string
	LogMessage    string
	// ParseCache and ParseCacheReason are what the parse-cache init container
	// reported.
	ParseCache       string
	ParseCacheReason string
	// AwaitingStopSince is when a pod of the attempt was first seen
	// terminating, or on a node that stopped reporting.
	AwaitingStopSince *time.Time
	// StopRequestedAt is when state asked for the attempt to stop because its
	// run was cancelled.
	StopRequestedAt *time.Time
	// Reason is why the attempt failed; empty until decided.
	Reason pkgmodel.FailureReason
}

// Class is the failure class of Reason; empty until a reason is decided.
func (f AttemptFacts) Class() pkgmodel.FailureClass { return f.Reason.Class() }

// Merge returns f with every fact next knows that f does not, and whether it
// added any. A fact f already knows is kept.
func (f AttemptFacts) Merge(next AttemptFacts) (AttemptFacts, bool) {
	changed := false
	fillTime(&f.StartedAt, next.StartedAt, &changed)
	fillTime(&f.FinishedAt, next.FinishedAt, &changed)
	fillTime(&f.AwaitingStopSince, next.AwaitingStopSince, &changed)
	fillTime(&f.StopRequestedAt, next.StopRequestedAt, &changed)
	if f.ExitCode == nil && next.ExitCode != nil {
		c := *next.ExitCode
		f.ExitCode, changed = &c, true
	}
	for _, s := range []struct {
		dst *string
		src string
	}{
		{&f.FailedContainer, next.FailedContainer},
		{&f.TerminationReason, next.TerminationReason},
		{&f.PodReason, next.PodReason},
		{&f.JobFailureReason, next.JobFailureReason},
		{&f.Message, next.Message},
		{&f.LogKey, next.LogKey},
		{&f.RunResultsKey, next.RunResultsKey},
		{&f.LogMessage, next.LogMessage},
		{&f.ParseCache, next.ParseCache},
		{&f.ParseCacheReason, next.ParseCacheReason},
	} {
		if *s.dst == "" && s.src != "" {
			*s.dst, changed = s.src, true
		}
	}
	if f.Reason == "" && next.Reason != "" {
		f.Reason, changed = next.Reason, true
	}
	if !f.JobSucceeded && next.JobSucceeded {
		f.JobSucceeded, changed = true, true
	}
	if !f.NothingToDo && next.NothingToDo {
		f.NothingToDo, changed = true, true
	}
	return f, changed
}

func fillTime(dst **time.Time, src *time.Time, changed *bool) {
	if *dst == nil && src != nil {
		t := *src
		*dst, *changed = &t, true
	}
}
