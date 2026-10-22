package model

import (
	"time"

	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
)

// Report is what execution-controller reported of one production attempt:
// its status and every fact known of it when the report was written. Status
// is a value of the attempt_status vocabulary: queued once the attempt was
// accepted and waits for a slot or its Job, running once its main container
// started, succeeded or failed once it ended and no pod of it can run again.
// A report is cumulative, so the last one stands for all before it.
type Report struct {
	Status           pkgmodel.AttemptStatus
	QueuedAt         *time.Time
	StartedAt        *time.Time
	FinishedAt       *time.Time
	ExecutionSeconds *float64
	ExitCode         *int32
	FailureReason    pkgmodel.FailureReason
	ErrorMessage     string
	LogKey           string
	RunResultsKey    string
	ParseCache       string
	ParseCacheReason string
}

// ReportOf builds the report of d's attempt with status from everything
// observed of it: queued when the deployment was accepted, the main
// container's start and end and the time between them, its exit code, the
// log and result keys and the parse cache, and, for a failure, its reason
// and errorMessage.
func (d *Deployment) ReportOf(status pkgmodel.AttemptStatus, errorMessage string) Report {
	f := d.facts
	queued := d.createdAt
	r := Report{
		Status: status, QueuedAt: &queued, StartedAt: f.StartedAt, FinishedAt: f.FinishedAt, ExitCode: f.ExitCode,
		LogKey: f.LogKey, RunResultsKey: f.RunResultsKey, ParseCache: f.ParseCache, ParseCacheReason: f.ParseCacheReason,
	}
	if f.StartedAt != nil && f.FinishedAt != nil {
		secs := f.FinishedAt.Sub(*f.StartedAt).Seconds()
		r.ExecutionSeconds = &secs
	}
	if status == pkgmodel.AttemptStatusFailed {
		r.FailureReason, r.ErrorMessage = f.Reason, errorMessage
	}
	return r
}

// RecordReport keeps r as the report last emitted for the attempt, which a
// repeated dispatch of the attempt is answered with.
func (d *Deployment) RecordReport(r Report) { d.lastReport = &r }

// LastReport is the report last emitted for the attempt; nil before any.
func (d *Deployment) LastReport() *Report { return d.lastReport }

// HydrateLastReport restores the report last emitted for the attempt.
// Adapter-only: domain code records a report through RecordReport.
func (d *Deployment) HydrateLastReport(r *Report) { d.lastReport = r }
