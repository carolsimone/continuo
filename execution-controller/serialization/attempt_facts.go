package serialization

import (
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
)

// AttemptFactsDTO is the JSON shape of model.AttemptFacts as stored in the
// deployments.facts JSONB column. Every field is omitted while unknown;
// failure_class is written from failure_reason and ignored on reading.
type AttemptFactsDTO struct {
	StartedAt         *time.Time `json:"started_at,omitempty"`
	FinishedAt        *time.Time `json:"finished_at,omitempty"`
	ExitCode          *int32     `json:"exit_code,omitempty"`
	FailedContainer   string     `json:"failed_container,omitempty"`
	TerminationReason string     `json:"termination_reason,omitempty"`
	PodReason         string     `json:"pod_reason,omitempty"`
	JobSucceeded      bool       `json:"job_succeeded,omitempty"`
	JobFailureReason  string     `json:"job_failure_reason,omitempty"`
	Message           string     `json:"message,omitempty"`
	NothingToDo       bool       `json:"nothing_to_do,omitempty"`
	LogKey            string     `json:"log_s3_key,omitempty"`
	RunResultsKey     string     `json:"run_results_uri,omitempty"`
	LogMessage        string     `json:"log_message,omitempty"`
	ParseCache        string     `json:"parse_cache,omitempty"`
	ParseCacheReason  string     `json:"parse_cache_reason,omitempty"`
	AwaitingStopSince *time.Time `json:"awaiting_stop_since,omitempty"`
	StopRequestedAt   *time.Time `json:"stop_requested_at,omitempty"`
	FailureClass      string     `json:"failure_class,omitempty"`
	FailureReason     string     `json:"failure_reason,omitempty"`
}

// AttemptFactsFromDomain maps attempt facts to their DTO.
func AttemptFactsFromDomain(f model.AttemptFacts) AttemptFactsDTO {
	return AttemptFactsDTO{
		StartedAt: f.StartedAt, FinishedAt: f.FinishedAt, ExitCode: f.ExitCode, FailedContainer: f.FailedContainer,
		TerminationReason: f.TerminationReason, PodReason: f.PodReason,
		JobSucceeded: f.JobSucceeded, JobFailureReason: f.JobFailureReason,
		Message: f.Message, NothingToDo: f.NothingToDo,
		LogKey: f.LogKey, RunResultsKey: f.RunResultsKey, LogMessage: f.LogMessage,
		ParseCache: f.ParseCache, ParseCacheReason: f.ParseCacheReason,
		AwaitingStopSince: f.AwaitingStopSince, StopRequestedAt: f.StopRequestedAt,
		FailureClass: string(f.Class()), FailureReason: string(f.Reason),
	}
}

// ToDomain maps a decoded DTO back to attempt facts.
func (d AttemptFactsDTO) ToDomain() model.AttemptFacts {
	return model.AttemptFacts{
		StartedAt: d.StartedAt, FinishedAt: d.FinishedAt, ExitCode: d.ExitCode, FailedContainer: d.FailedContainer,
		TerminationReason: d.TerminationReason, PodReason: d.PodReason,
		JobSucceeded: d.JobSucceeded, JobFailureReason: d.JobFailureReason,
		Message: d.Message, NothingToDo: d.NothingToDo,
		LogKey: d.LogKey, RunResultsKey: d.RunResultsKey, LogMessage: d.LogMessage,
		ParseCache: d.ParseCache, ParseCacheReason: d.ParseCacheReason,
		AwaitingStopSince: d.AwaitingStopSince, StopRequestedAt: d.StopRequestedAt,
		Reason: pkgmodel.FailureReason(d.FailureReason),
	}
}
