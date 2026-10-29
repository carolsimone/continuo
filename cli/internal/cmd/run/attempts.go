package run

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/carolsimone/continuo/cli/internal/runview"
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/spf13/cobra"
)

// attemptsPageSize is the page size of the one page `run attempts` reads; it
// is the largest the server serves.
const attemptsPageSize = runview.PageSize

type attemptEntry struct {
	TaskID           string  `json:"task_id"`
	Attempt          int32   `json:"attempt"`
	JobName          string  `json:"job_name"`
	Status           string  `json:"status"`
	FailureClass     string  `json:"failure_class,omitempty"`
	FailureReason    string  `json:"failure_reason,omitempty"`
	ExitCode         int32   `json:"exit_code"`
	DispatchedAt     string  `json:"dispatched_at,omitempty"`
	QueuedAt         string  `json:"queued_at,omitempty"`
	StartedAt        string  `json:"started_at,omitempty"`
	FinishedAt       string  `json:"finished_at,omitempty"`
	ExecutionSeconds float64 `json:"execution_seconds"`
	ErrorMessage     string  `json:"error_message,omitempty"`
	LogS3Key         string  `json:"log_s3_key,omitempty"`
	RunResultsURI    string  `json:"run_results_uri,omitempty"`
}

type attemptsPayload struct {
	Attempts   []attemptEntry `json:"attempts"`
	TotalCount int32          `json:"total_count"`
}

// NewAttemptsCommand builds `continuo run attempts <run-id> [task-id]`.
func NewAttemptsCommand(factory RunClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "attempts <run-id> [task-id]",
		Short: "List the attempts of a run's tasks, or of one task",
		Long: `List the attempts of a run's tasks, or of one task.

Use when the user asks why a task failed, how often it was retried, how long an
attempt took, or where its logs and results are. "run show" gives each task's
latest failure; this lists every attempt with its timings and error.

Arguments:
  <run-id>   The UUID of the run.
  [task-id]  Optional. The UUID of one task of the run (the task_id of "run graph");
             only that task's attempts are listed. A blank value is refused.

Output (stdout, JSON):
  {"attempts":[{"task_id":string,"attempt":number,"job_name":string,"status":string,
                "failure_class":string,"failure_reason":string,"exit_code":number,
                "dispatched_at":string,"queued_at":string,"started_at":string,
                "finished_at":string,"execution_seconds":number,"error_message":string,
                "log_s3_key":string,"run_results_uri":string}],
   "total_count":number}
  Attempts are ordered by task id, then attempt number (0-based). status is one of
  dispatched|queued|running|succeeded|failed|stopped. failure_class and failure_reason
  are set only for a failed attempt; a timestamp, error_message, log_s3_key and
  run_results_uri are omitted until recorded. Timestamps are RFC3339 in UTC.
  exit_code 0 is a clean exit only for a succeeded attempt; it is also what an
  unknown exit code reads as.
  The output is ONE page of at most 200 attempts, in that order. total_count counts every
  attempt the filter matches: when it exceeds the length of "attempts", the list is
  truncated, so name a task-id to read one task's attempts.
  An unknown run, or a task the run does not have, lists nothing and exits 0.

Errors:
  usage      (exit 2)  wrong number of arguments, a blank task id, or an id that is not a UUID
  unavailable(exit 5)  the run-controller service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo run attempts 0b6f4a52-3c1d-4d7e-9a0b-1f2e3d4c5b6a\n  continuo run attempts 0b6f4a52-3c1d-4d7e-9a0b-1f2e3d4c5b6a 7c9e6679-7425-40de-944b-e07fc1f90ae7",
		Annotations: map[string]string{
			"output_schema": `{"attempts":[{"task_id":"string","attempt":"number","job_name":"string","status":"string","failure_class":"string","failure_reason":"string","exit_code":"number","dispatched_at":"string","queued_at":"string","started_at":"string","finished_at":"string","execution_seconds":"number","error_message":"string","log_s3_key":"string","run_results_uri":"string"}],"total_count":"number"}`,
			"exit_codes":    `[0,2,5,6]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 || len(args) > 2 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("attempts requires <run-id> and an optional [task-id]"))
			}
			if len(args) == 2 && strings.TrimSpace(args[1]) == "" {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("[task-id] must not be blank; omit it to list every task's attempts"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			taskID := ""
			if len(args) == 2 {
				taskID = args[1]
			}

			c, err := factory(cmd.Context(), cfg.RunControllerEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()
			resp, err := c.ListRunAttempts(ctx, args[0], taskID, attemptsPageSize, 0)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			payload := toAttemptsPayload(resp)
			if cfg.Human {
				return humanAttempts(stderr, payload)
			}
			return output.EmitSuccess(stdout, payload)
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

// toAttemptsPayload converts the response into the JSON payload; the list is
// never nil, so it prints as [] and not null.
func toAttemptsPayload(resp *runv1.ListRunAttemptsResponse) attemptsPayload {
	p := attemptsPayload{
		Attempts:   make([]attemptEntry, 0, len(resp.GetAttempts())),
		TotalCount: resp.GetTotalCount(),
	}
	for _, a := range resp.GetAttempts() {
		p.Attempts = append(p.Attempts, attemptEntry{
			TaskID:           a.GetTaskId(),
			Attempt:          a.GetAttempt(),
			JobName:          a.GetJobName(),
			Status:           a.GetStatus(),
			FailureClass:     a.GetFailureClass(),
			FailureReason:    a.GetFailureReason(),
			ExitCode:         a.GetExitCode(),
			DispatchedAt:     runview.Timestamp(a.GetDispatchedAt()),
			QueuedAt:         runview.Timestamp(a.GetQueuedAt()),
			StartedAt:        runview.Timestamp(a.GetStartedAt()),
			FinishedAt:       runview.Timestamp(a.GetFinishedAt()),
			ExecutionSeconds: a.GetExecutionSeconds(),
			ErrorMessage:     a.GetErrorMessage(),
			LogS3Key:         a.GetLogS3Key(),
			RunResultsURI:    a.GetRunResultsUri(),
		})
	}
	return p
}

// humanAttempts writes one line per attempt to w.
func humanAttempts(w io.Writer, p attemptsPayload) error {
	if _, err := fmt.Fprintf(w, "%d of %d attempts\n", len(p.Attempts), p.TotalCount); err != nil {
		return err
	}
	for _, a := range p.Attempts {
		line := fmt.Sprintf("  %s  attempt %d  %s", a.TaskID, a.Attempt, a.Status)
		if a.FailureClass != "" {
			line += fmt.Sprintf("  %s %s", a.FailureClass, a.FailureReason)
		}
		if _, err := fmt.Fprintln(w, line); err != nil {
			return err
		}
	}
	return nil
}
