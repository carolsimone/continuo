package schedule

import (
	"context"
	"fmt"
	"io"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/spf13/cobra"
)

type runEntry struct {
	RunID         string `json:"run_id"`
	ScheduleName  string `json:"schedule_name"`
	Status        string `json:"status"`
	CreatedAt     string `json:"created_at"`
	CompletedAt   string `json:"completed_at"`
	FailureReason string `json:"failure_reason,omitempty"`
}

type runsPayload struct {
	Runs       []runEntry `json:"runs"`
	TotalCount int32      `json:"total_count"`
}

// NewRunsCommand builds `continuo schedule runs <schedule-name>`.
func NewRunsCommand(factory RunClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "runs <schedule-name>",
		Short: "List the completed runs of a schedule, newest first",
		Long: `List the completed runs of a schedule, newest first.

Use when the user asks how a schedule did over time, when it last failed, or for
the id of an earlier run to inspect with "run show". A run that has not ended
(pending, running or cancelling) is not listed: "schedule status" shows it.

Arguments:
  <schedule-name>  The schedule whose runs to list (exact match).

Output (stdout, JSON):
  {"runs":[{"run_id":string,"schedule_name":string,"status":string,
            "created_at":string,"completed_at":string,"failure_reason":string}],
   "total_count":number}
  status is one of succeeded|failed|cancelled|skipped. created_at and completed_at are
  RFC3339 in UTC. failure_reason (stalled|empty_selection|invalid_node_type) is set only
  when the run failed without a failed task and is omitted otherwise.
  The output is ONE page: the newest runs, at most the server's default page size.
  total_count counts every completed run of the schedule: when it exceeds the length
  of "runs", older runs exist that this command does not list.
  A schedule with no completed run, or one that does not exist, returns "runs":[]
  and "total_count":0 with exit 0.

Errors:
  usage      (exit 2)  wrong number of arguments
  unavailable(exit 5)  the run-controller service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo schedule runs daily-revenue",
		Annotations: map[string]string{
			"output_schema": `{"runs":[{"run_id":"string","schedule_name":"string","status":"string","created_at":"string","completed_at":"string","failure_reason":"string"}],"total_count":"number"}`,
			"exit_codes":    `[0,2,5,6]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("runs requires exactly one argument: <schedule-name>"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := factory(cmd.Context(), cfg.RunControllerEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()
			// A page_size of 0 asks for the server's default page.
			resp, err := c.ListRuns(ctx, args[0], 0, 0)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			payload := toRunsPayload(resp)
			if cfg.Human {
				return humanRuns(stderr, payload)
			}
			return output.EmitSuccess(stdout, payload)
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

// toRunsPayload converts the response into the JSON payload; the list is never
// nil, so it prints as [] and not null.
func toRunsPayload(resp *runv1.ListRunsResponse) runsPayload {
	p := runsPayload{Runs: make([]runEntry, 0, len(resp.GetRuns())), TotalCount: resp.GetTotalCount()}
	for _, r := range resp.GetRuns() {
		p.Runs = append(p.Runs, runEntry{
			RunID:         r.GetRunId(),
			ScheduleName:  r.GetScheduleName(),
			Status:        r.GetStatus(),
			CreatedAt:     r.GetCreatedAt(),
			CompletedAt:   r.GetCompletedAt(),
			FailureReason: r.GetFailureReason(),
		})
	}
	return p
}

// humanRuns writes one line per run to w.
func humanRuns(w io.Writer, p runsPayload) error {
	if _, err := fmt.Fprintf(w, "%d of %d runs\n", len(p.Runs), p.TotalCount); err != nil {
		return err
	}
	for _, r := range p.Runs {
		if _, err := fmt.Fprintf(w, "  %s  %s  %s\n", r.RunID, r.Status, r.CompletedAt); err != nil {
			return err
		}
	}
	return nil
}
