package node

import (
	"context"
	"fmt"
	"io"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	statev1 "github.com/carolsimone/continuo/cli/proto/state/v1"
	"github.com/spf13/cobra"
)

// nodeRun is the JSON-serialisable representation of one run. Fields the state
// service leaves empty while a run is in flight or on success are omitted.
type nodeRun struct {
	RunID           string `json:"run_id"`
	ScheduleName    string `json:"schedule_name"`
	Kind            string `json:"kind"`
	TerminalStatus  string `json:"terminal_status,omitempty"`
	TaskID          string `json:"task_id"`
	TaskStatus      string `json:"task_status"`
	RetryCount      int32  `json:"retry_count"`
	ImageTag        string `json:"image_tag"`
	ManifestVersion string `json:"manifest_version"`
	CreatedAt       string `json:"created_at"`
	StartedAt       string `json:"started_at,omitempty"`
	CompletedAt     string `json:"completed_at,omitempty"`
	ErrorMessage    string `json:"error_message,omitempty"`
	LogS3Key        string `json:"log_s3_key,omitempty"`
}

type historyPayload struct {
	Runs []nodeRun `json:"runs"`
}

// nodeHistoryLimit is the fixed page size. The state service clamps to (0,50];
// the CLI always requests the full window.
const nodeHistoryLimit int32 = 50

// NewHistoryCommand builds `continuo node history <service> <schema> <table>`.
func NewHistoryCommand(factory StateClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "history <service> <schema> <table>",
		Short: "List the recent run history of one model node",
		Long: `List the recent run history of one model node.

Use when the user wants to see the last runs of a specific dbt model, including
whether each run succeeded, which image and manifest version it used, and any
error message.

Arguments:
  <service>  The owning service name.
  <schema>   The schema name.
  <table>    The table (model) name.

Output (stdout, JSON): up to 50 runs, newest first.
  {"runs":[{"run_id":string,"schedule_name":string,"kind":string,
   "terminal_status":string,"task_id":string,"task_status":string,
   "retry_count":number,"image_tag":string,"manifest_version":string,
   "created_at":string,"started_at":string,"completed_at":string,
   "error_message":string,"log_s3_key":string}]}
  terminal_status, started_at, completed_at, error_message, and log_s3_key are
  omitted when empty. An unknown node returns {"runs":[]}, not an error.

Errors:
  usage      (exit 2)  wrong number of arguments
  unavailable(exit 5)  the state service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo node history finance analytics orders",
		Annotations: map[string]string{
			"output_schema": `{"runs":"array"}`,
			"exit_codes":    `[0,2,5,6]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 3 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("history requires exactly three arguments: <service> <schema> <table>"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.StateEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.ListNodeRuns(ctx, args[0], args[1], args[2], nodeHistoryLimit)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return humanHistory(stderr, resp.GetRuns())
			}
			return output.EmitSuccess(stdout, toHistoryPayload(resp.GetRuns()))
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

func toHistoryPayload(runs []*statev1.NodeRun) historyPayload {
	out := make([]nodeRun, 0, len(runs))
	for _, r := range runs {
		out = append(out, nodeRun{
			RunID:           r.GetRunId(),
			ScheduleName:    r.GetScheduleName(),
			Kind:            r.GetKind(),
			TerminalStatus:  r.GetTerminalStatus(),
			TaskID:          r.GetTaskId(),
			TaskStatus:      r.GetTaskStatus(),
			RetryCount:      r.GetRetryCount(),
			ImageTag:        r.GetImageTag(),
			ManifestVersion: r.GetManifestVersion(),
			CreatedAt:       r.GetCreatedAt(),
			StartedAt:       r.GetStartedAt(),
			CompletedAt:     r.GetCompletedAt(),
			ErrorMessage:    r.GetErrorMessage(),
			LogS3Key:        r.GetLogS3Key(),
		})
	}
	return historyPayload{Runs: out}
}

// NewTriggerCommand is a temporary placeholder replaced by trigger.go in the
// next task; it exists only so the package compiles while node history is
// developed in isolation.
func NewTriggerCommand(_ StateClientFactory, _ *config.Config, _, _ io.Writer) *cobra.Command {
	return &cobra.Command{Use: "trigger", Short: "placeholder", RunE: func(*cobra.Command, []string) error { return nil }}
}

// humanHistory writes one line per run to stderr:
//
//	<run_id>  <task_status>  <kind>  <completed_at>
func humanHistory(stderr io.Writer, runs []*statev1.NodeRun) error {
	for _, r := range runs {
		if _, err := fmt.Fprintf(stderr, "%s  %s  %s  %s\n",
			r.GetRunId(), r.GetTaskStatus(), r.GetKind(), r.GetCompletedAt()); err != nil {
			return err
		}
	}
	return nil
}
