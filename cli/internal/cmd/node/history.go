package node

import (
	"context"
	"fmt"
	"io"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/spf13/cobra"
)

// nodeRun is the JSON-serialisable representation of one run. Fields the state
// service leaves empty while a run is in flight or on success are omitted.
type nodeRun struct {
	RunID        string `json:"run_id"`
	ScheduleName string `json:"schedule_name"`
	Kind         string `json:"kind"`
	RunStatus    string `json:"run_status,omitempty"`
	TaskID       string `json:"task_id"`
	TaskStatus   string `json:"task_status"`
	// Attempt is the zero-based number of the task's current attempt.
	Attempt      int32  `json:"attempt"`
	ImageTag     string `json:"image_tag"`
	CreatedAt    string `json:"created_at"`
	StartedAt    string `json:"started_at,omitempty"`
	CompletedAt  string `json:"completed_at,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
	// FailureClass and FailureReason say why the task's latest attempt failed;
	// both are empty unless it failed.
	FailureClass  string `json:"failure_class,omitempty"`
	FailureReason string `json:"failure_reason,omitempty"`
	LogS3Key      string `json:"log_s3_key,omitempty"`
	RunResultsURI string `json:"run_results_uri,omitempty"`
	Operation     string `json:"operation"`
	// ContentHash is the node's content hash in the release the run ran on, as
	// the state service records it. Empty for runs that name no release.
	ContentHash string `json:"content_hash,omitempty"`
	// WaitReason, WaitingOnRunID and WaitingSince say why the run's pending
	// task waits besides its upstreams, on which run and since when; each is
	// omitted when the state service leaves it empty.
	WaitReason     string `json:"wait_reason,omitempty"`
	WaitingOnRunID string `json:"waiting_on_run_id,omitempty"`
	WaitingSince   string `json:"waiting_since,omitempty"`
}

type historyPayload struct {
	Runs []nodeRun `json:"runs"`
}

// nodeHistoryLimit is the fixed page size. The state service clamps to (0,50];
// the CLI always requests the full window.
const nodeHistoryLimit int32 = 50

// validHistoryOperations are the values accepted by --operation.
var validHistoryOperations = map[string]bool{"run": true, "test": true, "build": true}

// NewHistoryCommand builds `continuo node history <service> <schema> <table>`.
func NewHistoryCommand(factory RunClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "history <service> <schema> <table>",
		Short: "List the recent run history of one model node",
		Long: `List the recent run history of one model node.

Use when the user wants to see the last runs of a specific dbt model, including
whether each run succeeded, which attempt the task reached, why its latest
attempt failed (failure class, reason and error message), which image it ran,
its log and result locations, and the content hash of the code it executed.

Arguments:
  <service>  The owning service name.
  <schema>   The schema name.
  <table>    The table (model) name.

Flags:
  --operation  Filter history to one operation kind: run | test | build.
               Default "run" (dbt run/materialize rows). "test" returns dbt
               test rows; "build" returns dbt build rows. An unrecognized
               value is a usage error.

Output (stdout, JSON): up to 50 runs, newest first.
  {"runs":[{"run_id":string,"schedule_name":string,"kind":string,
   "run_status":string,"task_id":string,"task_status":string,
   "attempt":number,"image_tag":string,
   "created_at":string,"started_at":string,"completed_at":string,
   "error_message":string,"failure_class":string,"failure_reason":string,
   "log_s3_key":string,"run_results_uri":string,"operation":string,
   "content_hash":string,
   "wait_reason":string,"waiting_on_run_id":string,"waiting_since":string}]}
  attempt is the zero-based number of the task's current attempt (0 on the
  first try). run_status is the run's status: pending|running|cancelling|succeeded|failed|cancelled|skipped.
  task_status is one of: pending|dispatched|queued|running|stopping|succeeded|failed|cancelled|skipped.
  failure_class and failure_reason say why the latest attempt
  failed. run_status, started_at, completed_at, error_message,
  failure_class, failure_reason, log_s3_key, run_results_uri, and content_hash
  are omitted when empty. run_results_uri points at the structured result
  block the run's container printed — python-family nodes (python-node,
  python-csv, python-api) always emit one, dbt nodes never do. An unknown node returns {"runs":[]}, not an error.

content_hash is the node's content hash in the release the run ran on, as
the state service records it; it is omitted for runs that name no release.

wait_reason is node_lease while the task waits for another run to release its node:
another run's task is writing this node, and this run's pending task starts
once that write ends. waiting_on_run_id is the run writing the node (empty
for a moment when the node has just been freed and the task is about to be
granted it); waiting_since is when the wait began (RFC3339). The three are
omitted when the task waits for nothing besides its upstreams.

In --human mode, content_hash is rendered too: each line ends with an
EXECUTED_HASH column (the first 12 characters of content_hash, or "-" when
it is unavailable).

Errors:
  usage      (exit 2)  wrong number of arguments, or --operation is not run|test|build
  unavailable(exit 5)  the state service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo node history finance analytics orders --operation test",
		Annotations: map[string]string{
			"output_schema": `{"runs":[{"run_id":"string","schedule_name":"string","kind":"string","run_status":"string","task_id":"string","task_status":"string","attempt":"number","image_tag":"string","created_at":"string","started_at":"string","completed_at":"string","error_message":"string","failure_class":"string","failure_reason":"string","log_s3_key":"string","run_results_uri":"string","operation":"string","content_hash":"string","wait_reason":"string","waiting_on_run_id":"string","waiting_since":"string"}]}`,
			"exit_codes":    `[0,2,5,6]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 3 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("history requires exactly three arguments: <service> <schema> <table>"))
			}
			operation, _ := cmd.Flags().GetString("operation")
			if !validHistoryOperations[operation] {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("--operation must be one of run, test, build; got "+operation))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			operation, _ := cmd.Flags().GetString("operation")

			c, err := factory(ctx, cfg.StateEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.ListNodeRuns(ctx, args[0], args[1], args[2], operation, nodeHistoryLimit)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return humanHistory(stderr, resp.GetTasks())
			}
			return output.EmitSuccess(stdout, toHistoryPayload(resp.GetTasks()))
		},
	}
	cmd.Flags().String("operation", "run", "Filter history by operation: run | test | build (default \"run\")")
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

func toHistoryPayload(runs []*runv1.NodeTask) historyPayload {
	out := make([]nodeRun, 0, len(runs))
	for _, r := range runs {
		out = append(out, nodeRun{
			RunID:          r.GetRunId(),
			ScheduleName:   r.GetScheduleName(),
			Kind:           r.GetKind(),
			RunStatus:      r.GetRunStatus(),
			TaskID:         r.GetTaskId(),
			TaskStatus:     r.GetTaskStatus(),
			Attempt:        r.GetAttempt(),
			ImageTag:       r.GetImageTag(),
			CreatedAt:      r.GetCreatedAt(),
			StartedAt:      r.GetStartedAt(),
			CompletedAt:    r.GetCompletedAt(),
			ErrorMessage:   r.GetErrorMessage(),
			FailureClass:   r.GetFailureClass(),
			FailureReason:  r.GetFailureReason(),
			LogS3Key:       r.GetLogS3Key(),
			RunResultsURI:  r.GetRunResultsUri(),
			Operation:      r.GetOperation(),
			ContentHash:    r.GetContentHash(),
			WaitReason:     r.GetWaitReason(),
			WaitingOnRunID: r.GetWaitingOnRunId(),
			WaitingSince:   r.GetWaitingSince(),
		})
	}
	return historyPayload{Runs: out}
}

// executedHashDisplayLen is how many leading characters of content_hash
// humanHistory renders — enough to eyeball a match against another hash
// column without printing the full digest.
const executedHashDisplayLen = 12

// humanHistory writes a header then one line per run to stderr:
//
//	RUN_ID  OPERATION  STATUS  KIND  COMPLETED_AT  EXECUTED_HASH
//	<run_id>  <operation>  <task_status>  <kind>  <completed_at>  <executed_hash>
//
// executed_hash is the run's content_hash, truncated to executedHashDisplayLen
// characters, or "-" when the run names no release.
func humanHistory(stderr io.Writer, runs []*runv1.NodeTask) error {
	if _, err := fmt.Fprintf(stderr, "RUN_ID  OPERATION  STATUS  KIND  COMPLETED_AT  EXECUTED_HASH\n"); err != nil {
		return err
	}
	for _, r := range runs {
		if _, err := fmt.Fprintf(stderr, "%s  %s  %s  %s  %s  %s\n",
			r.GetRunId(), r.GetOperation(), r.GetTaskStatus(), r.GetKind(), r.GetCompletedAt(),
			shortExecutedHash(r.GetContentHash())); err != nil {
			return err
		}
	}
	return nil
}

// shortExecutedHash renders the leading executedHashDisplayLen characters of
// a content hash for human display, or "-" when the run names no release.
func shortExecutedHash(hash string) string {
	if hash == "" {
		return "-"
	}
	if len(hash) > executedHashDisplayLen {
		return hash[:executedHashDisplayLen]
	}
	return hash
}
