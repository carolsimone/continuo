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
	RunResultsURI   string `json:"run_results_uri,omitempty"`
	Operation       string `json:"operation"`
	// ContentHash is the code this run executed, joined client-side from the
	// orchestrator's run history by run id. Empty for runs that predate the
	// stamp, and for every run when the orchestrator join itself could not be
	// completed (see NewHistoryCommand's degrade behavior).
	ContentHash string `json:"content_hash,omitempty"`
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
func NewHistoryCommand(factory StateClientFactory, orchFactory OrchestratorClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "history <service> <schema> <table>",
		Short: "List the recent run history of one model node",
		Long: `List the recent run history of one model node.

Use when the user wants to see the last runs of a specific dbt model, including
whether each run succeeded, which image and manifest version it used, any
error message, and the exact code it executed.

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
   "terminal_status":string,"task_id":string,"task_status":string,
   "retry_count":number,"image_tag":string,"manifest_version":string,
   "created_at":string,"started_at":string,"completed_at":string,
   "error_message":string,"log_s3_key":string,"operation":string,
   "run_results_uri":string,"content_hash":string}]}
  terminal_status, started_at, completed_at, error_message, log_s3_key,
  run_results_uri, and content_hash are omitted when empty. run_results_uri
  points at the structured result block the run's container printed —
  python-model nodes always emit one, dbt nodes never do. An unknown node
  returns {"runs":[]}, not an error.

content_hash is the code this run executed, joined client-side from the
orchestrator's GetNodeRunHistory by run id, filtered server-side to the same
--operation so the enrichment call cannot be starved by newer executions of a
different operation; it is omitted for runs that predate the stamp. State
remains the primary source of truth for run history: if the join to the
orchestrator fails for any reason (the service is unreachable, the node has
no recorded version history, or any other error), history still returns
state's rows in full, every content_hash omitted rather than the command
failing.

In --human mode, the joined hash is rendered too: each line ends with an
EXECUTED_HASH column (the first 12 characters of content_hash, or "-" when
it is unavailable).

Errors:
  usage      (exit 2)  wrong number of arguments, or --operation is not run|test|build
  unavailable(exit 5)  the state service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo node history finance analytics orders --operation test",
		Annotations: map[string]string{
			"output_schema": `{"runs":"array"}`,
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

			contentHashByRunID := fetchContentHashByRunID(ctx, orchFactory, cfg.OrchestratorEndpoint, args[1], args[2], operation)

			if cfg.Human {
				return humanHistory(stderr, resp.GetRuns(), contentHashByRunID)
			}
			return output.EmitSuccess(stdout, toHistoryPayload(resp.GetRuns(), contentHashByRunID))
		},
	}
	cmd.Flags().String("operation", "run", "Filter history by operation: run | test | build (default \"run\")")
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

// fetchContentHashByRunID joins run history against the orchestrator's
// recorded code-version history by run id, filtered server-side to
// operation: without that filter, newer executions of a different operation
// could fill the orchestrator's limit and starve out the hashes for the rows
// state actually returned. It never fails the caller: state is the primary
// source of truth for node history, so any error dialing the orchestrator or
// calling GetNodeRunHistory (unreachable service, unknown node, or otherwise)
// produces an empty map rather than an error, leaving every content_hash
// omitted for this response.
func fetchContentHashByRunID(ctx context.Context, orchFactory OrchestratorClientFactory, orchestratorEndpoint, schema, table, operation string) map[string]string {
	empty := map[string]string{}

	c, err := orchFactory(ctx, orchestratorEndpoint)
	if err != nil {
		return empty
	}
	defer func() { _ = c.Close() }()

	uniqueID := schema + "." + table
	resp, err := c.GetNodeRunHistory(ctx, uniqueID, nodeHistoryLimit, operation)
	if err != nil {
		return empty
	}

	byRunID := make(map[string]string, len(resp.GetRuns()))
	for _, r := range resp.GetRuns() {
		if r.GetContentHash() != "" {
			byRunID[r.GetRunId()] = r.GetContentHash()
		}
	}
	return byRunID
}

func toHistoryPayload(runs []*statev1.NodeRun, contentHashByRunID map[string]string) historyPayload {
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
			RunResultsURI:   r.GetRunResultsUri(),
			Operation:       r.GetOperation(),
			ContentHash:     contentHashByRunID[r.GetRunId()],
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
// executed_hash is the orchestrator-joined content_hash (see
// fetchContentHashByRunID), truncated to executedHashDisplayLen characters,
// or "-" when it is unavailable for this run.
func humanHistory(stderr io.Writer, runs []*statev1.NodeRun, contentHashByRunID map[string]string) error {
	if _, err := fmt.Fprintf(stderr, "RUN_ID  OPERATION  STATUS  KIND  COMPLETED_AT  EXECUTED_HASH\n"); err != nil {
		return err
	}
	for _, r := range runs {
		if _, err := fmt.Fprintf(stderr, "%s  %s  %s  %s  %s  %s\n",
			r.GetRunId(), r.GetOperation(), r.GetTaskStatus(), r.GetKind(), r.GetCompletedAt(),
			shortExecutedHash(contentHashByRunID[r.GetRunId()])); err != nil {
			return err
		}
	}
	return nil
}

// shortExecutedHash renders the leading executedHashDisplayLen characters of
// a content hash for human display, or "-" when the run predates the stamp
// or the orchestrator join could not be completed.
func shortExecutedHash(hash string) string {
	if hash == "" {
		return "-"
	}
	if len(hash) > executedHashDisplayLen {
		return hash[:executedHashDisplayLen]
	}
	return hash
}
