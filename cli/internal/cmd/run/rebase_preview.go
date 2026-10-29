package run

import (
	"context"
	"fmt"
	"io"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/carolsimone/continuo/cli/internal/runview"
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/spf13/cobra"
)

// rebasePreview is the JSON form of what a rebase would run.
type rebasePreview struct {
	ReleaseID       string        `json:"release_id"`
	PromotionSeq    int64         `json:"promotion_seq"`
	TotalNodes      int32         `json:"total_nodes"`
	WillRun         int32         `json:"will_run"`
	FailedOrSkipped int32         `json:"failed_or_skipped"`
	NewNodes        int32         `json:"new_nodes"`
	ChangedUpstream int32         `json:"changed_upstream"`
	Downstream      int32         `json:"downstream"`
	Inherited       int32         `json:"inherited"`
	ChangedNodes    []changedNode `json:"changed_nodes"`
}

// changedNode is a changed node upstream of what a rebase re-executes.
type changedNode struct {
	ServiceName        string  `json:"service_name"`
	SchemaName         string  `json:"schema_name"`
	TableName          string  `json:"table_name"`
	Downstream         int32   `json:"downstream"`
	LastSuccessSeconds float64 `json:"last_success_seconds"`
}

func toRebasePreview(r *runv1.PreviewRebaseResponse) rebasePreview {
	p := rebasePreview{
		ReleaseID:       r.GetReleaseId(),
		PromotionSeq:    r.GetPromotionSeq(),
		TotalNodes:      r.GetTotalNodes(),
		WillRun:         r.GetWillRun(),
		FailedOrSkipped: r.GetFailedOrSkipped(),
		NewNodes:        r.GetNewNodes(),
		ChangedUpstream: r.GetChangedUpstream(),
		Downstream:      r.GetDownstream(),
		Inherited:       r.GetInherited(),
		ChangedNodes:    make([]changedNode, 0, len(r.GetChangedNodes())),
	}
	for _, n := range r.GetChangedNodes() {
		p.ChangedNodes = append(p.ChangedNodes, changedNode{
			ServiceName:        n.GetServiceName(),
			SchemaName:         n.GetSchemaName(),
			TableName:          n.GetTableName(),
			Downstream:         n.GetDownstream(),
			LastSuccessSeconds: n.GetLastSuccessSeconds(),
		})
	}
	return p
}

func (p rebasePreview) writeHuman(w io.Writer) error {
	if _, err := fmt.Fprintf(w, "live release %s (promotion %d): %d of %d nodes run (%d failed or skipped, %d new, %d changed upstream, %d downstream), %d inherited\n",
		p.ReleaseID, p.PromotionSeq, p.WillRun, p.TotalNodes, p.FailedOrSkipped, p.NewNodes, p.ChangedUpstream, p.Downstream, p.Inherited); err != nil {
		return err
	}
	for _, n := range p.ChangedNodes {
		if _, err := fmt.Fprintf(w, "  changed upstream: %s  feeds %d, last success %.1fs\n",
			runview.Node(n.ServiceName, n.SchemaName, n.TableName), n.Downstream, n.LastSuccessSeconds); err != nil {
			return err
		}
	}
	return nil
}

// NewRebasePreviewCommand builds
// `continuo run rebase-preview <run-id> [with-changed-upstream]`. It reads what
// a rebase of the run would re-execute on the live release and creates no run.
func NewRebasePreviewCommand(factory RunClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rebase-preview <run-id> [with-changed-upstream]",
		Short: "Preview what rebasing a failed or cancelled run on the live release would re-execute",
		Long: `Preview what rebasing a failed or cancelled run on the live release would re-execute.

Use when the user asks to restart a failed or cancelled run from its failure on
the newest code, or asks what such a restart would cost. It creates no run and is
the first step of a rebase: show the user the counts, ask for approval, then run
"run rebase" with the release_id this command printed.

A rebase re-executes the source run's tasks that did not succeed, the live
schedule's nodes the source never ran and everything below them, and with
with-changed-upstream also every changed node upstream of those. Every other
success of the source is inherited.

Arguments:
  <run-id>                The UUID of the failed or cancelled run to rebase from.
  [with-changed-upstream] Optional: "true" or "false" (default false). With true the
                          preview counts the changed upstream nodes as re-executed.
                          Any other value is refused.

Output (stdout, JSON):
  {"release_id":string,"promotion_seq":number,"total_nodes":number,"will_run":number,
   "failed_or_skipped":number,"new_nodes":number,"changed_upstream":number,
   "downstream":number,"inherited":number,
   "changed_nodes":[{"service_name":string,"schema_name":string,"table_name":string,
                     "downstream":number,"last_success_seconds":number}]}
  release_id is the live release the preview compared; pass it to "run rebase".
  total_nodes is re-executed plus inherited; will_run is the sum of
  failed_or_skipped, new_nodes, changed_upstream and downstream. changed_upstream
  is 0 without with-changed-upstream. changed_nodes lists every changed upstream
  node the option would add, whether or not it is set, ordered by service, schema
  and table; downstream counts the nodes below it, and last_success_seconds is the
  execution time of its latest successful task (0 when it has none).
  Maintenance mode does not refuse a preview.

Errors:
  usage      (exit 2)  wrong number of arguments, a blank run id, [with-changed-upstream] not true or false, or the run id is not a UUID
  not_found  (exit 3)  no run with that id
  conflict   (exit 4)  the run is not failed or cancelled, has no task that did not succeed, names no release, is a test run, or leaves nothing to re-execute
  unavailable(exit 5)  the run-controller service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo run rebase-preview 0b6f4a52-3c1d-4d7e-9a0b-1f2e3d4c5b6a true",
		Annotations: map[string]string{
			"output_schema": `{"release_id":"string","promotion_seq":"number","total_nodes":"number","will_run":"number","failed_or_skipped":"number","new_nodes":"number","changed_upstream":"number","downstream":"number","inherited":"number","changed_nodes":[{"service_name":"string","schema_name":"string","table_name":"string","downstream":"number","last_success_seconds":"number"}]}`,
			"exit_codes":    `[0,2,3,4,5,6]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 || len(args) > 2 {
				return usageFailure(cmd, stdout, stderr, "rebase-preview requires <run-id> and an optional [with-changed-upstream]")
			}
			if args[0] == "" {
				return usageFailure(cmd, stdout, stderr, "rebase-preview requires a non-empty <run-id>")
			}
			if _, err := parseChangedUpstream(args, 1); err != nil {
				return usageFailure(cmd, stdout, stderr, "%v", err)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			withChangedUpstream, _ := parseChangedUpstream(args, 1)
			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.RunControllerEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.PreviewRebase(ctx, args[0], withChangedUpstream)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			preview := toRebasePreview(resp)
			if cfg.Human {
				return preview.writeHuman(stderr)
			}
			return output.EmitSuccess(stdout, preview)
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}
