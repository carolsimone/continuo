package release

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	releasev1 "github.com/carolsimone/continuo/cli/proto/release/v1"
	"github.com/spf13/cobra"
)

// noticeJSON is one seed reload notice on stdout.
type noticeJSON struct {
	Kind              string                 `json:"kind"`
	NodeID            string                 `json:"node_id"`
	Service           string                 `json:"service"`
	EngineFamily      string                 `json:"engine_family"`
	Change            string                 `json:"change"`
	Columns           columnsJSON            `json:"columns"`
	Load              loadJSON               `json:"load"`
	Rebuilds          []rebuildJSON          `json:"rebuilds"`
	RefreshOnSchedule []scheduledRefreshJSON `json:"refresh_on_schedule"`
	Warehouse         warehouseJSON          `json:"warehouse"`
	Prediction        string                 `json:"prediction"`
	StopReason        string                 `json:"stop_reason"`
	Unvalidated       bool                   `json:"unvalidated"`
}

type columnsJSON struct {
	Added   []string `json:"added"`
	Removed []string `json:"removed"`
	Retyped []string `json:"retyped"`
}

type loadJSON struct {
	Operation  string `json:"operation"`
	DropsTable bool   `json:"drops_table"`
	Reason     string `json:"reason"`
}

type rebuildJSON struct {
	NodeID       string `json:"node_id"`
	Materialized string `json:"materialized"`
	Operation    string `json:"operation"`
	Schedule     string `json:"schedule"`
}

type scheduledRefreshJSON struct {
	NodeID       string `json:"node_id"`
	Materialized string `json:"materialized"`
	Schedule     string `json:"schedule"`
}

type warehouseJSON struct {
	Checked         bool                `json:"checked"`
	Completeness    string              `json:"completeness"`
	OutsideContinuo []outsideObjectJSON `json:"outside_continuo"`
}

type outsideObjectJSON struct {
	Object string `json:"object"`
	Kind   string `json:"kind"`
	Owner  string `json:"owner"`
}

// strs returns s, never nil, so an empty list encodes as [] rather than null.
func strs(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func noticeToJSON(n *releasev1.ReleaseNotice) noticeJSON {
	out := noticeJSON{
		Kind: n.GetKind(), NodeID: n.GetNodeId(), Service: n.GetService(), EngineFamily: n.GetEngineFamily(), Change: n.GetChange(),
		Columns: columnsJSON{
			Added: strs(n.GetColumns().GetAdded()), Removed: strs(n.GetColumns().GetRemoved()), Retyped: strs(n.GetColumns().GetRetyped()),
		},
		Load: loadJSON{
			Operation: n.GetLoad().GetOperation(), DropsTable: n.GetLoad().GetDropsTable(), Reason: n.GetLoad().GetReason(),
		},
		Rebuilds:          make([]rebuildJSON, 0, len(n.GetRebuilds())),
		RefreshOnSchedule: make([]scheduledRefreshJSON, 0, len(n.GetRefreshOnSchedule())),
		Warehouse: warehouseJSON{
			Checked: n.GetWarehouse().GetChecked(), Completeness: n.GetWarehouse().GetCompleteness(),
			OutsideContinuo: make([]outsideObjectJSON, 0, len(n.GetWarehouse().GetOutsideContinuo())),
		},
		Prediction: n.GetPrediction(), StopReason: n.GetStopReason(), Unvalidated: n.GetUnvalidated(),
	}
	for _, r := range n.GetRebuilds() {
		out.Rebuilds = append(out.Rebuilds, rebuildJSON{NodeID: r.GetNodeId(), Materialized: r.GetMaterialized(), Operation: r.GetOperation(), Schedule: r.GetSchedule()})
	}
	for _, r := range n.GetRefreshOnSchedule() {
		out.RefreshOnSchedule = append(out.RefreshOnSchedule, scheduledRefreshJSON{NodeID: r.GetNodeId(), Materialized: r.GetMaterialized(), Schedule: r.GetSchedule()})
	}
	for _, o := range n.GetWarehouse().GetOutsideContinuo() {
		out.Warehouse.OutsideContinuo = append(out.Warehouse.OutsideContinuo, outsideObjectJSON{Object: o.GetObject(), Kind: o.GetKind(), Owner: o.GetOwner()})
	}
	return out
}

// humanNotice renders one notice as a single stderr line.
func humanNotice(n *releasev1.ReleaseNotice) string {
	line := fmt.Sprintf("%s  %s  load %s  prediction %s", n.GetNodeId(), n.GetChange(), n.GetLoad().GetOperation(), n.GetPrediction())
	if r := n.GetStopReason(); r != "" {
		line += " (" + r + ")"
	}
	return line
}

// NewNoticesCommand builds `continuo release notices <release-id>`.
func NewNoticesCommand(factory ClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "notices <release-id>",
		Short: "Show what a candidate release changes in its seeds' loads",
		Long: `Show the seed reload notices of one candidate release: for each seed whose load the
release changes, what the load does, which views and materialized views it
rebuilds, which refresh on their schedule, what the warehouse holds that
continuo does not own, and whether the load is predicted to go ahead or stop.

Use when the user asks what a release does to a seed table, whether a release
will reload a seed, which views a seed reload rebuilds, or why a release's seed
stopped. A notice predicts and the run's preflight decides: the prediction is
made when the release is built, and the preflight of the run that loads the
seed settles what actually happens.

Arguments:
  <release-id>  The candidate release id, e.g. rel-a1b2c3d-201.

Output (stdout, JSON):
  {"release_id":string,"status":string,"notices":[{"kind":string,"node_id":string,
   "service":string,"engine_family":string,"change":string,
   "columns":{"added":[string],"removed":[string],"retyped":[string]},
   "load":{"operation":string,"drops_table":boolean,"reason":string},
   "rebuilds":[{"node_id":string,"materialized":string,"operation":string,"schedule":string}],
   "refresh_on_schedule":[{"node_id":string,"materialized":string,"schedule":string}],
   "warehouse":{"checked":boolean,"completeness":string,
    "outside_continuo":[{"object":string,"kind":string,"owner":string}]},
   "prediction":string,"stop_reason":string,"unvalidated":boolean}]}
  notices is [] for a release that changes no seed's load, in node id order.

Errors:
  usage      (exit 2)  missing, blank or extra release id
  not_found  (exit 3)  no candidate release has this id (a verification run's id is not one)
  unavailable(exit 5)  release-controller is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo release notices rel-a1b2c3d-201",
		Annotations: map[string]string{
			"output_schema": `{"release_id":"string","status":"string","notices":"array"}`,
			"exit_codes":    `[0,2,3,5,6]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 || strings.TrimSpace(args[0]) == "" {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("notices requires exactly one non-empty argument: <release-id>"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.ReleaseControllerEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.GetReleaseNotices(ctx, args[0])
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				if err := output.HumanSuccess(stderr, fmt.Sprintf("%s  %s  %d notices", resp.GetReleaseId(), resp.GetStatus(), len(resp.GetNotices()))); err != nil {
					return err
				}
				for _, n := range resp.GetNotices() {
					if err := output.HumanSuccess(stderr, "  "+humanNotice(n)); err != nil {
						return err
					}
				}
				return nil
			}
			notices := make([]noticeJSON, 0, len(resp.GetNotices()))
			for _, n := range resp.GetNotices() {
				notices = append(notices, noticeToJSON(n))
			}
			return output.EmitSuccess(stdout, map[string]any{
				"release_id": resp.GetReleaseId(),
				"status":     resp.GetStatus(),
				"notices":    notices,
			})
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}
