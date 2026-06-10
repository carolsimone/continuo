package schedule

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	statev1 "github.com/carolsimone/continuo/cli/proto/state/v1"
	"github.com/spf13/cobra"
)

// statusPageSize is the ListTasks page size used while paging through a run.
const statusPageSize = 200

type statusNode struct {
	ServiceName string `json:"service_name"`
	SchemaName  string `json:"schema_name"`
	TableName   string `json:"table_name"`
	Status      string `json:"status"`
}

type statusPayload struct {
	ScheduleName  string       `json:"schedule_name"`
	RunId         string       `json:"run_id"`
	IsRunning     bool         `json:"is_running"`
	LastRunStatus string       `json:"last_run_status"`
	Nodes         []statusNode `json:"nodes"`
}

// NewStatusCommand builds `continuo schedule status <schedule-name>`. It resolves
// the schedule name to its latest run via ListAllSchedules, then pages through
// ListTasks to collect every node's status for that run.
func NewStatusCommand(factory StateClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status <schedule-name>",
		Short: "Show the per-node status of a schedule's latest run",
		Long: `Show the per-node status of a schedule's latest run.

Use when the user asks where a schedule is, how far it has got, or which of its
nodes are running, failed, or succeeded right now.

Arguments:
  <schedule-name>  The schedule to inspect (exact match).

Output (stdout, JSON):
  {"schedule_name":string,"run_id":string,"is_running":bool,
   "last_run_status":string,
   "nodes":[{"service_name":string,"schema_name":string,
             "table_name":string,"status":string}]}
  status is one of: pending|running|succeeded|failed|cancelled|skipped.
  A schedule that has never run returns run_id:"" and nodes:[].

Errors:
  not_found  (exit 3)  no schedule with that name
  usage      (exit 2)  wrong number of arguments
  unavailable(exit 5)  the state service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo schedule status daily-revenue",
		Annotations: map[string]string{
			"output_schema": `{"schedule_name":"string","run_id":"string","is_running":"bool","last_run_status":"string","nodes":"array"}`,
			"exit_codes":    `[0,2,3,5,6]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("status requires exactly one argument: <schedule-name>"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			scheduleName := args[0]

			c, err := factory(cmd.Context(), cfg.StateEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer c.Close()

			// Each RPC gets its own --timeout deadline, matching the documented
			// per-call contract.
			listCtx, cancelList := context.WithTimeout(cmd.Context(), cfg.Timeout)
			schedules, err := c.ListAllSchedules(listCtx)
			cancelList()
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			var match *statev1.ScheduleSummary
			for _, s := range schedules.GetSchedules() {
				if s.GetScheduleName() == scheduleName {
					match = s
					break
				}
			}
			if match == nil {
				return emit(stdout, stderr, cfg.Human, output.CLIError{
					Code:    output.CodeNotFound,
					Message: "no schedule named '" + scheduleName + "'",
				})
			}

			payload := statusPayload{
				ScheduleName:  scheduleName,
				RunId:         match.GetLastRunId(),
				IsRunning:     match.GetIsRunning(),
				LastRunStatus: match.GetLastRunStatus(),
				Nodes:         []statusNode{},
			}

			// Never run: no run to page over.
			if match.GetLastRunId() == "" {
				return emitStatus(stdout, stderr, cfg.Human, payload)
			}

			// Page through every task row of the latest run.
			for offset := int32(0); ; offset += statusPageSize {
				pageCtx, cancelPage := context.WithTimeout(cmd.Context(), cfg.Timeout)
				resp, err := c.ListTasks(pageCtx, match.GetLastRunId(), statev1.TaskStatus_TASK_STATUS_UNSPECIFIED, statusPageSize, offset)
				cancelPage()
				if err != nil {
					return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
				}
				tasks := resp.GetTasks()
				for _, t := range tasks {
					payload.Nodes = append(payload.Nodes, statusNode{
						ServiceName: t.GetServiceName(),
						SchemaName:  t.GetSchemaName(),
						TableName:   t.GetTableName(),
						Status:      taskStatusString(t.GetStatus()),
					})
				}
				if len(tasks) == 0 || int32(len(payload.Nodes)) >= resp.GetTotalCount() {
					break
				}
			}

			return emitStatus(stdout, stderr, cfg.Human, payload)
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

// emitStatus writes the payload as JSON on stdout, or a human summary on stderr.
func emitStatus(stdout, stderr io.Writer, human bool, p statusPayload) error {
	if human {
		state := "idle"
		if p.IsRunning {
			state = "running"
		}
		if _, err := fmt.Fprintf(stderr, "%s (%s): %d nodes\n", p.ScheduleName, state, len(p.Nodes)); err != nil {
			return err
		}
		for _, n := range p.Nodes {
			if _, err := fmt.Fprintf(stderr, "  %s.%s.%s  %s\n", n.ServiceName, n.SchemaName, n.TableName, n.Status); err != nil {
				return err
			}
		}
		return nil
	}
	return output.EmitSuccess(stdout, p)
}

// taskStatusString maps the proto TaskStatus enum to its lowercase short form
// ("TASK_STATUS_SUCCEEDED" -> "succeeded"); UNSPECIFIED maps to "".
func taskStatusString(s statev1.TaskStatus) string {
	const prefix = "TASK_STATUS_"
	name := strings.TrimPrefix(s.String(), prefix)
	lower := strings.ToLower(name)
	if lower == "unspecified" {
		return ""
	}
	return lower
}
