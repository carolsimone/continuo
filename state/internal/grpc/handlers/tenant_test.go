package handlers

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Every handler refuses a request whose context carries no tenant with
// Internal, before it reads or writes anything: the dependencies below fail
// the test when they are reached.
func TestHandlers_RefuseARequestWithoutATenant(t *testing.T) {
	logger := newTestLogger()
	noTasks := func(string) TaskReads { t.Fatal("a task read was made without a tenant"); return nil }
	noNodeRuns := func(string) postgres.NodeRunRepository {
		t.Fatal("a node-run read was made without a tenant")
		return nil
	}
	scheduler := NewSchedulerHandler(nil, nil, nil, nil, noopUoWFactory(), logger)
	tasks := NewTaskHandler(noTasks, logger)
	nodeRuns := NewNodeRunHandler(noNodeRuns, 0, logger)
	reads := NewRunReadHandler(nil, logger)
	attempts := NewTaskAttemptHandler(nil, logger)
	rerun := NewRerunHandler(nil, noopUoWFactory(), logger)
	rebase := NewRebaseHandler(nil, noopUoWFactory(), logger)
	preview := NewPreviewRebaseHandler(nil, logger)
	singleNode := NewSingleNodeRunHandler(nil, noopUoWFactory(), logger)
	id := uuid.NewString()

	calls := map[string]func(context.Context) error{
		"GetScheduler": func(ctx context.Context) error {
			_, err := scheduler.GetScheduler(ctx, &statev1.GetSchedulerRequest{ScheduleId: id})
			return err
		},
		"CancelScheduler": func(ctx context.Context) error {
			_, err := scheduler.CancelScheduler(ctx, &statev1.CancelSchedulerRequest{ScheduleId: id})
			return err
		},
		"CancelSchedule": func(ctx context.Context) error {
			_, err := scheduler.CancelSchedule(ctx, &statev1.CancelScheduleRequest{ScheduleName: "daily"})
			return err
		},
		"ActivateSchedule": func(ctx context.Context) error {
			_, err := scheduler.ActivateSchedule(ctx, &statev1.ActivateScheduleRequest{ScheduleName: "daily"})
			return err
		},
		"TriggerSchedule": func(ctx context.Context) error {
			_, err := scheduler.TriggerSchedule(ctx, &statev1.TriggerScheduleRequest{ScheduleName: "daily"})
			return err
		},
		"ListAllSchedules": func(ctx context.Context) error {
			_, err := scheduler.ListAllSchedules(ctx, &statev1.ListAllSchedulesRequest{})
			return err
		},
		"GetSchedulerInitStatus": func(ctx context.Context) error {
			_, err := scheduler.GetSchedulerInitStatus(ctx, &statev1.GetSchedulerInitStatusRequest{ScheduleId: id})
			return err
		},
		"GetTask": func(ctx context.Context) error {
			_, err := tasks.GetTask(ctx, &statev1.GetTaskRequest{TaskId: id})
			return err
		},
		"GetTaskByScheduleAndNode": func(ctx context.Context) error {
			_, err := tasks.GetTaskByScheduleAndNode(ctx, &statev1.GetTaskByScheduleAndNodeRequest{
				ScheduleId: id, ServiceName: "svc", SchemaName: "raw", TableName: "a",
			})
			return err
		},
		"ListTasks": func(ctx context.Context) error {
			_, err := tasks.ListTasks(ctx, &statev1.ListTasksRequest{ScheduleId: id})
			return err
		},
		"ListTaskAttempts": func(ctx context.Context) error {
			_, err := attempts.ListTaskAttempts(ctx, &statev1.ListTaskAttemptsRequest{RunId: id})
			return err
		},
		"GetRunGraph": func(ctx context.Context) error {
			_, err := reads.GetRunGraph(ctx, &statev1.GetRunGraphRequest{RunId: id})
			return err
		},
		"ListRuns": func(ctx context.Context) error {
			_, err := reads.ListRuns(ctx, &statev1.ListRunsRequest{ScheduleName: "daily"})
			return err
		},
		"ListActiveRunDrifts": func(ctx context.Context) error {
			_, err := reads.ListActiveRunDrifts(ctx, &statev1.ListActiveRunDriftsRequest{})
			return err
		},
		"TriggerRerun": func(ctx context.Context) error {
			_, err := rerun.TriggerRerun(ctx, &statev1.TriggerRerunRequest{SourceRunId: id})
			return err
		},
		"TriggerRebase": func(ctx context.Context) error {
			_, err := rebase.TriggerRebase(ctx, &statev1.TriggerRebaseRequest{SourceRunId: id})
			return err
		},
		"PreviewRebase": func(ctx context.Context) error {
			_, err := preview.PreviewRebase(ctx, &statev1.PreviewRebaseRequest{SourceRunId: id})
			return err
		},
		"TriggerSingleNodeRun": func(ctx context.Context) error {
			_, err := singleNode.TriggerSingleNodeRun(ctx, &statev1.TriggerSingleNodeRunRequest{
				ServiceName: "svc", SchemaName: "raw", TableName: "a", MetadataSource: "latest",
			})
			return err
		},
		"ListNodeRuns": func(ctx context.Context) error {
			_, err := nodeRuns.ListNodeRuns(ctx, &statev1.ListNodeRunsRequest{ServiceName: "svc", SchemaName: "raw", TableName: "a"})
			return err
		},
		"ListNodes": func(ctx context.Context) error {
			_, err := nodeRuns.ListNodes(ctx, &statev1.ListNodesRequest{})
			return err
		},
		"ListNodeNames": func(ctx context.Context) error {
			_, err := nodeRuns.ListNodeNames(ctx, &statev1.ListNodeNamesRequest{})
			return err
		},
		"ListNodeServices": func(ctx context.Context) error {
			_, err := nodeRuns.ListNodeServices(ctx, &statev1.ListNodeServicesRequest{})
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			err := call(context.Background())
			assert.Equal(t, codes.Internal, status.Code(err), "%v", err)
			assert.Equal(t, "the request carries no tenant", status.Convert(err).Message())
		})
	}
}
