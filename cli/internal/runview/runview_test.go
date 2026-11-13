package runview

import (
	"bytes"
	"context"
	"testing"
	"time"

	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubReader serves one page of tasks and one of attempts; totals are the
// total_count each page reports.
type stubReader struct {
	run           *runv1.Run
	tasks         []*runv1.Task
	tasksTotal    int32
	attempts      []*runv1.TaskAttempt
	attemptsTotal int32
}

func (s stubReader) GetRun(context.Context, string) (*runv1.RunResponse, error) {
	return &runv1.RunResponse{Run: s.run}, nil
}

func (s stubReader) ListTasks(context.Context, string, string, int32, int32) (*runv1.ListTasksResponse, error) {
	return &runv1.ListTasksResponse{Tasks: s.tasks, TotalCount: s.tasksTotal}, nil
}

func (s stubReader) ListRunAttempts(context.Context, string, string, int32, int32) (*runv1.ListRunAttemptsResponse, error) {
	return &runv1.ListRunAttemptsResponse{Attempts: s.attempts, TotalCount: s.attemptsTotal}, nil
}

// The latest attempt of a task is the one with the highest number, wherever
// the server lists it.
func TestLoad_TakesTheFailureOfTheHighestNumberedAttempt(t *testing.T) {
	r := stubReader{
		run:        &runv1.Run{RunId: "r", Status: "failed"},
		tasks:      []*runv1.Task{{TaskId: "t", ServiceName: "s", SchemaName: "x", TableName: "a", Status: "failed", Attempt: 2}},
		tasksTotal: 1, attemptsTotal: 3,
		attempts: []*runv1.TaskAttempt{
			{TaskId: "t", Attempt: 2, FailureClass: "timeout", FailureReason: "deadline_exceeded"},
			{TaskId: "t", Attempt: 0, FailureClass: "infra", FailureReason: "create_failed"},
			{TaskId: "t", Attempt: 1, FailureClass: "task", FailureReason: "dbt_error"},
		},
	}

	v, err := Load(context.Background(), r, time.Second, "r")

	require.NoError(t, err)
	require.Len(t, v.Tasks, 1)
	assert.Equal(t, "timeout", v.Tasks[0].FailureClass)
	assert.Equal(t, "deadline_exceeded", v.Tasks[0].FailureReason)
	assert.Equal(t, "s.x.a", v.Tasks[0].Node)
}

func TestEmpty_HasNoRunAndNoTasks(t *testing.T) {
	v := Empty()

	assert.Nil(t, v.Run)
	assert.NotNil(t, v.Tasks)
	assert.Empty(t, v.Tasks)
}

func TestWriteHuman_NamesTheRunAndEachTask(t *testing.T) {
	v := View{Run: &Run{RunID: "r1", ScheduleName: "daily", Status: "failed"}, Tasks: []Task{
		{Node: "s.x.a", Status: "failed", Attempt: 1, FailureClass: "task", FailureReason: "dbt_error"},
		{Node: "s.x.b", Status: "pending", WaitReason: "node_lease", WaitingOnRunID: "r0"},
	}}
	var buf bytes.Buffer

	require.NoError(t, v.WriteHuman(&buf))

	assert.Equal(t, "run r1  daily  failed  2 tasks\n  s.x.a  failed  attempt 1  task dbt_error\n  s.x.b  pending  attempt 0  waiting: node_lease on run r0\n", buf.String())
}

func TestWriteHuman_NamesAnOperationOtherThanRunAndThePlanReason(t *testing.T) {
	v := View{Run: &Run{RunID: "r1", ScheduleName: "daily", Status: "running"}, Tasks: []Task{
		{Node: "s.x.a", Status: "succeeded", Operation: "full_refresh", PlanReason: "columns changed"},
		{Node: "s.x.b", Status: "running", Operation: "run", PlanReason: "rebuild: dropped by the load of x.a"},
		{Node: "s.x.c", Status: "pending", Operation: "run"},
	}}
	var buf bytes.Buffer

	require.NoError(t, v.WriteHuman(&buf))

	assert.Equal(t, "run r1  daily  running  3 tasks\n"+
		"  s.x.a  succeeded  attempt 0  full_refresh  why: columns changed\n"+
		"  s.x.b  running  attempt 0  why: rebuild: dropped by the load of x.a\n"+
		"  s.x.c  pending  attempt 0\n", buf.String())
}

func TestLoad_CarriesEachTasksOperationAndPlanReason(t *testing.T) {
	r := stubReader{run: &runv1.Run{RunId: "r", Status: "running"}, tasksTotal: 2, tasks: []*runv1.Task{
		{TaskId: "t1", ServiceName: "service-1", SchemaName: "e2e_schema", TableName: "seed_table_1", Status: "succeeded", Operation: "full_refresh", PlanReason: "columns changed"},
		{TaskId: "t2", ServiceName: "service-1", SchemaName: "e2e_schema", TableName: "seed_table_1_view", Status: "running", Operation: "run", PlanReason: "rebuild: dropped by the load of e2e_schema.seed_table_1"}}}

	v, err := Load(context.Background(), r, time.Second, "r")

	require.NoError(t, err)
	assert.Equal(t, "full_refresh", v.Tasks[0].Operation)
	assert.Equal(t, "columns changed", v.Tasks[0].PlanReason)
	assert.Equal(t, "run", v.Tasks[1].Operation)
	assert.Equal(t, "rebuild: dropped by the load of e2e_schema.seed_table_1", v.Tasks[1].PlanReason)
}

func TestWriteHuman_ANeverRunScheduleHasNoRun(t *testing.T) {
	var buf bytes.Buffer

	require.NoError(t, Empty().WriteHuman(&buf))

	assert.Equal(t, "no run\n", buf.String())
}
