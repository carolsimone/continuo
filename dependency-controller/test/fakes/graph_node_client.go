package fakes

import (
	"context"

	"github.com/carolsimone/continuo/dependency-controller/domain/model"
)

// FakeGraphNodeClient is a fake implementation of handlers.GraphNodeClient
type FakeGraphNodeClient struct {
	UpdateNodeStatusFunc        func(ctx context.Context, scheduleName, schema, tableName, status, runID string) error
	GetReadyDownstreamFunc      func(ctx context.Context, scheduleName, schema, tableName, runID string) ([]model.DownstreamNode, error)
	CheckScheduleCompletionFunc func(ctx context.Context, scheduleName, runID string) (bool, bool, error)
	FinalizeRunFunc             func(ctx context.Context, runID, terminalStatus string) error

	UpdateNodeStatusCalls        []UpdateNodeStatusCall
	GetReadyDownstreamCalls      []GetReadyDownstreamCall
	CheckScheduleCompletionCalls []CheckScheduleCompletionCall
	FinalizeRunCalls            []FinalizeRunCall
}

type UpdateNodeStatusCall struct {
	ScheduleName string
	Schema       string
	TableName    string
	Status       string
	RunID        string
}

type GetReadyDownstreamCall struct {
	ScheduleName string
	Schema       string
	TableName    string
	RunID        string
}

type CheckScheduleCompletionCall struct {
	ScheduleName string
	RunID        string
}

type FinalizeRunCall struct {
	RunID          string
	TerminalStatus string
}

func (f *FakeGraphNodeClient) UpdateNodeStatus(ctx context.Context, scheduleName, schema, tableName, status, runID string) error {
	f.UpdateNodeStatusCalls = append(f.UpdateNodeStatusCalls, UpdateNodeStatusCall{
		ScheduleName: scheduleName,
		Schema:       schema,
		TableName:    tableName,
		Status:       status,
		RunID:        runID,
	})
	if f.UpdateNodeStatusFunc != nil {
		return f.UpdateNodeStatusFunc(ctx, scheduleName, schema, tableName, status, runID)
	}
	return nil
}

func (f *FakeGraphNodeClient) GetReadyDownstream(ctx context.Context, scheduleName, schema, tableName, runID string) ([]model.DownstreamNode, error) {
	f.GetReadyDownstreamCalls = append(f.GetReadyDownstreamCalls, GetReadyDownstreamCall{
		ScheduleName: scheduleName,
		Schema:       schema,
		TableName:    tableName,
		RunID:        runID,
	})
	if f.GetReadyDownstreamFunc != nil {
		return f.GetReadyDownstreamFunc(ctx, scheduleName, schema, tableName, runID)
	}
	return nil, nil
}

func (f *FakeGraphNodeClient) CheckScheduleCompletion(ctx context.Context, scheduleName, runID string) (bool, bool, error) {
	f.CheckScheduleCompletionCalls = append(f.CheckScheduleCompletionCalls, CheckScheduleCompletionCall{
		ScheduleName: scheduleName,
		RunID:        runID,
	})
	if f.CheckScheduleCompletionFunc != nil {
		return f.CheckScheduleCompletionFunc(ctx, scheduleName, runID)
	}
	return false, false, nil
}

func (f *FakeGraphNodeClient) FinalizeRun(ctx context.Context, runID, terminalStatus string) error {
	f.FinalizeRunCalls = append(f.FinalizeRunCalls, FinalizeRunCall{
		RunID:          runID,
		TerminalStatus: terminalStatus,
	})
	if f.FinalizeRunFunc != nil {
		return f.FinalizeRunFunc(ctx, runID, terminalStatus)
	}
	return nil
}
