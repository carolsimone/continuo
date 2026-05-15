package handlers_test

import (
	"context"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeRunFinalizer struct {
	calls []struct {
		runID  string
		status string
	}
	err error
}

func (f *fakeRunFinalizer) FinalizeRun(_ context.Context, runID, status string) error {
	if f.err != nil {
		return f.err
	}
	f.calls = append(f.calls, struct {
		runID  string
		status string
	}{runID, status})
	return nil
}

var _ handlers.RunFinalizer = (*fakeRunFinalizer)(nil)

func TestRunFinalizedHandler_HappyPath(t *testing.T) {
	f := &fakeRunFinalizer{}
	h := handlers.NewRunFinalizedHandler(f, testLogger())
	err := h.Handle(context.Background(), domain.RunFinalized{ScheduleID: "sched-1", Status: "success"})
	require.NoError(t, err)
	require.Len(t, f.calls, 1)
	assert.Equal(t, "sched-1", f.calls[0].runID)
	assert.Equal(t, "success", f.calls[0].status)
}

func TestRunFinalizedHandler_FinalizerErrorPropagates(t *testing.T) {
	f := &fakeRunFinalizer{err: errors.New("neo4j down")}
	h := handlers.NewRunFinalizedHandler(f, testLogger())
	err := h.Handle(context.Background(), domain.RunFinalized{ScheduleID: "sched-1", Status: "success"})
	require.Error(t, err)
}
