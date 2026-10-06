package run

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/google/uuid"
)

// createOnlyTasks is a TaskCollection that records BulkCreate; the dispatch
// paths exercised here call nothing else.
type createOnlyTasks struct {
	TaskCollection
	created []Task
}

func (c *createOnlyTasks) BulkCreate(_ context.Context, tasks []Task) error {
	c.created = append(c.created, tasks...)
	return nil
}

// A projected task that cannot be named as a Kubernetes Job cannot run, so the
// run finalizes failed with a recorded reason instead of returning an error
// that would roll back the dispatch and dead-letter the message.
func TestAcceptDispatch_UnnameableTaskFinalizesRunFailed(t *testing.T) {
	r, _, err := NewPendingRun("daily", KindCron, nil, identity.SystemUserID, nil, model.OperationRun, time.Now())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	r.ResetChanges()
	tasks := &createOnlyTasks{}
	noName := func(_, _, _, _ string) (string, error) {
		return "", errors.New("computed job_name is empty after sanitization")
	}

	evts, err := r.acceptDispatch(context.Background(), tasks, []DispatchedTask{
		{TaskID: uuid.New(), ServiceName: "-", SchemaName: "-", TableName: "-", Status: TaskStatusPending, MaxRetries: 3},
	}, time.Now(), noName)
	if err != nil {
		t.Fatalf("acceptDispatch returned %v; an unnameable task must finalize the run, not fail the message", err)
	}
	if r.Status() != SchedulerStatusFailed {
		t.Fatalf("status = %q, want failed", r.Status())
	}
	if len(tasks.created) != 0 {
		t.Fatalf("BulkCreate received %d tasks, want none", len(tasks.created))
	}
	if len(evts) != 2 {
		t.Fatalf("got %d events, want RunFinalized + RunDispatchTerminal", len(evts))
	}
	if fin, ok := evts[0].(RunFinalized); !ok || fin.Outcome != SchedulerStatusFailed {
		t.Fatalf("events[0] = %+v, want RunFinalized{Outcome: failed}", evts[0])
	}
	if term, ok := evts[1].(RunDispatchTerminal); !ok || term.Reason != DispatchReasonInvalidTask {
		t.Fatalf("events[1] = %+v, want RunDispatchTerminal{Reason: %q}", evts[1], DispatchReasonInvalidTask)
	}
	if !r.Changes().IsStatusDirty() || !r.Changes().IsCompletedDirty() {
		t.Fatal("the failed outcome must be marked for SaveRun")
	}
}
