package redis

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// noTasks is a run.TaskCollection that holds no task. Only LoadAttemptState is
// answered; any other method panics on the nil embedded interface.
type noTasks struct{ run.TaskCollection }

func (noTasks) LoadAttemptState(context.Context, uuid.UUID) (run.TaskAttemptState, bool, error) {
	return run.TaskAttemptState{}, false, nil
}

// A record of a task no run holds, and a record without a Job name, are
// permanent, so the consumer dead-letters them at once.
func TestTaskExecutionRecordedBinding_UnmatchableRecordsArePermanent(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	values := func() map[string]interface{} {
		return map[string]interface{}{
			"execution_id": uuid.New().String(),
			"task_id":      uuid.New().String(),
			"job_name":     "job-r1",
		}
	}
	cases := map[string]struct {
		values map[string]interface{}
		is     error
	}{
		"a task no run holds": {values(), run.ErrTaskNotHeld},
		"no job name":         {func() map[string]interface{} { v := values(); delete(v, "job_name"); return v }(), nil},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			u, _ := newRecordingUnitOfWork(false)
			u.SetTaskCollection(noTasks{})
			bind := NewTaskExecutionRecordedBinding(func() uow.UnitOfWork { return u },
				handlers.NewTaskExecutionRecordedHandler(logger), logger)

			err := bind(context.Background(), goredis.XMessage{ID: "1-0", Values: tc.values})

			require.True(t, errors.Is(err, pkgevents.ErrPermanent), "got %v", err)
			if tc.is != nil {
				require.ErrorIs(t, err, tc.is)
			}
			require.Zero(t, u.CommitCalled)
		})
	}
}
