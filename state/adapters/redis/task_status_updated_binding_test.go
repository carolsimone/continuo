package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// A status for a task its run does not hold is permanent, so the consumer
// dead-letters it at once; every other handler result keeps its class.
func TestPermanentWhenTaskNotHeld(t *testing.T) {
	cases := map[string]struct {
		err       error
		permanent bool
	}{
		"a task the run does not hold": {fmt.Errorf("record task status: %w", run.ErrTaskNotHeld), true},
		"any other failure":            {errors.New("connection reset"), false},
		"no failure":                   {nil, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			handle := permanentWhenTaskNotHeld(func(context.Context, uow.UnitOfWork, events.TaskStatusUpdated, uuid.UUID) error {
				return tc.err
			})
			err := handle(context.Background(), nil, events.TaskStatusUpdated{}, uuid.Nil)
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, tc.permanent, errors.Is(err, pkgevents.ErrPermanent))
		})
	}
}
