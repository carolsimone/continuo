package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// A report for a run state never created, for a task its run does not hold,
// for an attempt state never dispatched, or a stopped report of an attempt
// state never asked to stop, is permanent, so the consumer dead-letters it at
// once; every other handler result keeps its class.
func TestPermanentWhenNeverValid(t *testing.T) {
	cases := map[string]struct {
		err       error
		permanent bool
	}{
		"a task the run does not hold":   {fmt.Errorf("record attempt report: %w", run.ErrTaskNotHeld), true},
		"an attempt state never issued":  {fmt.Errorf("record attempt report: %w", run.ErrAttemptNotIssued), true},
		"an attempt never asked to stop": {fmt.Errorf("record attempt report: %w", run.ErrStopNotRequested), true},
		"a run state never created":      {fmt.Errorf("load run: %w", postgres.ErrNotFound), true},
		"any other failure":              {errors.New("connection reset"), false},
		"no failure":                     {nil, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			handle := permanentWhenNeverValid(func(context.Context, uow.UnitOfWork, events.AttemptReported, uuid.UUID) error {
				return tc.err
			})
			err := handle(context.Background(), nil, events.AttemptReported{}, uuid.Nil)
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, tc.permanent, errors.Is(err, pkgevents.ErrPermanent))
		})
	}
}
