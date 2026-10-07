package model_test

import (
	"testing"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/stretchr/testify/assert"
)

func TestStatusInFlight(t *testing.T) {
	assert.ElementsMatch(t,
		[]model.Status{model.StatusReserved, model.StatusStarting, model.StatusRunning},
		model.InFlightStatuses())
	for _, s := range model.AllStatuses() {
		want := s == model.StatusReserved || s == model.StatusStarting || s == model.StatusRunning
		assert.Equal(t, want, s.InFlight(), s)
	}
}

func TestAllStatuses(t *testing.T) {
	assert.ElementsMatch(t, []model.Status{
		model.StatusPending, model.StatusBlocked, model.StatusReserved, model.StatusStarting,
		model.StatusRunning, model.StatusDone, model.StatusFailed, model.StatusSkipped,
	}, model.AllStatuses())
}

// TestTransitions walks every (from, to) pair: exactly these moves are legal.
// Recording an outcome on a done row is not a move (the status stays done).
func TestTransitions(t *testing.T) {
	legal := map[[2]model.Status]bool{
		{model.StatusPending, model.StatusReserved}:  true, // claim
		{model.StatusBlocked, model.StatusPending}:   true, // upstreams succeeded
		{model.StatusBlocked, model.StatusSkipped}:   true, // an upstream failed
		{model.StatusReserved, model.StatusStarting}: true, // Job created
		{model.StatusReserved, model.StatusPending}:  true, // transient failure, or stale reservation
		{model.StatusReserved, model.StatusFailed}:   true, // could not be deployed
		{model.StatusStarting, model.StatusRunning}:  true, // first status check: unfinished
		{model.StatusStarting, model.StatusDone}:     true, // finished before a check saw it running
		{model.StatusRunning, model.StatusDone}:      true, // finished
	}
	for _, from := range model.AllStatuses() {
		for _, to := range model.AllStatuses() {
			assert.Equal(t, legal[[2]model.Status{from, to}], model.CanMove(from, to), "%s → %s", from, to)
		}
	}
}
