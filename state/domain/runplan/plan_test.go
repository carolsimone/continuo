package runplan_test

import (
	"testing"

	"github.com/carolsimone/continuo/state/domain/runplan"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestPlan_HasPending(t *testing.T) {
	inherited := runplan.PlannedTask{Key: "stg.a", Inherited: true, InheritedFrom: uuid.New()}
	assert.False(t, runplan.Plan{}.HasPending(), "an empty plan")
	assert.False(t, runplan.Plan{Tasks: []runplan.PlannedTask{inherited}}.HasPending(), "every task inherited")
	assert.True(t, runplan.Plan{Tasks: []runplan.PlannedTask{inherited, {Key: "stg.b"}}}.HasPending(), "one task pending")
}
