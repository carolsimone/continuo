package handlers

import (
	"fmt"
	"testing"

	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/runplan"
	"github.com/stretchr/testify/assert"
)

// A cron fire whose selection cannot run fails with the reason the refusal
// names: no node to run, or a node of an undeclared type.
func TestRefusalFailureReason(t *testing.T) {
	assert.Equal(t, run.RunFailureEmptySelection,
		refusalFailureReason(fmt.Errorf("%w: no release is live", runplan.ErrEmptySelection)))
	assert.Equal(t, run.RunFailureInvalidNodeType,
		refusalFailureReason(fmt.Errorf("%w: dbt-macro", runplan.ErrInvalidNodeType)))
}
