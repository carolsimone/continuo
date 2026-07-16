package model_test

import (
	"testing"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/stretchr/testify/assert"
)

// TestExecutionMode_Valid pins that only the two known paths are accepted. The
// empty mode is not one of them: every Deployment is created on a definite path.
func TestExecutionMode_Valid(t *testing.T) {
	assert.True(t, model.ExecutionModeJobs.Valid())
	assert.True(t, model.ExecutionModeWorkers.Valid())
	assert.False(t, model.ExecutionMode("").Valid())
	assert.False(t, model.ExecutionMode("lambdas").Valid())
}

func TestExecutionPath_Valid(t *testing.T) {
	assert.True(t, model.ExecutionPathNative.Valid())
	assert.True(t, model.ExecutionPathWrapperRequired.Valid())
	assert.True(t, model.ExecutionPathWrapperOpaque.Valid())
	assert.False(t, model.ExecutionPath("wrapper").Valid())
}

// TestExecutionPath_EmptyIsValid pins the unset path a Jobs-mode deployment
// carries: execution_path is resolved only when a worker claims the task, so
// the empty value must round-trip through the NULL column.
func TestExecutionPath_EmptyIsValid(t *testing.T) {
	assert.True(t, model.ExecutionPath("").Valid())
}

// TestExecutionModeAndPathWireValues pins the strings the executor_deployments
// CHECK constraints accept.
func TestExecutionModeAndPathWireValues(t *testing.T) {
	assert.Equal(t, "jobs", string(model.ExecutionModeJobs))
	assert.Equal(t, "workers", string(model.ExecutionModeWorkers))
	assert.Equal(t, "native", string(model.ExecutionPathNative))
	assert.Equal(t, "wrapper_required", string(model.ExecutionPathWrapperRequired))
	assert.Equal(t, "wrapper_opaque", string(model.ExecutionPathWrapperOpaque))
}
