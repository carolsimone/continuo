package postgres

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRowFromEvent verifies that rowFromEvent maps every field of
// TaskExecutionRecorded to the correct column on the storage row, and that
// ExecutorID is always nil (the event carries no executor id).
func TestRowFromEvent(t *testing.T) {
	t.Run("all optional fields populated", func(t *testing.T) {
		executionID := uuid.New()
		taskID := uuid.New()
		jobName := "k8s-job-abc"
		started := time.Now().Add(-2 * time.Minute)
		completed := time.Now().Add(-1 * time.Minute)
		execTime := 60.5
		errMsg := "something went wrong"
		s3Key := "logs/run-abc/task.log"

		evt := events.TaskExecutionRecorded{
			ExecutionID:          executionID,
			TaskID:               taskID,
			JobName:              &jobName,
			StartedAt:            &started,
			CompletedAt:          &completed,
			ExecutionTimeSeconds: &execTime,
			ErrorMessage:         &errMsg,
			LogS3Key:             &s3Key,
		}

		row := rowFromEvent(evt)

		require.NotNil(t, row)
		assert.Equal(t, executionID, row.ID)
		assert.Equal(t, taskID, row.TaskID)
		assert.Equal(t, &started, row.StartedAt)
		assert.Equal(t, &completed, row.CompletedAt)
		assert.Equal(t, &execTime, row.ExecutionTimeSeconds)
		assert.Equal(t, &jobName, row.K8sJobName)
		assert.Equal(t, &errMsg, row.ErrorMessage)
		assert.Equal(t, &s3Key, row.LogS3Key)
		// ExecutorID must always be nil: the event carries no executor id.
		assert.Nil(t, row.ExecutorID)
		// CreatedAt is set to time.Now() inside rowFromEvent.
		assert.False(t, row.CreatedAt.IsZero())
	})

	t.Run("all optional fields nil", func(t *testing.T) {
		executionID := uuid.New()
		taskID := uuid.New()

		evt := events.TaskExecutionRecorded{
			ExecutionID:          executionID,
			TaskID:               taskID,
			JobName:              nil,
			StartedAt:            nil,
			CompletedAt:          nil,
			ExecutionTimeSeconds: nil,
			ErrorMessage:         nil,
			LogS3Key:             nil,
		}

		row := rowFromEvent(evt)

		require.NotNil(t, row)
		assert.Equal(t, executionID, row.ID)
		assert.Equal(t, taskID, row.TaskID)
		assert.Nil(t, row.StartedAt)
		assert.Nil(t, row.CompletedAt)
		assert.Nil(t, row.ExecutionTimeSeconds)
		assert.Nil(t, row.K8sJobName)
		assert.Nil(t, row.ErrorMessage)
		assert.Nil(t, row.LogS3Key)
		// ExecutorID must always be nil.
		assert.Nil(t, row.ExecutorID)
		assert.False(t, row.CreatedAt.IsZero())
	})
}
