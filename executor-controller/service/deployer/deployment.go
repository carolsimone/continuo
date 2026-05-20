package deployer

import (
	"time"

	"github.com/google/uuid"
)

// Deployment is one row of executor_deployments.
type Deployment struct {
	ID                  uuid.UUID  `db:"id"`
	MessageProcessingID *uuid.UUID `db:"message_processing_id"`
	TaskID              uuid.UUID  `db:"task_id"`
	ScheduleID          uuid.UUID  `db:"schedule_id"`
	JobParams           []byte     `db:"job_params"`
	Status              string     `db:"status"`
	RetryCount          int        `db:"retry_count"`
	MaxRetries          int        `db:"max_retries"`
	NextAttemptAt       time.Time  `db:"next_attempt_at"`
	CreatedAt           time.Time  `db:"created_at"`
	DeployedAt          *time.Time `db:"deployed_at"`
	ErrorMessage        *string    `db:"error_message"`
}
