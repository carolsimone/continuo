package command

import "github.com/google/uuid"

// Command is a marker interface for all commands
type Command interface {
	isCommand()
}

// InitializeScheduler represents a command to initialize a schedule
type InitializeScheduler struct {
	RunnerID     uuid.UUID // Same as schedule_id from Redis event
	ScheduleName string
}

func (InitializeScheduler) isCommand() {}
