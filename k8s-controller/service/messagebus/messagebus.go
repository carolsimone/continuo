package messagebus

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/k8s-controller/domain/command"
)

// CommandHandler handles a specific command
type CommandHandler func(ctx context.Context, cmd command.Command) error

// MessageBus coordinates command handling
type MessageBus struct {
	commandHandlers map[string]CommandHandler
	logger          *slog.Logger
}

// NewMessageBus creates a new MessageBus
func NewMessageBus(
	commandHandlers map[string]CommandHandler,
	logger *slog.Logger,
) *MessageBus {
	return &MessageBus{
		commandHandlers: commandHandlers,
		logger:          logger,
	}
}

// Handle processes a command
func (mb *MessageBus) Handle(ctx context.Context, cmd command.Command) error {
	commandType := fmt.Sprintf("%T", cmd)

	handler, exists := mb.commandHandlers[commandType]
	if !exists {
		mb.logger.Error("No handler registered for command", "command_type", commandType)
		return fmt.Errorf("no handler registered for command: %s", commandType)
	}

	mb.logger.Debug("Handling command", "command_type", commandType)

	if err := handler(ctx, cmd); err != nil {
		mb.logger.Error("Command handler failed",
			"command_type", commandType,
			"error", err,
		)
		return fmt.Errorf("command handler failed: %w", err)
	}

	mb.logger.Debug("Command handled successfully", "command_type", commandType)

	return nil
}
