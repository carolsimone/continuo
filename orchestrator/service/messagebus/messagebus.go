package messagebus

import (
	"context"
	"fmt"
	"log/slog"
)

// CommandHandler handles a specific command with a message ID
type CommandHandler func(ctx context.Context, cmd interface{}, messageID string) error

// MessageBus coordinates command handling
type MessageBus struct {
	commandHandlers map[string]CommandHandler
	logger          *slog.Logger
}

// NewMessageBus creates a new MessageBus
func NewMessageBus(commandHandlers map[string]CommandHandler, logger *slog.Logger) *MessageBus {
	return &MessageBus{
		commandHandlers: commandHandlers,
		logger:          logger,
	}
}

// HandleWithMessageID processes a command with a specific message ID
func (mb *MessageBus) HandleWithMessageID(ctx context.Context, cmd interface{}, messageID string) error {
	commandType := fmt.Sprintf("%T", cmd)

	handler, exists := mb.commandHandlers[commandType]
	if !exists {
		mb.logger.Error("No handler registered for command", "command_type", commandType)
		return fmt.Errorf("no handler registered for command: %s", commandType)
	}

	mb.logger.Debug("Handling command", "command_type", commandType, "message_id", messageID)

	if err := handler(ctx, cmd, messageID); err != nil {
		mb.logger.Error("Command handler failed",
			"command_type", commandType,
			"message_id", messageID,
			"error", err,
		)
		return fmt.Errorf("command handler failed: %w", err)
	}

	mb.logger.Debug("Command handled successfully", "command_type", commandType, "message_id", messageID)

	return nil
}
