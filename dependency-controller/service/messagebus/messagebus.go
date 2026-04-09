package messagebus

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/dependency-controller/domain/command"
	"github.com/carolsimone/continuo/dependency-controller/domain/event"
	"github.com/carolsimone/continuo/dependency-controller/service/uow"
)

// CommandHandler handles a specific command with a message ID
type CommandHandler func(ctx context.Context, cmd command.Command, messageID string) error

// EventHandler handles a specific event
type EventHandler func(ctx context.Context, evt event.Event) error

// MessageBus coordinates command and event handling
type MessageBus struct {
	uow             uow.UnitOfWork
	commandHandlers map[string]CommandHandler
	eventHandlers   map[string][]EventHandler
	logger          *slog.Logger
}

// NewMessageBus creates a new MessageBus
func NewMessageBus(
	uow uow.UnitOfWork,
	commandHandlers map[string]CommandHandler,
	eventHandlers map[string][]EventHandler,
	logger *slog.Logger,
) *MessageBus {
	return &MessageBus{
		uow:             uow,
		commandHandlers: commandHandlers,
		eventHandlers:   eventHandlers,
		logger:          logger,
	}
}

// Handle process a command (backward compatibility, uses test message ID)
func (mb *MessageBus) Handle(ctx context.Context, cmd command.Command) error {
	return mb.HandleWithMessageID(ctx, cmd, "test-message-id")
}

// HandleWithMessageID processes a command with a specific message ID
func (mb *MessageBus) HandleWithMessageID(ctx context.Context, cmd command.Command, messageID string) error {
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

// PublishEvent publishes an event to all registered handlers
func (mb *MessageBus) PublishEvent(ctx context.Context, evt event.Event) error {
	eventType := fmt.Sprintf("%T", evt)

	handlers, exists := mb.eventHandlers[eventType]
	if !exists || len(handlers) == 0 {
		mb.logger.Debug("No handlers registered for event", "event_type", eventType)
		return nil
	}

	mb.logger.Debug("Publishing event", "event_type", eventType, "handler_count", len(handlers))

	for i, handler := range handlers {
		if err := handler(ctx, evt); err != nil {
			mb.logger.Error("Event handler failed",
				"event_type", eventType,
				"handler_index", i,
				"error", err,
			)
			return fmt.Errorf("event handler failed: %w", err)
		}
	}

	mb.logger.Debug("Event published successfully", "event_type", eventType)

	return nil
}
