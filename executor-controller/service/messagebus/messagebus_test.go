package messagebus

import (
	"log/slog"
	"testing"
)

func TestNewMessageBus_HasNoTransactionDependency(t *testing.T) {
	var _ func(map[string]CommandHandler, map[string][]EventHandler, *slog.Logger) *MessageBus = NewMessageBus
}
