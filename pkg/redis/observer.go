package redis

import (
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
)

// Observer receives what a consumer does with its messages, for metrics.
// Implementations must be safe for concurrent use.
type Observer interface {
	// Watch is called once, when Start begins consuming stream as group.
	Watch(stream, group string)
	// Handled records one handler invocation that took d and ended with
	// result: "ok", or the ErrorClass of the error it returned.
	Handled(stream, group, result string, d time.Duration)
	// DeadLettered records one message written to the dead-letter stream.
	DeadLettered(stream, group string, kind model.DeadLetterKind)
	// Paused records one pause after an infrastructure error.
	Paused(stream, group string)
}

type nopObserver struct{}

func (nopObserver) Watch(string, string)                              {}
func (nopObserver) Handled(string, string, string, time.Duration)     {}
func (nopObserver) DeadLettered(string, string, model.DeadLetterKind) {}
func (nopObserver) Paused(string, string)                             {}

// SetObserver reports this consumer's activity to o. Call it before Start.
func (c *StreamConsumer) SetObserver(o Observer) {
	if o != nil {
		c.observer = o
	}
}
