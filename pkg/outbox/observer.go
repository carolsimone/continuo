package outbox

import "time"

// Observer receives what a processor does with its rows, for metrics.
// Implementations must be safe for concurrent use.
type Observer interface {
	// Published records rows published by one committed batch.
	Published(table string, rows int)
	// Failed records one failed publish: kind is "retry" for a row
	// rescheduled, otherwise the dead-letter kind of a row that went terminal.
	Failed(table, kind string)
}

type nopObserver struct{}

func (nopObserver) Published(string, int) {}
func (nopObserver) Failed(string, string) {}

// Backlog is the state of an outbox table at one moment.
type Backlog struct {
	// Open counts pending and scheduled rows.
	Open int
	// OldestOpenAge is the age of the oldest open row; 0 when there is none.
	OldestOpenAge time.Duration
	// DeadLettered counts rows parked as failed.
	DeadLettered int
}
