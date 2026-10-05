package ports

import "github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"

// Observer is told about every dead letter the service stores, redrives or
// deletes past the replay horizon, for metrics.
type Observer interface {
	Recorded(dl deadletter.DeadLetter)
	Redriven(dl deadletter.DeadLetter)
	Expired(dl deadletter.DeadLetter)
}

// NopObserver discards every notification.
type NopObserver struct{}

func (NopObserver) Recorded(deadletter.DeadLetter) {}
func (NopObserver) Redriven(deadletter.DeadLetter) {}
func (NopObserver) Expired(deadletter.DeadLetter)  {}
