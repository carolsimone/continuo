package handlers

import (
	"log/slog"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
)

// logDispatchTerminal logs, at WARN, the reason carried by a RunDispatchTerminal
// among evts: the event maps to no stream and no column, so this line is where
// the reason a dispatch finalized a run is visible. label names the inbound
// stream in the message.
func logDispatchTerminal(logger *slog.Logger, label string, scheduleID uuid.UUID, evts []run.DomainEvent) {
	for _, e := range evts {
		if t, ok := e.(run.RunDispatchTerminal); ok {
			logger.Warn(label+": dispatch finalized the run",
				"schedule_id", scheduleID,
				"schedule_name", t.Name,
				"reason", t.Reason,
			)
		}
	}
}
