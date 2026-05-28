package handlers

import (
	"log/slog"

	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/carolsimone/continuo/release-controller/service/uow"
)

// Deps bundles the collaborators every handler shares.
// Constructed once in main.go; handlers are stateless functions over it.
type Deps struct {
	UoW       uow.UnitOfWork
	Clock     ports.Clock
	Telemetry ports.Telemetry
	Logger    *slog.Logger
}
