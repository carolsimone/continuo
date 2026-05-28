package handlers

import (
	"log/slog"

	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/carolsimone/continuo/release-controller/service/uow"
)

// Deps bundles the collaborators every handler shares.
// Constructed once in main.go; handlers are stateless functions over it.
// NewUoW is a factory called per-handler-invocation so that each request
// owns an independent UnitOfWork with isolated tx state. The underlying
// *sqlx.DB pool is goroutine-safe; UoW instances are not.
type Deps struct {
	NewUoW    func() uow.UnitOfWork
	Clock     ports.Clock
	Telemetry ports.Telemetry
	Logger    *slog.Logger
}
