package workerapi

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	"github.com/carolsimone/continuo/executor-controller/service/ports"
)

// InitializationReport is a worker's account of hydrating its runtime artifact
// at startup. A worker that cannot hydrate reports why and stays unready rather
// than crash-looping, so the pool's failure is visible instead of noisy.
type InitializationReport struct {
	PoolKey string
	OK      bool
	// ErrorCode names the failure class; Message describes the instance. Both
	// are empty on a successful report.
	ErrorCode string
	Message   string
	// HydrationSeconds is how long the worker took to load its artifact. It is
	// observed rather than stored: it describes one pod's startup, not the
	// pool's state.
	HydrationSeconds float64
}

// Pools records what workers report about their own pool.
type Pools struct {
	pools  repository.WorkerPoolRepository
	clock  ports.Clock
	logger *slog.Logger
}

// NewPools constructs the pool-reporting application service.
func NewPools(pools repository.WorkerPoolRepository, clock ports.Clock, logger *slog.Logger) *Pools {
	return &Pools{pools: pools, clock: clock, logger: logger}
}

// RecordInitialization marks a pool ready or not ready from one worker's
// startup report. A failure stops the pool being handed work its workers cannot
// run; a success clears an earlier failure, so a pool recovers on the next
// worker that hydrates cleanly rather than waiting for an operator.
func (p *Pools) RecordInitialization(ctx context.Context, report InitializationReport) error {
	pool, err := p.pools.Get(ctx, report.PoolKey)
	if err != nil {
		return fmt.Errorf("load worker pool: %w", err)
	}
	if pool == nil {
		return fmt.Errorf("worker pool %s is not registered", report.PoolKey)
	}

	now := p.clock.Now()
	if report.OK {
		pool.ClearInitializationError(now)
		p.logger.Info("worker pool hydrated its runtime artifact",
			"pool_key", pool.PoolKey, "service_name", pool.ServiceName,
			"hydration_seconds", report.HydrationSeconds)
	} else {
		pool.RecordInitializationFailure(report.ErrorCode, report.Message, now)
		p.logger.Error("worker pool cannot hydrate its runtime artifact — it will be handed no work",
			"pool_key", pool.PoolKey, "service_name", pool.ServiceName,
			"error_code", report.ErrorCode, "reason", report.Message)
	}
	return p.pools.Save(ctx, *pool)
}
