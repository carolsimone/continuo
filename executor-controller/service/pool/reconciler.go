// Package pool reconciles the executor's worker pools: it registers the pools
// the waiting work implies, shares the executor's capacity between them, and
// tells the runtime how many pods each should run.
//
// Nothing declares a pool. A task routed to workers names the pool it needs by
// key, and that is the whole source: pools follow the work, appear when
// something needs them, and retire when nothing does.
package pool

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	"github.com/carolsimone/continuo/executor-controller/domain/workerpool"
	"github.com/carolsimone/continuo/executor-controller/service/ports"
	"github.com/carolsimone/continuo/executor-controller/service/routing"
	"github.com/carolsimone/continuo/executor-controller/service/workerapi"
)

// ControllerContextFunc resolves the canonical parse context for a service: the
// controller's description of the conditions a dbt parse would resolve under.
// Every worker in a pool is given it, and rejects an artifact produced under a
// different one.
type ControllerContextFunc func(serviceName string) (string, error)

// CredentialFunc mints a pool credential. It is a seam for tests; production
// wiring leaves it unset and gets workerapi.NewCredential.
type CredentialFunc func() (string, error)

// Deps are the reconciler's collaborators.
type Deps struct {
	Pools             repository.WorkerPoolRepository
	Deployments       repository.DeploymentRepository
	Runtime           ports.WorkerPoolRuntime
	Policy            routing.Policy
	ControllerContext ControllerContextFunc
	Clock             ports.Clock
	Logger            *slog.Logger
	// Credential mints a pool credential. Optional; defaults to
	// workerapi.NewCredential.
	Credential CredentialFunc
}

// Config are the reconciler's tunables.
type Config struct {
	// MaxConcurrentExecutions is the executor's shared capacity budget. Worker
	// pods and Kubernetes Jobs draw on the same one.
	MaxConcurrentExecutions int
	// IdleTimeout is how long a pool with nothing to do keeps its pods.
	IdleTimeout time.Duration
}

// Reconciler brings the worker pools into line with the work waiting for them.
type Reconciler struct {
	pools       repository.WorkerPoolRepository
	deployments repository.DeploymentRepository
	runtime     ports.WorkerPoolRuntime
	policy      routing.Policy
	context     ControllerContextFunc
	clock       ports.Clock
	logger      *slog.Logger
	credential  CredentialFunc
	limit       int
	idleTimeout time.Duration
}

// NewReconciler constructs the pool reconciler.
func NewReconciler(deps Deps, cfg Config) *Reconciler {
	credential := deps.Credential
	if credential == nil {
		credential = workerapi.NewCredential
	}
	return &Reconciler{
		pools:       deps.Pools,
		deployments: deps.Deployments,
		runtime:     deps.Runtime,
		policy:      deps.Policy,
		context:     deps.ControllerContext,
		clock:       deps.Clock,
		logger:      deps.Logger,
		credential:  credential,
		limit:       cfg.MaxConcurrentExecutions,
		idleTimeout: cfg.IdleTimeout,
	}
}

// Reconcile runs one pass: register what is missing, size what exists, and tell
// the runtime.
//
// The reads are taken outside a transaction and without the capacity lock,
// because the sizing they feed is advisory. A replica count is a request for
// pods, not a reservation: a worker still has to claim its task through the
// lease path, which takes the lock and re-checks the budget. So a tick that
// sizes a pool against a slightly stale slot count over-asks for pods at worst,
// and those pods find no work to claim.
func (r *Reconciler) Reconcile(ctx context.Context) error {
	now := r.clock.Now()

	minted, err := r.registerNewPools(ctx, now)
	if err != nil {
		return err
	}

	pools, err := r.pools.List(ctx)
	if err != nil {
		return fmt.Errorf("list worker pools: %w", err)
	}
	if len(pools) == 0 {
		return nil
	}

	demand, err := r.deployments.ListPoolDemand(ctx, now)
	if err != nil {
		return fmt.Errorf("list pool demand: %w", err)
	}
	activeSlots, err := r.deployments.ActiveSlotCount(ctx)
	if err != nil {
		return fmt.Errorf("count active slots: %w", err)
	}

	byKey := make(map[string]model.PoolDemand, len(demand))
	for _, d := range demand {
		byKey[d.PoolKey] = d
	}

	allocated := workerpool.Allocate(r.eligible(pools, byKey), activeSlots, r.limit)

	for _, p := range pools {
		if err := r.reconcilePool(ctx, p, byKey[p.PoolKey], allocated[p.PoolKey], minted[p.PoolKey], now); err != nil {
			return err
		}
	}
	return nil
}

// registerNewPools registers the pools that waiting work implies, returning the
// raw credentials it minted, keyed by pool.
//
// A minted credential is held for the rest of this tick and no longer: it goes
// into its pool's Secret and is dropped. Nothing reads a credential back — the
// pool row keeps only its digest — so if a tick dies between registering a pool
// and writing its Secret, the credential it minted is gone. That is not a leak
// to repair but the ordinary case the rotation path already handles: the next
// tick sees a registered pool with no Secret and mints a replacement.
func (r *Reconciler) registerNewPools(ctx context.Context, now time.Time) (map[string]string, error) {
	wanted, err := r.pools.ListUnregistered(ctx)
	if err != nil {
		return nil, fmt.Errorf("list unregistered worker pools: %w", err)
	}

	minted := make(map[string]string, len(wanted))
	for _, identity := range wanted {
		credential, err := r.credential()
		if err != nil {
			return nil, err
		}
		if err := r.pools.Add(ctx, model.WorkerPool{
			PoolKey:          identity.PoolKey,
			ServiceName:      identity.ServiceName,
			ImageTag:         identity.ImageTag,
			RuntimeManifest:  identity.RuntimeManifest,
			CredentialSHA256: workerapi.HashCredential(credential),
			DesiredReplicas:  0,
			LastActivityAt:   now,
			CreatedAt:        now,
			UpdatedAt:        now,
		}); err != nil {
			return nil, fmt.Errorf("register worker pool %s: %w", identity.PoolKey, err)
		}
		minted[identity.PoolKey] = credential

		r.logger.Info("registered a worker pool for the work waiting on it",
			"pool_key", identity.PoolKey, "service_name", identity.ServiceName,
			"image_tag", identity.ImageTag,
			"runtime_manifest_sha256", identity.RuntimeManifest.RuntimeManifestSHA256)
	}
	return minted, nil
}

// eligible narrows the demand to the pools that may be given capacity.
//
// A pool is skipped when its service has been turned back to jobs — it is not
// allowed to run that work any more — and when its workers cannot initialize,
// because pods that can execute nothing would hold capacity away from pools
// whose pods can. Both keep their leases and drain through reconcilePool; what
// they do not get is anything new.
func (r *Reconciler) eligible(pools []model.WorkerPool, byKey map[string]model.PoolDemand) []model.PoolDemand {
	out := make([]model.PoolDemand, 0, len(pools))
	for _, p := range pools {
		if r.policy.ModeFor(p.ServiceName) == model.ExecutionModeJobs || !p.Ready() {
			continue
		}
		if d, ok := byKey[p.PoolKey]; ok {
			out = append(out, d)
		}
	}
	return out
}

// reconcilePool sizes one pool, repairs its credential when it must, and tells
// the runtime.
func (r *Reconciler) reconcilePool(
	ctx context.Context,
	p model.WorkerPool,
	demand model.PoolDemand,
	allocatedPending int,
	mintedCredential string,
	now time.Time,
) error {
	if err := r.demoteIfRolledBack(ctx, p, now); err != nil {
		return err
	}

	status, _, err := r.runtime.Status(ctx, p.PoolKey)
	if err != nil {
		return fmt.Errorf("read worker pool %s status: %w", p.PoolKey, err)
	}

	credential, err := r.credentialFor(p, status, mintedCredential)
	if err != nil {
		return err
	}
	if credential != "" {
		p.CredentialSHA256 = workerapi.HashCredential(credential)
	}

	p.DesiredReplicas = workerpool.DesiredReplicas(workerpool.ScaleInput{
		CurrentReplicas:      status.DesiredReplicas,
		ActiveLeases:         demand.ActiveLeases,
		AllocatedPending:     allocatedPending,
		LastActivityAt:       p.LastActivityAt,
		Now:                  now,
		IdleTimeout:          r.idleTimeout,
		InitializationFailed: !p.Ready(),
	})
	// The idle clock measures time with nothing to do, so any work at all resets
	// it. Without this a pool busy for longer than the timeout would be retired
	// the instant its last task settled, throwing away warm pods that the next
	// task needs immediately.
	if demand.ActiveLeases > 0 || demand.Pending > 0 {
		p.LastActivityAt = now
	}
	p.UpdatedAt = now

	if err := r.pools.Save(ctx, p); err != nil {
		return fmt.Errorf("save worker pool %s: %w", p.PoolKey, err)
	}

	controllerContext, err := r.context(p.ServiceName)
	if err != nil {
		return fmt.Errorf("build runtime context for service %s: %w", p.ServiceName, err)
	}

	if err := r.runtime.Ensure(ctx, ports.WorkerPoolSpec{
		PoolKey:               p.PoolKey,
		ServiceName:           p.ServiceName,
		ImageTag:              p.ImageTag,
		RuntimeManifest:       p.RuntimeManifest,
		ControllerContextJSON: controllerContext,
		Credential:            credential,
		DesiredReplicas:       int32(p.DesiredReplicas), // #nosec G115 -- bounded by MaxConcurrentExecutions
	}); err != nil {
		return fmt.Errorf("reconcile worker pool %s: %w", p.PoolKey, err)
	}
	return nil
}

// credentialFor returns the raw credential this tick must write into the pool's
// Secret, or empty when the Secret already holds the pool's credential.
//
// A pool registered this tick carries the credential that registration minted. A
// pool whose Secret has gone missing is rotated: its stored digest matches a
// value nobody holds any more, so no worker could ever authenticate, and both
// halves are replaced together in this one attempt rather than leaving the pool
// unreachable until some later repair.
//
// Everything else returns empty, which is the overwhelming majority of ticks: a
// pool's credential is minted once and read back never.
func (r *Reconciler) credentialFor(
	p model.WorkerPool, status ports.PoolStatus, mintedCredential string,
) (string, error) {
	if mintedCredential != "" {
		return mintedCredential, nil
	}
	if status.SecretExists {
		return "", nil
	}

	credential, err := r.credential()
	if err != nil {
		return "", err
	}
	r.logger.Warn("worker pool secret is missing — rotating the pool's credential",
		"pool_key", p.PoolKey, "service_name", p.ServiceName)
	return credential, nil
}

// demoteIfRolledBack moves a pool's not-yet-started work back to the Kubernetes
// Job path when its service has been turned back to jobs.
//
// Only work that has not started moves. A leased or running task has a worker
// executing it, and converting it would run the same node twice — once on the
// pod that is still working, once as a Job. Those tasks finish where they are;
// the pool drains and then retires on its idle timeout.
func (r *Reconciler) demoteIfRolledBack(ctx context.Context, p model.WorkerPool, now time.Time) error {
	if r.policy.ModeFor(p.ServiceName) != model.ExecutionModeJobs {
		return nil
	}

	moved, err := r.deployments.DemotePendingPoolToJobs(ctx, p.PoolKey, now)
	if err != nil {
		return fmt.Errorf("demote worker pool %s to jobs: %w", p.PoolKey, err)
	}
	if moved > 0 {
		r.logger.Info("service is back on the Jobs path — moved its pool's not-yet-started work",
			"pool_key", p.PoolKey, "service_name", p.ServiceName, "deployments", moved)
	}
	return nil
}
