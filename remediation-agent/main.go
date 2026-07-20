package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/liveness"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	ragithub "github.com/carolsimone/continuo/remediation-agent/adapters/github"
	grpcadapter "github.com/carolsimone/continuo/remediation-agent/adapters/grpc"
	"github.com/carolsimone/continuo/remediation-agent/adapters/llm"
	"github.com/carolsimone/continuo/remediation-agent/adapters/postgres"
	rredis "github.com/carolsimone/continuo/remediation-agent/adapters/redis"
	"github.com/carolsimone/continuo/remediation-agent/adapters/s3"
	"github.com/carolsimone/continuo/remediation-agent/adapters/sanitizer"
	remediationv1 "github.com/carolsimone/continuo/remediation-agent/api/remediation/v1"
	"github.com/carolsimone/continuo/remediation-agent/config"
	"github.com/carolsimone/continuo/remediation-agent/service/handlers"
	"github.com/carolsimone/continuo/remediation-agent/service/llmcache"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
	"github.com/carolsimone/continuo/remediation-agent/service/proposals"
	"github.com/carolsimone/continuo/remediation-agent/service/uow"
)

// llmClientTimeout bounds a single LLM HTTP request. Without it the LLM path
// uses http.DefaultClient, which has NO timeout, so one hung request could
// block the handler goroutine forever. The remediation.requested handler makes
// up to two sequential LLM calls per invocation, so the whole handler is
// bounded by roughly 2 × this value (plus DB/S3 work), which consumerHandler
// Timeout below then caps as a hard ceiling.
const llmClientTimeout = 120 * time.Second

// consumerHandlerTimeout bounds each message handler invocation with a context
// deadline, so a genuinely-hung handler eventually returns control to the read
// loop. It is generous because this handler runs up to two large (~16k-token)
// LLM requests; 300s comfortably covers 2 × llmClientTimeout plus DB/S3 work
// while still bounding a true hang.
const consumerHandlerTimeout = 5 * time.Minute

// consumerHeartbeatStale is the liveness heartbeat budget: how long the
// consumer's read loop may make no progress before the liveness probe restarts
// the pod. It MUST exceed consumerHandlerTimeout plus a margin so a legitimately
// in-flight handler — including a slow multi-call LLM invocation — never trips
// liveness (the heartbeat advances per handler attempt; see
// pkg/redis.StreamConsumer.safeInvoke), while a true wedge still trips within
// budget.
const consumerHeartbeatStale = 6 * time.Minute

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	v := &pkgconfig.Validator{}
	cfg := config.Load(v)
	if missing := v.Missing(); len(missing) > 0 {
		logger.Error("missing required env vars", "vars", strings.Join(missing, ", "))
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("shutdown signal received")
		cancel()
	}()

	// Health registry feeding /healthz (readiness) and /livez (liveness) —
	// deploy config points the two Kubernetes probes at those DIFFERENT paths
	// (see deploy/continuo/values.yaml). Worker registrations and consumer
	// heartbeats feed both checks (a dead/wedged consumer restarts the pod);
	// dependency probes (Redis/Postgres) feed readiness ONLY (a backing-store
	// outage stops traffic but must not restart a pod whose consumer is already
	// retrying).
	liveReg := liveness.NewRegistry()

	// runConsumer starts a tracked stream consumer: a bounded handler deadline
	// so a hung handler (e.g. a stuck LLM request) eventually returns;
	// RegisterWorker before launch so a missing worker is observable;
	// WorkerExited when Start returns (which now happens on a permanent
	// bootstrap error too, not only clean shutdown); and a worker heartbeat
	// probe so a wedged-but-not-exited loop is caught.
	runConsumer := func(name string, consumer *pkgredis.StreamConsumer) {
		consumer.SetHandlerTimeout(consumerHandlerTimeout)
		liveReg.RegisterWorker(name)
		liveReg.AddWorkerProbe(name+"_heartbeat", 10*time.Second, func(context.Context) error {
			return consumer.Healthy(consumerHeartbeatStale)
		})
		go func() {
			err := consumer.Start(ctx)
			liveReg.WorkerExited(name, err)
			if err != nil && ctx.Err() == nil {
				logger.Error("consumer stopped", "consumer", name, "error", err)
			}
		}()
	}

	db, err := postgres.NewDB(cfg.Postgres)
	if err != nil {
		logger.Error("postgres connect", "error", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()
	liveReg.AddProbe("postgres", 5*time.Second, func(ctx context.Context) error {
		return db.PingContext(ctx)
	})

	rc, err := rredis.NewClient(ctx, rredis.Config{
		Host:     cfg.Redis.Host,
		Port:     strconv.Itoa(cfg.Redis.Port),
		Password: cfg.Redis.Password,
	})
	if err != nil {
		logger.Error("redis connect", "error", err)
		os.Exit(1)
	}
	defer func() { _ = rc.Close() }()
	liveReg.AddProbe("redis", 5*time.Second, func(ctx context.Context) error {
		return rc.Ping(ctx).Err()
	})

	store := s3.NewS3(
		cfg.S3.EndpointURL,
		cfg.S3.Bucket,
		cfg.S3.Region,
		cfg.S3.AccessKeyID,
		cfg.S3.SecretAccessKey,
	)

	ancestryClient, err := grpcadapter.NewAncestryClient(cfg.OrchestratorAddr)
	if err != nil {
		logger.Error("grpc ancestry client dial", "error", err)
		os.Exit(1)
	}

	// Bound the LLM HTTP client explicitly: passing nil would fall back to
	// http.DefaultClient, which has NO request timeout, so a single hung LLM
	// call could block the handler goroutine indefinitely (and, with the
	// consumer heartbeat feeding liveness, get the pod restarted underneath
	// in-flight work). A per-request timeout keeps each call bounded; the
	// consumer's handler-timeout is the outer ceiling over the whole handler.
	llmProvider, err := llm.NewProvider(cfg.LLMProvider, cfg.LLMAPIKey, cfg.LLMModel, cfg.LLMBaseURL,
		&http.Client{Timeout: llmClientTimeout})
	if err != nil {
		logger.Error("llm provider init", "error", err)
		os.Exit(1)
	}

	// Wrap the provider in a best-effort, idempotency-keyed Redis cache so a
	// redelivered remediation.requested trigger reuses the prior completion
	// instead of re-paying the LLM call. A cache miss or error falls through to
	// the real provider, so the cache can never break the happy path.
	cachedLLM := llmcache.New(
		llmProvider,
		rredis.NewLLMResponseCache(rc, cfg.LLMCacheTTL),
		cfg.LLMModel,
		logger,
	)

	// One GitHub adapter instance serves both read-only ports: source reads for
	// the fixers and PR-status reads for the outcome reconciler. A request
	// deadline keeps a hung GitHub connection from stalling the callers that
	// share this adapter — source reads and the PR reconciler.
	gh := ragithub.NewSourceReader(cfg.GitHubBaseURL, cfg.GitHubToken, &http.Client{Timeout: 30 * time.Second})

	deps := handlers.Deps{
		NewUoW:           func() uow.UnitOfWork { return postgres.NewUnitOfWork(db, logger) },
		LLM:              cachedLLM,
		Evidence:         store,
		Ancestry:         ancestryClient,
		Source:           gh,
		Sanitizer:        sanitizer.Passthrough{},
		Artifacts:        store,
		Clock:            ports.SystemClock{},
		Logger:           logger,
		MaxAttempts:      cfg.MaxAttempts,
		ServiceRepoPaths: cfg.ServiceRepoPaths,
	}

	// Start the outbox publisher; spawns its own goroutine internally and runs
	// until ctx is cancelled.
	rredis.StartOutboxPublisher(ctx, db, rc, liveReg, logger)

	// Start the remediation.requested consumer in a goroutine; blocks until ctx
	// is cancelled.
	runConsumer("remediation_requested", rredis.NewRemediationRequestedConsumer(rc, deps, logger))

	mux := http.NewServeMux()
	// Two health paths with different semantics, both registry-backed. Deploy
	// config points the Kubernetes readinessProbe at /healthz and the
	// livenessProbe at /livez (see deploy/continuo/values.yaml): /healthz
	// (readiness) reflects workers + heartbeats + dependency probes, so a Redis/
	// Postgres outage pulls the pod from Service endpoints; /livez (liveness)
	// reflects workers + heartbeats ONLY, so a dependency outage does NOT restart
	// a pod whose consumer is already retrying, while a dead/wedged consumer does.
	mux.HandleFunc("/healthz", healthHandler("readiness", liveReg.Check, logger))
	mux.HandleFunc("/livez", healthHandler("liveness", liveReg.LivenessCheck, logger))
	srv := &http.Server{Addr: ":" + cfg.HTTPPort, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.ListenAndServe() }()

	// Start the RemediationProposals gRPC server. The proposal service uses a
	// DB-bound (non-transactional) repository for reads and the UoW factory for
	// write operations, matching the consumer's wiring above.
	proposalRepo := postgres.NewProposalRepository(db)
	proposalSvc := proposals.New(proposals.Deps{
		Repo:   proposalRepo,
		NewUoW: func() uow.UnitOfWork { return postgres.NewUnitOfWork(db, logger) },
		Clock:  ports.SystemClock{},
	})
	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		logger.Error("grpc listen", "error", err)
		os.Exit(1)
	}
	grpcSrv := grpc.NewServer()
	remediationv1.RegisterRemediationProposalsServer(grpcSrv, grpcadapter.NewProposalsServer(proposalSvc))
	go func() {
		if err := grpcSrv.Serve(lis); err != nil && ctx.Err() == nil {
			logger.Error("grpc server stopped", "error", err)
		}
	}()

	// Mirror terminal GitHub PR outcomes (merged / closed-without-merge) onto
	// proposal rows on a fixed cadence.
	reconciler := proposals.NewReconciler(proposals.ReconcilerDeps{
		Lister:   proposalRepo,
		Checker:  gh,
		Recorder: proposalSvc,
		Clock:    ports.SystemClock{},
		Logger:   logger,
		Interval: cfg.PRPollInterval,
	})
	go reconciler.Run(ctx)

	logger.Info("remediation-agent started", "http_port", cfg.HTTPPort, "grpc_port", cfg.GRPCPort)
	<-ctx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	grpcSrv.GracefulStop()
	_ = srv.Shutdown(shutdownCtx)
	logger.Info("remediation-agent stopped")
}

// healthHandler backs a health endpoint with the supplied registry check: 200
// when it reports no failures, 503 otherwise (logging each failing component).
// kind names the probe ("readiness" or "liveness"). Extracted from main so it
// is directly unit-testable (see main_test.go) — the regression coverage that
// pins "a dead consumer flips both probes to 503, but a dependency outage flips
// only readiness" rather than the old hardcoded-200 behaviour.
func healthHandler(kind string, check func(context.Context) []liveness.Failure, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		failures := check(r.Context())
		if len(failures) == 0 {
			w.WriteHeader(http.StatusOK)
			return
		}
		for _, f := range failures {
			logger.Warn("Health check failed", "kind", kind, "component", f.Name, "error", f.Err)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}
