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
	ragithub "github.com/carolsimone/continuo/agent-remediation/adapters/github"
	grpcadapter "github.com/carolsimone/continuo/agent-remediation/adapters/grpc"
	"github.com/carolsimone/continuo/agent-remediation/adapters/llm"
	"github.com/carolsimone/continuo/agent-remediation/adapters/packaging"
	"github.com/carolsimone/continuo/agent-remediation/adapters/postgres"
	rredis "github.com/carolsimone/continuo/agent-remediation/adapters/redis"
	"github.com/carolsimone/continuo/agent-remediation/adapters/releasehttp"
	"github.com/carolsimone/continuo/agent-remediation/adapters/repofs"
	"github.com/carolsimone/continuo/agent-remediation/adapters/s3"
	"github.com/carolsimone/continuo/agent-remediation/adapters/sanitizer"
	remediationv1 "github.com/carolsimone/continuo/agent-remediation/api/remediation/v1"
	"github.com/carolsimone/continuo/agent-remediation/config"
	"github.com/carolsimone/continuo/agent-remediation/service/handlers"
	"github.com/carolsimone/continuo/agent-remediation/service/llmcache"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/promptlog"
	"github.com/carolsimone/continuo/agent-remediation/service/proposals"
	"github.com/carolsimone/continuo/agent-remediation/service/shadowverify"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
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

	bundleReader := s3.NewCandidateSourceReader(
		cfg.S3.EndpointURL, cfg.S3.Bucket, cfg.S3.Region, cfg.S3.AccessKeyID, cfg.S3.SecretAccessKey)

	graphClient, err := grpcadapter.NewGraphClient(cfg.OrchestratorAddr)
	if err != nil {
		logger.Error("grpc graph client dial", "error", err)
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

	// Log the full prompt fed to the model on every real call. The logging
	// decorator wraps the raw provider and the cache wraps the logger, so a
	// prompt is recorded exactly when the model is called — a cache hit reuses a
	// prior completion and logs nothing, because nothing is fed to the model.
	loggedLLM := promptlog.New(llmProvider, logger)

	// Wrap the provider in a best-effort, idempotency-keyed Redis cache so a
	// redelivered remediation.requested trigger reuses the prior completion
	// instead of re-paying the LLM call. A cache miss or error falls through to
	// the real provider, so the cache can never break the happy path.
	cachedLLM := llmcache.New(
		loggedLLM,
		rredis.NewLLMResponseCache(rc, cfg.LLMCacheTTL),
		cfg.LLMModel,
		logger,
	)

	// One GitHub adapter instance serves every read-only port backed by the
	// GitHub API: source reads and repo-archive fetches for the fixers, and
	// PR-status reads for the outcome reconciler. A request deadline keeps a
	// hung GitHub connection from stalling the callers that share this
	// adapter.
	gh := ragithub.NewSourceReader(cfg.GitHubBaseURL, cfg.GitHubToken, &http.Client{Timeout: 30 * time.Second})

	// Shadow verification: a proposed python-node fix is packaged by the same
	// continuo-runtime CLI the team's release CI runs, then submitted to
	// release-controller as a release that runs the full validation pipeline
	// but stops before promoting.
	releaseGateway := releasehttp.NewGateway(cfg.ReleaseControllerURL, store,
		&http.Client{Timeout: 30 * time.Second})

	// The packager is resolved here rather than at first use: an image built
	// without the CLI cannot package any python fix, and failing at boot names
	// that once, instead of failing every remediation trigger forever.
	packager, err := packaging.NewCLIPackager()
	if err != nil {
		logger.Error("contract packager unavailable", "error", err)
		os.Exit(1)
	}

	// The proposal repository is bound to the DB rather than to a transaction:
	// the gRPC read path, the reconciler, and the fixers' prior-attempt reads
	// all use it outside any unit of work.
	proposalRepo := postgres.NewProposalRepository(db, cfg.ServiceRepoPaths)
	contracts := repofs.NewLocator(logger)

	deps := handlers.Deps{
		NewUoW:           func() uow.UnitOfWork { return postgres.NewUnitOfWork(db, logger, cfg.ServiceRepoPaths) },
		LLM:              cachedLLM,
		Evidence:         store,
		Source:           gh,
		Sanitizer:        sanitizer.Passthrough{},
		Artifacts:        store,
		Clock:            ports.SystemClock{},
		Logger:           logger,
		MaxAttempts:      cfg.MaxAttempts,
		ServiceRepoPaths: cfg.ServiceRepoPaths,
		Locator:          graphClient,
		Upstream:         graphClient,
		Versions:         graphClient,
		Precedents:       graphClient,
		CandidateSource:  bundleReader,
		RepoArchive:      gh,
		ContractLocator:  contracts,
		// The same adapter answers both: locating the file that declares a node
		// and reading the declarations a file holds are one yaml shape.
		ContractInspector: contracts,
		Packager:          packager,
		Releases:         releaseGateway,
		PriorAttempts:    proposalRepo,
		SQLDialect:       cfg.SQLDialect,
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
	mux.HandleFunc("/healthz", liveness.Handler("readiness", liveReg.Check, logger))
	mux.HandleFunc("/livez", liveness.Handler("liveness", liveReg.LivenessCheck, logger))
	srv := &http.Server{Addr: ":" + cfg.HTTPPort, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.ListenAndServe() }()

	// Start the RemediationProposals gRPC server. The proposal service uses a
	// DB-bound (non-transactional) repository for reads and the UoW factory for
	// write operations, matching the consumer's wiring above.
	proposalSvc := proposals.New(proposals.Deps{
		Repo:   proposalRepo,
		NewUoW: func() uow.UnitOfWork { return postgres.NewUnitOfWork(db, logger, cfg.ServiceRepoPaths) },
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
	// proposal rows, and sweep stuck 'opening' claims — recovering a proposal
	// whose PR was created on GitHub but never recorded, and releasing a
	// genuinely abandoned claim back to 'failed' — on a fixed cadence.
	reconciler := proposals.NewReconciler(proposals.ReconcilerDeps{
		Lister:             proposalRepo,
		Checker:            gh,
		Recorder:           proposalSvc,
		Clock:              ports.SystemClock{},
		Logger:             logger,
		Interval:           cfg.PRPollInterval,
		OpeningLister:      proposalRepo,
		BranchFinder:       gh,
		OpeningRecorder:    proposalSvc,
		Failer:             proposalSvc,
		OpeningGracePeriod: cfg.PROpeningGracePeriod,
	})
	go reconciler.Run(ctx)

	// Resolve proposals whose fix is being judged by a shadow release: read
	// each waiting attempt's release, finalize the ones it validated so an
	// operator can review them, and record why the rest failed before starting
	// the next attempt. The decoder and the fix proposer are the same ones the
	// remediation.requested consumer uses, so a retried attempt runs through
	// exactly the code path the original trigger did.
	shadowReconciler := shadowverify.New(shadowverify.Deps{
		Lister:   proposalRepo,
		Releases: releaseGateway,
		NewUoW:   func() uow.UnitOfWork { return postgres.NewUnitOfWork(db, logger, cfg.ServiceRepoPaths) },
		Decode:   rredis.TriggerFromPayload,
		Propose: func(ctx context.Context, t handlers.Trigger) error {
			return handlers.ProposeFix(ctx, deps, t)
		},
		Clock:    ports.SystemClock{},
		Logger:   logger,
		Interval: cfg.ShadowVerifyPollInterval,
		Timeout:  cfg.ShadowVerifyTimeout,
	})
	go shadowReconciler.Run(ctx)

	logger.Info("agent-remediation started", "http_port", cfg.HTTPPort, "grpc_port", cfg.GRPCPort)
	<-ctx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	grpcSrv.GracefulStop()
	_ = srv.Shutdown(shutdownCtx)
	logger.Info("agent-remediation stopped")
}
