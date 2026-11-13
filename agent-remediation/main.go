package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"

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
	"github.com/carolsimone/continuo/agent-remediation/adapters/workflowfile"
	remediationv1 "github.com/carolsimone/continuo/agent-remediation/api/remediation/v1"
	"github.com/carolsimone/continuo/agent-remediation/config"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/handlers"
	"github.com/carolsimone/continuo/agent-remediation/service/llmcache"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/promptlog"
	"github.com/carolsimone/continuo/agent-remediation/service/proposals"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
	"github.com/carolsimone/continuo/agent-remediation/service/verification"
	"github.com/carolsimone/continuo/agent-remediation/workflows"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgdb "github.com/carolsimone/continuo/pkg/db"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/lifecycle"
	"github.com/carolsimone/continuo/pkg/liveness"
	pkgmessageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/carolsimone/continuo/pkg/objectstore"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	pkgretention "github.com/carolsimone/continuo/pkg/retention"
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

// dbPool bounds the Postgres pool when DB_MAX_OPEN_CONNS / DB_MAX_IDLE_CONNS
// are unset: two connections per stream consumer (a handler's transaction and
// a read outside it), plus the outbox relay, background loops and request
// handlers.
var dbPool = pkgconfig.PoolConfig{MaxOpenConns: 10, MaxIdleConns: 5}

func main() {
	// The logger writes at the level LOG_LEVEL names. An unknown level is
	// recorded on v, and the missing-configuration check below stops the
	// service once this logger can report it.
	v := &pkgconfig.Validator{}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: pkgconfig.LoadLogLevel(v)}))

	cfg := config.Load(v)
	if missing := v.Missing(); len(missing) > 0 {
		logger.Error("missing required env vars", "vars", strings.Join(missing, ", "))
		os.Exit(1)
	}

	if cfg.Maintenance {
		logger.Warn("Maintenance mode is on: fix attempts that come due are recorded skipped")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lifecycleManager := lifecycle.NewApplicationLifecycle(logger)

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
	metricsReg := pkgmetrics.New(config.ServiceName)

	// runConsumer starts a tracked stream consumer: a bounded handler deadline
	// so a hung handler (e.g. a stuck LLM request) eventually returns;
	// RegisterWorker before launch so a missing worker is observable;
	// WorkerExited when Start returns (on a permanent consumer-group bootstrap
	// error as well as on a clean shutdown); and a worker heartbeat probe so a
	// wedged-but-not-exited loop is caught.
	runConsumer := func(name string, consumer *pkgredis.StreamConsumer) {
		consumer.SetService(config.ServiceName)
		consumer.SetObserver(metricsReg.Consumers())
		consumer.SetHandlerTimeout(consumerHandlerTimeout)
		liveReg.RegisterWorker(name)
		liveReg.AddWorkerProbe(name+"_heartbeat", 10*time.Second, func(context.Context) error {
			return consumer.Healthy(consumer.HeartbeatBudget())
		})
		go func() {
			err := consumer.Start(ctx)
			liveReg.WorkerExited(name, err)
			if err != nil && ctx.Err() == nil {
				logger.Error("consumer stopped", "consumer", name, "error", err)
			}
		}()
	}

	mux := http.NewServeMux()
	// Two health paths with different semantics, both registry-backed. Deploy
	// config points the Kubernetes readinessProbe at /healthz and the
	// livenessProbe at /livez (see deploy/continuo/values.yaml): /healthz
	// (readiness) reflects workers + heartbeats + dependency probes, so a Redis/
	// Postgres outage pulls the pod from Service endpoints; /livez (liveness)
	// reflects workers + heartbeats ONLY, so a dependency outage does NOT restart
	// a pod whose consumer is already retrying, while a dead/wedged consumer does.
	// The server comes up before any dependency is reachable so /livez keeps
	// answering while the boot waits for Redis; the startup gate holds /healthz
	// false until the consumer, gRPC server and reconcilers are running.
	startup := liveReg.AddStartupGate()
	mux.HandleFunc("/healthz", liveness.Handler("readiness", liveReg.Check, logger))
	mux.HandleFunc("/livez", liveness.Handler("liveness", liveReg.LivenessCheck, logger))
	srv := &http.Server{Addr: ":" + cfg.HTTPPort, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.ListenAndServe() }()

	// The Prometheus listener comes up before any dependency is reachable so a
	// dependency outage stays observable. Serve runs in a plain goroutine; it
	// is stopped when ctx ends.
	metricsServer, err := pkgmetrics.Listen(cfg.MetricsPort, metricsReg, logger)
	if err != nil {
		logger.Error("Failed to bind the metrics port", "port", cfg.MetricsPort, "error", err)
		os.Exit(1)
	}
	go func() {
		if err := metricsServer.Serve(); err != nil {
			logger.Error("Metrics server error", "error", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsServer.Shutdown(shutdownCtx)
	}()

	db, err := pkgdb.Connect(ctx, cfg.Postgres, dbPool, pkgdb.DefaultStartupPolicy(), logger)
	if errors.Is(err, context.Canceled) {
		logger.Info("Shutdown requested while waiting for Postgres")
		os.Exit(0)
	}
	if err != nil {
		logger.Error("Failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()
	liveReg.AddProbe("postgres", 5*time.Second, func(ctx context.Context) error {
		return db.PingContext(ctx)
	})
	metricsReg.WatchDB(db.DB, cfg.Postgres.DB)

	// Redis is a separate workload that may still be starting (or its Service
	// not yet resolvable) when this process boots, so wait for it with backoff
	// instead of exiting on the first refused dial.
	rc, err := pkgredis.Connect(ctx, cfg.Redis, pkgredis.DefaultStartupPolicy(), logger)
	if errors.Is(err, context.Canceled) {
		logger.Info("shutdown requested while waiting for redis")
		os.Exit(0)
	}
	if err != nil {
		logger.Error("redis connect", "error", err)
		os.Exit(1)
	}
	defer func() { _ = rc.Close() }()
	liveReg.AddProbe("redis", 5*time.Second, func(ctx context.Context) error {
		return rc.Ping(ctx).Err()
	})
	metricsReg.WatchRedis(rc)

	// One object-storage client for agent-remediation's adapters, each given
	// the role it uses: the evidence reader and the candidate-source reader
	// read through it, the artifact writer writes through it. Static S3 keys
	// when both are set; otherwise the SDK's default credential chain (IAM
	// role, workload identity).
	objectStore, err := objectstore.New(ctx, objectstore.ConfigFromS3(cfg.S3))
	if err != nil {
		logger.Error("object storage client", "error", err)
		os.Exit(1)
	}
	evidenceReader := s3.NewEvidenceReader(objectStore)
	artifactWriter := s3.NewArtifactWriter(objectStore, cfg.S3.Bucket)
	bundleReader := s3.NewCandidateSourceReader(objectStore)

	graphClient, err := grpcadapter.NewGraphClient(cfg.CatalogAddr)
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

	// Fix verification: a proposed fix's edits are packaged (a merged contract
	// for a python service, a source overlay for a dbt one) and submitted to
	// release-controller as a verification run that carries the full
	// parse -> candidate-schema -> validation pipeline through to a terminal
	// passed/failed verdict without ever promoting.
	releaseGateway := releasehttp.NewGateway(cfg.ReleaseControllerURL, evidenceReader,
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
		Evidence:         evidenceReader,
		Source:           gh,
		Sanitizer:        sanitizer.Passthrough{},
		Artifacts:        artifactWriter,
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
		Pipeline:          releaseGateway,
		Releases:          releaseGateway,
		PriorAttempts:     proposalRepo,
		SQLDialect:        cfg.SQLDialect,
		Maintenance:       cfg.Maintenance,
	}

	// The runner works a heal trigger as a remediation of committed steps. Its
	// worker, gate poller and sweeper run with either engine: the engine only
	// decides what the remediation.requested consumer does with a new trigger,
	// and a remediation the runner started must finish whichever engine runs
	// now. Its built-in files are checked against the steps the registry holds
	// before anything starts, and a refused file stops startup.
	newUoW := func() uow.UnitOfWork { return postgres.NewUnitOfWork(db, logger, cfg.ServiceRepoPaths) }
	collab := runnerCollaborators(cfg, runnerAdapters{
		github:    gh,
		evidence:  evidenceReader,
		artifacts: artifactWriter,
		graph:     graphClient,
		bundle:    bundleReader,
		contracts: contracts,
		packager:  packager,
		release:   releaseGateway,
		proposals: proposalRepo,
	}, logger)
	files, err := loadRunnerFiles(workflows.Builtin, collab, logger)
	if err != nil {
		logger.Error("the built-in remediation files are refused", "error", err)
		os.Exit(1)
	}
	warnIgnoredMaxAttempts(cfg, files, logger)
	remediationRepo := postgres.NewRemediationRepository(db)
	accept := startRunner(ctx, cfg, collab, files, runnerWiring{
		llm:    cachedLLM,
		newUoW: newUoW,
		repo:   func() repository.RemediationRepository { return remediationRepo },
		owner:  leaseOwner(hostName(), os.Getpid()),
		live:   liveReg,
		logger: logger,
	}, lifecycleManager.Go)

	// The outbox relay wakes on the notification a committed insert into its
	// table sends.
	outboxWaker, err := pkgoutbox.NewPostgresWaker(ctx, cfg.Postgres.DSN, postgres.OutboxTable, logger)
	if errors.Is(err, context.Canceled) {
		logger.Info("shutdown requested while waiting for the outbox listener")
		os.Exit(0)
	}
	if err != nil {
		logger.Error("Failed to listen for outbox notifications", "error", err)
		os.Exit(1)
	}
	// Closing the listener can wait on a connection attempt in progress, so
	// shutdown stops waiting for it after 5 seconds.
	defer func() {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelClose()
		_ = outboxWaker.CloseContext(closeCtx)
	}()

	// Start the outbox publisher; spawns its own goroutine internally and runs
	// until ctx is cancelled.
	outboxMetrics := pkgoutbox.NewMetrics(metricsReg)
	outboxProc := rredis.StartOutboxPublisher(ctx, db, rc, outboxWaker, outboxMetrics, liveReg, logger)
	outboxMetrics.Watch(outboxProc)

	// Retention sweeper — prunes processed agent_remediation_outbox rows older
	// than RETENTION_DAYS and message_processing dedup rows, whatever their
	// state, older than the longer of RETENTION_DAYS and the 30-day replay
	// horizon, on one timer using DB-clock cutoffs.
	mpPruner := pkgmessageprocessing.NewPruner(db, postgres.OutboxTable, logger)
	retentionSweeper := pkgretention.NewSweeper(
		[]pkgretention.Target{
			pkgoutbox.OutboxRetentionTarget(db, postgres.OutboxTable, logger),
			{
				Name:         "message_processing",
				Prune:        mpPruner.DeleteOlderThan,
				MinRetention: model.ReplayHorizon,
			},
			{
				Name:         "remediation",
				Prune:        remediationRepo.DeleteFinishedOlderThan,
				MinRetention: model.ReplayHorizon,
			},
		},
		pkgretention.Config{
			Retention: time.Duration(cfg.RetentionDays) * 24 * time.Hour,
			Interval:  time.Duration(cfg.RetentionSweepIntervalMin) * time.Minute,
		},
		logger,
	)
	lifecycleManager.Go(func() { retentionSweeper.Run(ctx) })

	// Start the remediation.requested consumer in a goroutine; blocks until ctx
	// is cancelled. The engine decides whether it proposes a fix itself or
	// hands the trigger to the runner.
	logger.Info("remediation engine", "engine", cfg.Engine)
	runConsumer("remediation_requested", rredis.NewRemediationRequestedConsumer(rc, deps, accept, cfg.Engine, logger))

	// Start the RemediationProposals gRPC server. The proposal service uses a
	// DB-bound (non-transactional) repository for reads and the UoW factory for
	// write operations, matching the consumer's wiring above.
	proposalSvc := proposals.New(proposals.Deps{
		Repo:             proposalRepo,
		NewUoW:           func() uow.UnitOfWork { return postgres.NewUnitOfWork(db, logger, cfg.ServiceRepoPaths) },
		Clock:            ports.SystemClock{},
		ServiceRepoPaths: cfg.ServiceRepoPaths,
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
		// At a merged PR the reconciler byte-compares each edited file at the
		// merge commit against the proposal to stamp per-edit amend flags on
		// pr_closed: the source reader reads the merged file from GitHub, the S3
		// store the proposed content and diff, the getter loads the proposal's
		// edits, and ServiceRepoPaths splits them by owning service.
		Getter:           proposalRepo,
		Sources:          gh,
		Evidence:         evidenceReader,
		ServiceRepoPaths: cfg.ServiceRepoPaths,
	})
	go reconciler.Run(ctx)

	// Resolve proposals whose fix is being judged by a verification run: read
	// each waiting attempt's runs, finalize the ones that passed so an
	// operator can review them, and record why the rest failed before starting
	// the next attempt. The decoder and the fix proposer are the same ones the
	// remediation.requested consumer uses, so a retried attempt runs through
	// exactly the code path the original trigger did.
	verifyReconciler := verification.New(verification.Deps{
		Lister:   proposalRepo,
		Pipeline: releaseGateway,
		NewUoW:   func() uow.UnitOfWork { return postgres.NewUnitOfWork(db, logger, cfg.ServiceRepoPaths) },
		Decode:   rredis.TriggerFromPayload,
		Propose: func(ctx context.Context, t handlers.Trigger) error {
			return handlers.ProposeFix(ctx, deps, t)
		},
		Clock:    ports.SystemClock{},
		Logger:   logger,
		Interval: cfg.VerificationPollInterval,
		Timeout:  cfg.VerificationTimeout,
	})
	go verifyReconciler.Run(ctx)

	// Everything is initialised and running: readiness may now turn true.
	startup.Done()

	logger.Info("agent-remediation started", "http_port", cfg.HTTPPort, "grpc_port", cfg.GRPCPort)
	<-ctx.Done()
	// Drain the retention worker before deferred connection-pool closure.
	lifecycleManager.Shutdown(cancel, 5*time.Second)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	grpcSrv.GracefulStop()
	_ = srv.Shutdown(shutdownCtx)
	logger.Info("agent-remediation stopped")
}

// loop is a background loop that runs until its context ends.
type loop interface{ Run(ctx context.Context) }

// runnerLoops are the loops that carry a remediation the runner accepted to its
// end: the worker that drives it, the poller that reads its gate and the
// sweeper that ends one that can go no further. They run with either engine, so
// a remediation the runner started finishes whichever engine runs now.
type runnerLoops struct {
	worker, poller, sweeper loop
}

// runnerWiring is what the runner and its loops are built over.
type runnerWiring struct {
	llm    ports.LLMProvider
	newUoW func() uow.UnitOfWork
	repo   func() repository.RemediationRepository
	// owner names the process on the leases it holds.
	owner  string
	live   *liveness.Registry
	logger *slog.Logger
}

// loopLivenessSlack is added to the longest a loop can go between two beats
// before it is reported wedged.
const loopLivenessSlack = time.Minute

// loopBudgets are how long the worker and the poller may go without beating
// before they are reported wedged. A worker beats at every claim and every
// third of a lease while it drives, so it can be silent for one step and the
// park of a remediation whose drive failed. A poller beats at every pass and
// every remediation it polls, so it can be silent for one poll of a gate and
// the wait between two passes, which VERIFICATION_POLL_INTERVAL sets.
func loopBudgets(cfg config.Config) (worker, poller time.Duration) {
	worker = runner.DefaultStepTimeout + runner.ParkDuration + loopLivenessSlack
	poller = cfg.VerificationPollInterval + runner.DefaultStepTimeout + loopLivenessSlack
	return worker, poller
}

// startRunner builds the runner, starts its three loops with spawn and returns
// the function that accepts a heal trigger as a remediation. The loops start
// with either engine: the engine only decides what the consumer does with a new
// trigger, and a remediation the runner started finishes whichever engine runs
// now.
func startRunner(ctx context.Context, cfg config.Config, collab runner.Collaborators, files workflow.Set, w runnerWiring, spawn func(func())) func(context.Context, runner.Accepted) error {
	loops, accept := buildRunner(cfg, collab, files, w)
	for _, l := range []loop{loops.worker, loops.poller, loops.sweeper} {
		spawn(func() { l.Run(ctx) })
	}
	return accept
}

// buildRunner builds the runner, its three loops and the function that accepts
// a heal trigger as a remediation. The worker and the poller are registered
// with the liveness registry: each beats as it works, and a loop that stops, or
// stops beating, fails the pod's probes. The sweeper takes its own lease name,
// so a worker and the sweeper of one process never share a lease.
func buildRunner(cfg config.Config, collab runner.Collaborators, files workflow.Set, w runnerWiring) (runnerLoops, func(context.Context, runner.Accepted) error) {
	r := runner.New(newRunnerDeps(cfg, collab, files, w.newUoW, w.llm, w.owner, w.logger))
	worker := runner.NewWorker(r, w.repo, w.owner, ports.SystemClock{}, w.logger)
	poller := runner.NewGatePoller(r, w.repo, w.owner, cfg.VerificationPollInterval, worker.Wake, ports.SystemClock{}, w.logger)
	sweeper := runner.NewSweeper(w.newUoW, w.repo, w.owner+"/sweeper", ports.SystemClock{}, w.logger)
	workerBudget, pollerBudget := loopBudgets(cfg)
	worker.SetHeartbeat(registerLoop(w.live, "remediation_worker", workerBudget))
	poller.SetHeartbeat(registerLoop(w.live, "remediation_gate_poller", pollerBudget))
	w.logger.Info("remediation runner built", "engine", cfg.Engine, "owner", w.owner)
	// A trigger the runner accepts wakes the worker, which would otherwise find
	// it at its next look.
	accept := func(ctx context.Context, a runner.Accepted) error {
		err := r.Accept(ctx, a)
		worker.Wake()
		return err
	}
	return runnerLoops{
		worker:  trackedLoop{name: "remediation_worker", inner: worker, live: w.live},
		poller:  trackedLoop{name: "remediation_gate_poller", inner: poller, live: w.live},
		sweeper: sweeper,
	}, accept
}

// registerLoop registers a background loop with the liveness registry, as a
// stream consumer is, with the longest it may go without beating, and returns
// the function the loop calls to show it is working.
func registerLoop(live *liveness.Registry, name string, budget time.Duration) func() {
	h := &loopHealth{}
	h.beat()
	live.RegisterWorker(name)
	live.AddWorkerProbe(name+"_heartbeat", 10*time.Second, func(context.Context) error {
		return h.healthy(budget)
	})
	return h.beat
}

// loopHealth is when a background loop last showed it was working.
type loopHealth struct{ last atomic.Int64 }

func (h *loopHealth) beat() { h.last.Store(time.Now().UnixNano()) }

// healthy fails when the loop has not beaten within budget.
func (h *loopHealth) healthy(budget time.Duration) error {
	if idle := time.Since(time.Unix(0, h.last.Load())); idle > budget {
		return fmt.Errorf("loop stalled: no activity for %s", idle.Round(time.Second))
	}
	return nil
}

// trackedLoop runs a loop and tells the liveness registry when it returns.
type trackedLoop struct {
	name  string
	inner loop
	live  *liveness.Registry
}

func (t trackedLoop) Run(ctx context.Context) {
	t.inner.Run(ctx)
	var err error
	if ctx.Err() == nil {
		err = errors.New("the loop returned before shutdown")
	}
	t.live.WorkerExited(t.name, err)
}

// runnerAdapters are the adapters main builds that the runner's steps read
// through, each typed by every port it answers for the steps: the GitHub client
// reads single files and fetches whole checkouts, the catalog client answers
// every graph read, the contract locator also inspects a contract's
// declarations, and the release gateway reads a release's failures and runs
// verifications.
type runnerAdapters struct {
	github interface {
		ports.SourceReader
		ports.RepoArchive
	}
	evidence  ports.EvidenceReader
	artifacts ports.ArtifactWriter
	graph     interface {
		ports.NodeLocator
		ports.UpstreamChangeReader
		ports.VersionReader
		ports.PrecedentReader
	}
	bundle    ports.CandidateSourceReader
	contracts interface {
		ports.ContractLocator
		ports.ContractInspector
	}
	packager ports.ContractPackager
	release  interface {
		ports.ReleaseReader
		ports.VerificationPipeline
	}
	proposals repository.AttemptLister
}

// runnerCollaborators is the ports the steps are built from: the adapters main
// built, the sanitizer, the patcher that writes a draft over a checkout, the
// system clock, and what the install configures (the repository paths of the
// services, the warehouse's SQL dialect and how long a verification may run).
func runnerCollaborators(cfg config.Config, a runnerAdapters, logger *slog.Logger) runner.Collaborators {
	return runner.Collaborators{
		Source:              a.github,
		Evidence:            a.evidence,
		Sanitizer:           sanitizer.Passthrough{},
		Artifacts:           a.artifacts,
		Logger:              logger,
		ServiceRepoPaths:    cfg.ServiceRepoPaths,
		Locator:             a.graph,
		Upstream:            a.graph,
		Versions:            a.graph,
		Precedents:          a.graph,
		CandidateSource:     a.bundle,
		Archive:             a.github,
		Patcher:             repofs.CheckoutPatcher{},
		ContractLocator:     a.contracts,
		ContractInspector:   a.contracts,
		Packager:            a.packager,
		Releases:            a.release,
		PriorAttempts:       a.proposals,
		SQLDialect:          cfg.SQLDialect,
		Pipeline:            a.release,
		Clock:               ports.SystemClock{},
		VerificationTimeout: cfg.VerificationTimeout,
	}
}

// newRunnerDeps is what the runner works with. The registry is built from
// collab, so the gate's budget is the one collab carries, and the verifying
// try's deadline is built on the same VERIFICATION_TIMEOUT.
func newRunnerDeps(cfg config.Config, collab runner.Collaborators, files workflow.Set, newUoW func() uow.UnitOfWork, llm ports.LLMProvider, owner string, logger *slog.Logger) runner.Deps {
	return runner.Deps{
		NewUoW:              newUoW,
		Registry:            runner.NewRegistry(collab),
		Files:               files,
		LLM:                 llm,
		Collab:              collab,
		Clock:               ports.SystemClock{},
		Logger:              logger,
		Maintenance:         cfg.Maintenance,
		Owner:               owner,
		VerificationTimeout: cfg.VerificationTimeout,
	}
}

// loadRunnerFiles loads the remediation files of fsys, checks every flow and
// type of them against the steps collab's registry holds, and checks that they
// define the flow every heal trigger runs. Every refusal is logged on its own
// line and the error that joins them is returned; a warning is logged and does
// not stop startup. The worker drives the runner's remediations with either
// engine, so files without the trigger's flow stop startup with either.
func loadRunnerFiles(fsys fs.FS, collab runner.Collaborators, logger *slog.Logger) (workflow.Set, error) {
	set, err := workflowfile.Load(fsys)
	if err != nil {
		logRefusals(logger, err)
		return workflow.Set{}, err
	}
	warnings, err := runner.Validate(set, runner.NewRegistry(collab))
	for _, w := range warnings {
		logger.Warn("remediation file", "warning", w)
	}
	if _, ferr := runner.TriggerFlow(set); ferr != nil {
		err = errors.Join(err, ferr)
	}
	if err != nil {
		logRefusals(logger, err)
		return workflow.Set{}, err
	}
	return set, nil
}

// warnIgnoredMaxAttempts warns at startup when the engine is the runner and
// AGENT_REMEDIATION_MAX_ATTEMPTS differs from the tries of the flow every heal
// trigger runs: the runner gives up after the flow file's tries and never reads
// the variable, which only the legacy handler reads.
func warnIgnoredMaxAttempts(cfg config.Config, files workflow.Set, logger *slog.Logger) {
	if cfg.Engine != config.EngineRunner {
		return
	}
	flow, err := runner.TriggerFlow(files)
	if err != nil || flow.Tries == cfg.MaxAttempts {
		return
	}
	logger.Warn("AGENT_REMEDIATION_MAX_ATTEMPTS is not read by the runner engine; the flow file's tries govern",
		"max_attempts", cfg.MaxAttempts, "flow", flow.Name, "tries", flow.Tries)
}

// logRefusals logs each error joined in err on a line of its own.
func logRefusals(logger *slog.Logger, err error) {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, e := range joined.Unwrap() {
			logger.Error("remediation file refused", "refusal", e.Error())
		}
		return
	}
	logger.Error("remediation file refused", "refusal", err.Error())
}

// leaseOwner names a process on the leases it holds: its host and its pid. A
// restarted container keeps its host name and is often pid 1, so the name can
// repeat across restarts; the lease the old process held ends by itself, and
// until it does the new process cannot claim that remediation.
func leaseOwner(host string, pid int) string {
	if host == "" {
		host = "unknown"
	}
	return host + ":" + strconv.Itoa(pid)
}

// hostName is the host the process runs on: HOSTNAME, else the OS's name.
func hostName() string {
	if h := os.Getenv("HOSTNAME"); h != "" {
		return h
	}
	h, err := os.Hostname()
	if err != nil {
		return ""
	}
	return h
}
