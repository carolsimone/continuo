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
	rametrics "github.com/carolsimone/continuo/agent-remediation/adapters/metrics"
	"github.com/carolsimone/continuo/agent-remediation/adapters/packaging"
	"github.com/carolsimone/continuo/agent-remediation/adapters/postgres"
	rredis "github.com/carolsimone/continuo/agent-remediation/adapters/redis"
	"github.com/carolsimone/continuo/agent-remediation/adapters/releasehttp"
	"github.com/carolsimone/continuo/agent-remediation/adapters/repofs"
	"github.com/carolsimone/continuo/agent-remediation/adapters/s3"
	"github.com/carolsimone/continuo/agent-remediation/adapters/workflowfile"
	remediationv1 "github.com/carolsimone/continuo/agent-remediation/api/remediation/v1"
	"github.com/carolsimone/continuo/agent-remediation/config"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/llmcache"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/promptlog"
	"github.com/carolsimone/continuo/agent-remediation/service/proposals"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
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

// llmClientTimeout bounds a single LLM HTTP request, that is each model request
// a runner step makes. Without it the LLM path uses http.DefaultClient, which
// has NO timeout, so one hung request could block the worker's step forever.
const llmClientTimeout = 120 * time.Second

// consumerHandlerTimeout bounds each heal.triggered handler invocation
// with a context deadline, so a genuinely-hung handler eventually returns
// control to the read loop. The handler only records the trigger as a
// remediation, one database transaction, so the ceiling is generous.
const consumerHandlerTimeout = 5 * time.Minute

// dbPool bounds the Postgres pool when DB_MAX_OPEN_CONNS / DB_MAX_IDLE_CONNS
// are unset: two connections per stream consumer (a handler's transaction and
// a read outside it), plus the outbox relay, background loops and request
// handlers.
var dbPool = pkgconfig.PoolConfig{MaxOpenConns: 10, MaxIdleConns: 5}

func main() {
	// The logger writes at the level LOG_LEVEL names. An unknown level is
	// recorded on v, and the missing-configuration check below stops the
	// component once this logger can report it.
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
	metricsReg := pkgmetrics.New(config.ComponentName)

	// runConsumer starts a tracked stream consumer: a bounded handler deadline
	// so a hung handler (e.g. a stuck database call) eventually returns;
	// RegisterWorker before launch so a missing worker is observable;
	// WorkerExited when Start returns (on a permanent consumer-group bootstrap
	// error as well as on a clean shutdown); and a worker heartbeat probe so a
	// wedged-but-not-exited loop is caught.
	runConsumer := func(name string, consumer *pkgredis.StreamConsumer) {
		consumer.SetComponent(config.ComponentName)
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
	// false until the consumer, gRPC server and reconciler are running.
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
	// the role it uses: the evidence reader, the candidate-source reader and
	// the image-packages source read through it, the artifact writer writes
	// through it. Static S3 keys
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
	// call could block the worker's step indefinitely (and, with the worker
	// heartbeat feeding liveness, get the pod restarted underneath in-flight
	// work). A per-request timeout keeps each call bounded.
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
	// model call a step repeats (a retried step, a redriven trigger) reuses the
	// prior completion instead of re-paying the LLM call. A cache miss or error
	// falls through to the real provider, so the cache can never break the happy
	// path.
	cachedLLM := llmcache.New(
		loggedLLM,
		rredis.NewLLMResponseCache(rc, cfg.LLMCacheTTL),
		cfg.LLMModel,
		logger,
	)

	// One GitHub adapter instance serves every read-only port backed by the
	// GitHub API: source reads and repo-archive fetches for the steps, and
	// PR-status reads for the outcome reconciler. A request deadline keeps a
	// hung GitHub connection from stalling the callers that share this
	// adapter.
	gh := ragithub.NewSourceReader(cfg.GitHubBaseURL, cfg.GitHubToken, &http.Client{Timeout: 30 * time.Second})

	// Fix verification: a proposed fix's edits are packaged (an odcs-bundle.json
	// for a python project, a source overlay for a dbt one) and submitted to
	// release-controller as a verification run that carries the full
	// parse -> candidate-schema -> validation pipeline through to a terminal
	// passed/failed verdict without ever promoting.
	releaseGateway := releasehttp.NewGateway(cfg.ReleaseControllerURL, evidenceReader,
		&http.Client{Timeout: 30 * time.Second})
	// A python fix is packed with the package list of the rejected release's
	// image of its project, read from the release's contract bundle or from
	// the odcs-bundle.json the project's CI uploaded to the bucket.
	imagePackages := s3.NewImagePackagesSource(objectStore, releaseGateway, cfg.S3.Bucket)

	// The packager is resolved here rather than at first use: an image built
	// without the CLI cannot package any python fix, and failing at boot names
	// that once, instead of failing every remediation trigger forever.
	packager, err := packaging.NewCLIPackager()
	if err != nil {
		logger.Error("contract packager unavailable", "error", err)
		os.Exit(1)
	}

	// The proposal repository is bound to the DB rather than to a transaction:
	// the gRPC read path, the reconciler, and the steps' prior-attempt reads
	// all use it outside any unit of work.
	proposalRepo := postgres.NewProposalRepository(db, cfg.ProjectRepoPaths)
	contracts := repofs.NewLocator(logger)

	// The runner works a heal trigger as a remediation of committed steps. Its
	// worker, gate poller and sweeper carry each remediation to its end. Its
	// built-in files are checked against the steps the registry holds before
	// anything starts, and a refused file stops startup.
	newUoW := func() uow.UnitOfWork { return postgres.NewUnitOfWork(db, logger, cfg.ProjectRepoPaths) }
	remediationRepo := postgres.NewRemediationRepository(db)
	collab := runnerCollaborators(cfg, runnerAdapters{
		github:    gh,
		evidence:  evidenceReader,
		artifacts: artifactWriter,
		graph:     graphClient,
		bundle:    bundleReader,
		contracts: contracts,
		packager:  packager,
		images:    imagePackages,
		release:   releaseGateway,
		proposals: proposalRepo,
		tries:     remediationRepo,
	}, logger)
	install, err := installFiles(cfg.WorkflowsDir)
	if err != nil {
		logger.Error("the install's remediation files cannot be read", "error", err)
		os.Exit(1)
	}
	files, err := loadRunnerFiles(workflows.Builtin, install, collab, logger)
	if err != nil {
		logger.Error("the remediation files are refused", "error", err)
		os.Exit(1)
	}
	// The runner stamps a remediation on every attempt row it writes, so a row
	// in flight with none is driven by nothing. Until it is failed,
	// release-controller's retry gate and the release page read its round as
	// having a fix still open, which keeps "Try again" blocked. The sweep runs
	// before the runner's loops and the consumer start, so no new work begins
	// while such a row is open.
	if stop, code := sweepAtStartup(ctx, proposalRepo, logger); stop {
		os.Exit(code)
	}
	accept := startRunner(ctx, cfg, collab, files, runnerWiring{
		llm:     cachedLLM,
		newUoW:  newUoW,
		repo:    func() repository.RemediationRepository { return remediationRepo },
		owner:   leaseOwner(hostName(), os.Getpid()),
		live:    liveReg,
		logger:  logger,
		metrics: rametrics.NewRemediationMetrics(metricsReg),
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

	// Start the heal.triggered consumer in a goroutine; blocks until ctx
	// is cancelled. It hands each trigger to the runner, which records it as a
	// remediation.
	runConsumer("heal_triggered", rredis.NewHealTriggeredConsumer(rc, accept, logger))

	// Start the RemediationProposals gRPC server. The proposal service uses a
	// DB-bound (non-transactional) repository for reads and the UoW factory for
	// write operations, matching the consumer's wiring above.
	proposalSvc := proposals.New(proposals.Deps{
		Repo:             proposalRepo,
		NewUoW:           func() uow.UnitOfWork { return postgres.NewUnitOfWork(db, logger, cfg.ProjectRepoPaths) },
		Clock:            ports.SystemClock{},
		ProjectRepoPaths: cfg.ProjectRepoPaths,
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
		// edits, and ProjectRepoPaths splits them by owning project.
		Getter:           proposalRepo,
		Sources:          gh,
		Evidence:         evidenceReader,
		ProjectRepoPaths: cfg.ProjectRepoPaths,
	})
	go reconciler.Run(ctx)

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
// sweeper that ends one that can go no further.
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
	// metrics counts what the runner does. Nil counts nothing.
	metrics ports.RemediationMetrics
}

// loopLivenessSlack is added to the longest a loop can go between two beats
// before it is reported wedged.
const loopLivenessSlack = time.Minute

// loopBudgets are how long the worker and the poller may go without beating
// before they are reported wedged. A worker beats at every claim and every
// third of a lease while it drives, so it can be silent for one step and the
// park of a remediation whose drive failed. A poller beats at every pass,
// every remediation it polls and every third of a lease while a poll runs, so
// it can be silent for at most one poll of a gate and the wait between two
// passes, which VERIFICATION_POLL_INTERVAL sets.
func loopBudgets(cfg config.Config) (worker, poller time.Duration) {
	worker = runner.DefaultStepTimeout + runner.ParkDuration + loopLivenessSlack
	poller = cfg.VerificationPollInterval + runner.DefaultStepTimeout + loopLivenessSlack
	return worker, poller
}

// sweepAtStartup runs failLegacyInFlight and says whether the process must stop
// and with what exit code. A sweep that fails while ctx is done was cut short by
// a shutdown, which stops cleanly (code 0) as the other startup waits do,
// whatever error the driver reported for its cancelled statement. Any other
// failure of the sweep stops startup with code 1.
func sweepAtStartup(ctx context.Context, repo repository.ProposalRepository, logger *slog.Logger) (stop bool, code int) {
	err := failLegacyInFlight(ctx, repo, logger)
	switch {
	case err == nil:
		return false, 0
	case ctx.Err() != nil:
		logger.Info("shutdown requested during the startup sweep of attempt rows with no remediation")
		return true, 0
	default:
		logger.Error("failing the attempt rows in flight that belong to no remediation", "error", err)
		return true, 1
	}
}

// failLegacyInFlight fails every attempt row in flight that belongs to no
// remediation, which nothing drives since the runner stamps a remediation on
// every row it writes, and logs how many it moved: at Warn when there are any,
// at Info when there are none. It is idempotent, so every start runs it.
func failLegacyInFlight(ctx context.Context, repo repository.ProposalRepository, logger *slog.Logger) error {
	n, err := repo.FailLegacyInFlight(ctx, proposal.NoRemediationInFlightReason)
	if err != nil {
		return err
	}
	if n == 0 {
		logger.Info("no attempt row with no remediation was in flight", "count", n)
		return nil
	}
	logger.Warn("failed in-flight attempt rows that have no remediation", "count", n)
	return nil
}

// startRunner builds the runner, starts its three loops with spawn and returns
// the function that accepts a heal trigger as a remediation.
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
	r := runner.New(newRunnerDeps(cfg, collab, files, w))
	worker := runner.NewWorker(r, w.repo, w.owner, ports.SystemClock{}, w.logger)
	poller := runner.NewGatePoller(r, w.repo, w.owner, cfg.VerificationPollInterval, worker.Wake, ports.SystemClock{}, w.logger)
	sweeper := runner.NewSweeper(w.newUoW, w.repo, w.owner+"/sweeper", ports.SystemClock{}, w.logger, w.metrics)
	workerBudget, pollerBudget := loopBudgets(cfg)
	worker.SetHeartbeat(registerLoop(w.live, "remediation_worker", workerBudget))
	poller.SetHeartbeat(registerLoop(w.live, "remediation_gate_poller", pollerBudget))
	w.logger.Info("remediation runner built", "owner", w.owner)
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
		ports.CurrentContractReader
	}
	bundle    ports.CandidateSourceReader
	contracts interface {
		ports.ContractLocator
		ports.ContractInspector
	}
	packager ports.ContractPackager
	images   ports.ImagePackagesSource
	release  interface {
		ports.ReleaseReader
		ports.VerificationPipeline
	}
	proposals repository.AttemptLister
	tries     repository.TryLister
}

// runnerCollaborators is the ports the steps are built from: the adapters main
// built, the patcher that writes a draft over a checkout, the
// system clock, and what the install configures (the repository paths of the
// projects, the warehouse's SQL dialect and how long a verification may run).
func runnerCollaborators(cfg config.Config, a runnerAdapters, logger *slog.Logger) runner.Collaborators {
	return runner.Collaborators{
		Source:              a.github,
		Evidence:            a.evidence,
		Artifacts:           a.artifacts,
		Logger:              logger,
		ProjectRepoPaths:    cfg.ProjectRepoPaths,
		Locator:             a.graph,
		Upstream:            a.graph,
		Versions:            a.graph,
		Precedents:          a.graph,
		CandidateSource:     a.bundle,
		Archive:             a.github,
		Patcher:             repofs.CheckoutPatcher{},
		ContractLocator:     a.contracts,
		ModelReferences:     repofs.ModelReferenceFinder{},
		ContractInspector:   a.contracts,
		Packager:            a.packager,
		ImagePackages:       a.images,
		CurrentContracts:    a.graph,
		Releases:            a.release,
		PriorAttempts:       a.proposals,
		Tries:               a.tries,
		SQLDialect:          cfg.SQLDialect,
		Pipeline:            a.release,
		Clock:               ports.SystemClock{},
		VerificationTimeout: cfg.VerificationTimeout,
	}
}

// newRunnerDeps is what the runner works with. The registry is built from
// collab, so the gate's budget is the one collab carries, and the verifying
// try's deadline is built on the same VERIFICATION_TIMEOUT.
func newRunnerDeps(cfg config.Config, collab runner.Collaborators, files workflow.Set, w runnerWiring) runner.Deps {
	return runner.Deps{
		NewUoW:              w.newUoW,
		Registry:            runner.NewRegistry(collab),
		Files:               files,
		LLM:                 w.llm,
		Collab:              collab,
		Clock:               ports.SystemClock{},
		Logger:              w.logger,
		Maintenance:         cfg.Maintenance,
		Owner:               w.owner,
		VerificationTimeout: cfg.VerificationTimeout,
		Metrics:             w.metrics,
	}
}

// installFiles is the folder dir, where an install keeps its own remediation
// files, as a file system. An empty dir, or a folder that does not exist, means
// the install has none and gives a nil file system. A path that exists but is
// not a folder, or whose entries cannot be listed, is an error, so startup
// stops rather than running without the files the install put there.
func installFiles(dir string) (fs.FS, error) {
	return installFilesListing(dir, os.ReadDir)
}

// installFilesListing is installFiles with the function that lists the
// folder's entries given. A listing that fails is a refusal whoever runs the
// process, whatever the folder's permissions allow: the seam lets a test
// make the listing fail without depending on the user's rights, which a
// root user is never refused.
func installFilesListing(dir string, list func(name string) ([]fs.DirEntry, error)) (fs.FS, error) {
	if dir == "" {
		return nil, nil
	}
	info, err := os.Stat(dir)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("remediation files folder %s: %w", dir, err)
	case !info.IsDir():
		return nil, fmt.Errorf("remediation files folder %s is not a directory", dir)
	}
	if _, err := list(dir); err != nil {
		return nil, fmt.Errorf("remediation files folder %s: %w", dir, err)
	}
	return os.DirFS(dir), nil
}

// loadRunnerFiles loads the built-in remediation files and, over them, the
// install's (install is nil when it has none), checks every flow and type of
// them against the steps collab's registry holds, and checks that they define
// the flow every heal trigger runs. Every refusal is logged on its own line and
// the error that joins them is returned; a warning is logged and does not stop
// startup. What the install's files replace, and the files copied from a
// built-in that has changed since, are logged at Warn before the files are
// checked, so a file that check refuses still shows what it was meant to
// replace. Each flow no heal trigger runs is logged at Warn with the check's
// warnings. Files without the trigger's flow stop startup, since every
// remediation the runner accepted would follow a flow no file defines.
func loadRunnerFiles(builtin, install fs.FS, collab runner.Collaborators, logger *slog.Logger) (workflow.Set, error) {
	set, loaded, err := workflowfile.LoadInstall(builtin, install)
	if err != nil {
		logRefusals(logger, err)
		return workflow.Set{}, err
	}
	for _, n := range loaded.Notes {
		logger.Warn("remediation file", "warning", n)
	}
	warnings, err := runner.Validate(set, runner.NewRegistry(collab))
	warnings = append(warnings, runner.FlowsNoTriggerRuns(set)...)
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
