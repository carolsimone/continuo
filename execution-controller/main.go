package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/carolsimone/continuo/execution-controller/adapters/commandcfg"
	"github.com/carolsimone/continuo/execution-controller/adapters/http"
	"github.com/carolsimone/continuo/execution-controller/adapters/k8s"
	"github.com/carolsimone/continuo/execution-controller/adapters/k8s/watch"
	ecmetrics "github.com/carolsimone/continuo/execution-controller/adapters/metrics"
	"github.com/carolsimone/continuo/execution-controller/adapters/postgres"
	"github.com/carolsimone/continuo/execution-controller/adapters/publisher"
	"github.com/carolsimone/continuo/execution-controller/adapters/redis"
	s3adapter "github.com/carolsimone/continuo/execution-controller/adapters/s3"
	"github.com/carolsimone/continuo/execution-controller/config"
	"github.com/carolsimone/continuo/execution-controller/domain/repository"
	"github.com/carolsimone/continuo/execution-controller/service/deployer"
	"github.com/carolsimone/continuo/execution-controller/service/handlers"
	"github.com/carolsimone/continuo/execution-controller/service/observer"
	"github.com/carolsimone/continuo/execution-controller/service/outcomes"
	"github.com/carolsimone/continuo/execution-controller/service/uow"
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
	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
)

// consumerHandlerTimeout bounds each message handler invocation, so a hung
// handler eventually returns control to the read loop. Handlers do short DB
// writes and K8s API calls; 60s far exceeds any legitimate invocation. The
// candidate-schema handlers are the exception, see schemaOpHandlerTimeout.
const consumerHandlerTimeout = 60 * time.Second

// schemaOpHandlerTimeout is the handler budget for consumers that block on a
// candidate-schema engine-image Job: the k8s adapter waits up to
// k8s.SchemaOpJobTimeout for the Job to terminate, so the message deadline must
// sit above that wait.
const schemaOpHandlerTimeout = k8s.SchemaOpJobTimeout + time.Minute

// cancelledRunsSweepInterval is how often the fences of runs cancelled longer
// ago than the replay horizon are deleted.
const cancelledRunsSweepInterval = time.Hour

// dbPool bounds the Postgres pool when DB_MAX_OPEN_CONNS / DB_MAX_IDLE_CONNS
// are unset: two connections per stream consumer (a handler's transaction and
// a read outside it), plus the outbox relay, background loops and request
// handlers.
var dbPool = pkgconfig.PoolConfig{MaxOpenConns: 30, MaxIdleConns: 5}

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
	logger.Info("Starting execution-controller service")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lifecycleManager := lifecycle.NewApplicationLifecycle(logger)
	lifecycleManager.SetupSignalHandlers(cancel, cfg.ShutdownGrace)

	// /ready = workers + heartbeats + dependency probes; /livez = workers +
	// heartbeats only, so a backing-store outage stops traffic without
	// restarting a pod whose consumers are already retrying.
	liveReg := liveness.NewRegistry()
	metricsReg := pkgmetrics.New(config.ServiceName)

	startConsumer := func(name string, consumer *pkgredis.StreamConsumer, handlerTimeout time.Duration) {
		consumer.SetService(config.ServiceName)
		consumer.SetObserver(metricsReg.Consumers())
		consumer.SetHandlerTimeout(handlerTimeout)
		liveReg.RegisterWorker(name)
		liveReg.AddWorkerProbe(name+"_heartbeat", 10*time.Second, func(context.Context) error {
			return consumer.Healthy(consumer.HeartbeatBudget())
		})
		lifecycleManager.Go(func() {
			err := consumer.Start(ctx)
			liveReg.WorkerExited(name, err)
			if err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("Consumer exited", "consumer", name, "error", err)
			}
		})
	}
	runConsumer := func(name string, consumer *pkgredis.StreamConsumer) {
		startConsumer(name, consumer, consumerHandlerTimeout)
	}
	runSchemaOpConsumer := func(name string, consumer *pkgredis.StreamConsumer) {
		startConsumer(name, consumer, schemaOpHandlerTimeout)
	}
	// runWorker runs a background loop as a tracked goroutine registered with
	// the liveness registry; a clean shutdown is not an error.
	runWorker := func(name string, run func(context.Context) error) {
		liveReg.RegisterWorker(name)
		lifecycleManager.Go(func() {
			err := run(ctx)
			if errors.Is(err, context.Canceled) {
				err = nil
			}
			liveReg.WorkerExited(name, err)
			if err != nil {
				logger.Error("Worker exited", "worker", name, "error", err)
			}
		})
	}

	// The health server comes up before any dependency is reachable so /livez
	// keeps answering while the boot waits for Redis; the startup gate holds
	// /ready false until every consumer below is running.
	startup := liveReg.AddStartupGate()
	healthServer := http.NewHealthServer(cfg.HTTPPort, liveReg, logger)
	go func() {
		if err := healthServer.Start(); err != nil {
			logger.Error("Health server error", "error", err)
		}
	}()

	// The Prometheus listener comes up with the health server so a dependency
	// outage stays observable. Serve runs in a plain goroutine; the lifecycle
	// manager stops it at shutdown.
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
	lifecycleManager.RegisterShutdownHandler(metricsServer.Shutdown)

	// ---- infrastructure ----

	pgDB, err := pkgdb.Open(ctx, cfg.Postgres, dbPool)
	if err != nil {
		logger.Error("Failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Closing PostgreSQL connection")
		return pgDB.Close()
	})
	liveReg.AddProbe("postgres", 5*time.Second, func(ctx context.Context) error { return pgDB.PingContext(ctx) })
	metricsReg.WatchDB(pgDB.DB, cfg.Postgres.DB)

	// execution-controller reads columns an execution database not yet
	// migrated lacks, so it refuses to start until the migration has applied;
	// the pod restarts until it has. Nothing else has started yet.
	if err := handlers.CheckSchema(ctx, postgres.NewSchemaColumns(pgDB)); err != nil {
		logger.Error("Refusing to start: the execution database is not migrated", "error", err)
		os.Exit(1)
	}

	// Redis is a separate workload that may still be starting (or its Service
	// not yet resolvable) when this process boots, so wait for it with backoff
	// instead of exiting on the first refused dial.
	redisClient := goredis.NewClient(&goredis.Options{Addr: cfg.Redis.Addr(), Password: cfg.Redis.Password})
	err = pkgredis.WaitForRedis(ctx, redisClient, pkgredis.DefaultStartupPolicy(), logger)
	if errors.Is(err, context.Canceled) {
		logger.Info("Shutdown requested while waiting for Redis")
		os.Exit(0)
	}
	if err != nil {
		logger.Error("Failed to connect to Redis", "addr", cfg.Redis.Addr(), "error", err)
		os.Exit(1)
	}
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Closing Redis connection")
		return redisClient.Close()
	})
	liveReg.AddProbe("redis", 5*time.Second, func(ctx context.Context) error { return redisClient.Ping(ctx).Err() })
	metricsReg.WatchRedis(redisClient)

	// dbt command dialect, resolved per service at Job-build time. A missing
	// file means built-in plain-dbt commands; an invalid file is fatal.
	cmdResolver, err := commandcfg.Load(os.Getenv("DBT_COMMANDS_CONFIG_PATH"), logger)
	if err != nil {
		logger.Error("Invalid dbt commands config", "error", err)
		os.Exit(1)
	}
	k8sClient, err := k8s.NewK8sClient(logger, cmdResolver, k8s.RateLimit{QPS: cfg.K8sClientQPS, Burst: cfg.K8sClientBurst})
	if err != nil {
		logger.Error("Failed to create K8s client", "error", err)
		os.Exit(1)
	}

	// One watch on the Jobs and pods labelled app=dbt-job queues a Job's name
	// whenever either changes; the observer's workers take names off the
	// queue. It starts now so its caches fill while the rest boots.
	observations := watch.NewQueue()
	jobWatch, err := watch.New(k8sClient.Clientset(), cfg.K8sNamespace, observations)
	if err != nil {
		logger.Error("Failed to build the Job watch", "error", err)
		os.Exit(1)
	}
	jobWatch.Start(ctx)

	// The candidate-schema lifecycle never connects to the warehouse: it runs a
	// one-shot engine-image Job (ensure_schema/drop_schema) and blocks on it.
	candidateSchemaCreator := k8s.NewCandidateSchemaCreator(k8sClient, cfg.K8sNamespace, logger)
	candidateSchemaCleaner := k8s.NewCandidateSchemaCleaner(k8sClient, cfg.K8sNamespace, logger)

	// The log uploader writes through the shared object-storage client as its
	// objectstore.Writer role. Static S3 keys when both are set; otherwise the
	// SDK's default credential chain (IAM role, workload identity).
	objectStore, err := objectstore.New(ctx, objectstore.ConfigFromS3(cfg.S3))
	if err != nil {
		logger.Error("Failed to build the object-storage client", "error", err)
		os.Exit(1)
	}
	logUploader := s3adapter.NewLogUploader(objectStore)

	cancelledRunsRepo := postgres.NewCancelledRunsRepository(pgDB)

	// A fresh unit of work per inbound message keeps concurrent handlers isolated.
	uowFactory := func() uow.UnitOfWork { return postgres.NewPostgresUnitOfWork(pgDB, logger) }

	// The observer's series: attempts awaiting a pod stop, attempts reported
	// stopped, and Jobs deleted because no row deploys them.
	observerMetrics := ecmetrics.NewObserver(metricsReg)

	// ---- handlers and bindings ----

	// The Job control deletes the Jobs of stopped attempts and reads the logs
	// of finished ones.
	jobControl := k8s.NewJobControl(k8sClient, cfg.K8sNamespace)

	taskDispatchHandler := handlers.NewTaskDispatchHandler(jobWatch, jobWatch, observerMetrics, logger)
	runCancelledHandler := handlers.NewRunCancelledHandler(jobWatch, jobControl, observerMetrics, logger)
	validationReqHandler := handlers.NewValidationRequestedHandler(logger)
	seedBuildReqHandler := handlers.NewSeedBuildRequestedHandler(logger)
	compileReqHandler := handlers.NewCompileRequestedHandler(logger)

	newConsumer := func(stream, group string, binding pkgredis.MessageHandler) *pkgredis.StreamConsumer {
		c := pkgredis.NewStreamConsumer(redisClient, stream, group, binding, logger)
		logger.Info("consumer initialized", "stream", stream, "group", group)
		return c
	}

	taskDispatchConsumer := newConsumer(streams.TaskDispatchV1, streams.ExecutionControllerTaskDispatch,
		redis.NewTaskDispatchBinding(uowFactory, taskDispatchHandler, logger))
	runCancelledConsumer := newConsumer(streams.RunCancelledV1, streams.ExecutionControllerRunCancelled,
		redis.NewRunCancelledBinding(uowFactory, runCancelledHandler, logger))
	validationReqConsumer := newConsumer(streams.ValidationRequestedV1, streams.ExecutionControllerValidationRequested,
		redis.NewValidationRequestedBinding(uowFactory, validationReqHandler, candidateSchemaCreator, logger))
	seedBuildReqConsumer := newConsumer(streams.SeedBuildRequestedV1, streams.ExecutionControllerSeedBuildRequested,
		redis.NewSeedBuildRequestedBinding(uowFactory, seedBuildReqHandler, candidateSchemaCreator, logger))
	compileReqConsumer := newConsumer(streams.CompileRequestedV1, streams.ExecutionControllerCompileRequested,
		redis.NewCompileRequestedBinding(uowFactory, compileReqHandler, logger))
	validationResultTeardownConsumer := newConsumer(streams.ValidationResultV1, streams.ExecutionControllerValidationResultTeardown,
		redis.NewValidationResultTeardownBinding(candidateSchemaCleaner, logger))
	pipelineRunFinishedTeardownConsumer := newConsumer(streams.PipelineRunFinishedV1, streams.ExecutionControllerPipelineRunFinished,
		redis.NewPipelineRunFinishedTeardownBinding(candidateSchemaCleaner, logger))
	releasePromotedTeardownConsumer := newConsumer(streams.ReleasePromotedV2, streams.ExecutionControllerReleasePromoted,
		redis.NewReleasePromotedTeardownBinding(candidateSchemaCleaner, logger))
	releaseRejectedTeardownConsumer := newConsumer(streams.ReleaseRejectedV1, streams.ExecutionControllerReleaseRejected,
		redis.NewReleaseRejectedTeardownBinding(candidateSchemaCleaner, logger))

	// ---- background workers ----

	// Task status reports and release-leg results flow through this
	// outbox; the relay wakes on the notification a committed insert sends, so
	// a result reaches its stream without waiting for a poll.
	outboxWaker, err := pkgoutbox.NewPostgresWaker(ctx, cfg.Postgres.DSN(), postgres.OutboxTable, logger)
	if errors.Is(err, context.Canceled) {
		logger.Info("Shutdown requested while waiting for the outbox listener")
		os.Exit(0)
	}
	if err != nil {
		logger.Error("Failed to listen for outbox notifications", "error", err)
		os.Exit(1)
	}
	// Closing the listener can wait on a connection attempt in progress, so
	// the handler stops waiting at the shutdown deadline.
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error { return outboxWaker.CloseContext(ctx) })
	outboxMetrics := pkgoutbox.NewMetrics(metricsReg)
	outboxProcessor := pkgoutbox.NewProcessor(pgDB, postgres.OutboxTable, publisher.NewOutboxPublisher(redisClient, logger), nil, logger,
		pkgoutbox.ProcessorConfig{Tick: pkgoutbox.FallbackTick, BatchSize: 100, Waker: outboxWaker, Observer: outboxMetrics})
	outboxMetrics.Watch(outboxProcessor)
	runWorker("outbox_processor", outboxProcessor.Run)

	// The dispatcher wakes on the notification the deployments table's
	// triggers send when a deployment is accepted or a slot is released, so
	// queued work starts without waiting for the 5 s poll.
	dispatcherWaker, err := pkgoutbox.NewPostgresWaker(ctx, cfg.Postgres.DSN(), postgres.DeploymentsChannel, logger)
	if errors.Is(err, context.Canceled) {
		logger.Info("Shutdown requested while waiting for the dispatcher listener")
		os.Exit(0)
	}
	if err != nil {
		logger.Error("Failed to listen for deployment notifications", "error", err)
		os.Exit(1)
	}
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error { return dispatcherWaker.CloseContext(ctx) })

	newDeploymentRepo := func(exec pkgoutbox.Executor) repository.DeploymentRepository {
		return postgres.NewDeploymentsRepository(exec, logger)
	}
	newAdmissionRepo := func(exec pkgoutbox.Executor) repository.AdmissionRepository {
		return postgres.NewAdmissionRepository(exec, logger)
	}
	// A task's Job runs up to the start timeout plus the task's own timeout,
	// a release leg's up to the start timeout plus the install's task timeout.
	k8sDeployer := k8s.NewDeployer(k8sClient, cfg.K8sNamespace, cfg.JobStartTimeout, cfg.TaskTimeout)

	deployDispatcher := deployer.NewDispatcher(
		pgDB, k8sDeployer, jobWatch,
		newDeploymentRepo,
		func(exec pkgoutbox.Executor) repository.ValidationAggregateRepository {
			return postgres.NewValidationAggregateRepository(exec)
		},
		newAdmissionRepo,
		func(exec pkgoutbox.Executor) repository.CancelledRunsRepository {
			return postgres.NewCancelledRunsRepository(exec)
		},
		cfg.MaxConcurrentJobs, logger,
		deployer.DispatcherConfig{Tick: 5 * time.Second, BatchSize: 50, Waker: dispatcherWaker,
			ErrorMessageMaxLen: cfg.ErrorMessageMaxLength, StopMetrics: observerMetrics, Terminator: jobControl},
	)

	// Nothing creates or observes a Job until both caches have listed every
	// Job and pod once: until then an absence from the cache means nothing.
	if !jobWatch.WaitForCacheSync(ctx) {
		logger.Info("Shutdown requested while the Job watch synced")
		os.Exit(0)
	}
	runWorker("deploy_dispatcher", deployDispatcher.Run)

	// The observer follows every created Job to its end: it records what each
	// attempt shows, ends one that cannot start, and reports its result once
	// its pods are finished or gone. Every minute it queues every started
	// deployment again.
	attemptObserver := observer.New(observer.Config{
		StartTimeout:       cfg.JobStartTimeout,
		ErrorMessageMaxLen: cfg.ErrorMessageMaxLength,
		LogTailLines:       int64(cfg.LogTailLines),
	}, observer.Deps{
		NewUoW:      uowFactory,
		Observed:    postgres.NewObservedDeployments(pgDB, logger),
		Cancelled:   cancelledRunsRepo,
		Cache:       jobWatch,
		Lookup:      jobWatch,
		Terminator:  jobControl,
		Logs:        jobControl,
		Uploader:    logUploader,
		Queue:       observations,
		Outcomes:    outcomes.NewRecorder(logger),
		Metrics:     observerMetrics,
		StopMetrics: observerMetrics,
		Logger:      logger,
	})
	runWorker("observer", attemptObserver.Run)

	// Every minute, return to the queue the reservations a crashed launcher
	// left behind.
	admissionReconciler := deployer.NewReconciler(pgDB, newAdmissionRepo, logger, deployer.ReconcilerConfig{})
	runWorker("admission_reconciler", admissionReconciler.Run)

	// Every hour, the fences of runs cancelled longer ago than the replay
	// horizon are deleted: no message of such a run is replayed any more.
	runWorker("cancelled_runs_sweeper", func(ctx context.Context) error {
		ticker := time.NewTicker(cancelledRunsSweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if n, err := cancelledRunsRepo.DeleteOlderThan(ctx, model.ReplayHorizon); err != nil {
					logger.Error("cancelled_runs sweep failed", "error", err)
				} else if n > 0 {
					logger.Info("Swept expired cancelled_runs rows", "count", n)
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	// Retention sweeper — keeps the two unbounded-growth tables in check:
	// processed execution_outbox rows and message_processing dedup rows (the
	// latter retains a full payload per consumed message). Processed outbox rows
	// are kept RETENTION_DAYS; dedup rows, whatever their state, are kept at
	// least 30 days (the replay horizon), longer when RETENTION_DAYS is longer. A
	// dedup row an outbox row still references is kept until that outbox row is
	// pruned. Both are pruned on the same timer using DB-clock cutoffs.
	mpPruner := pkgmessageprocessing.NewPruner(pgDB, postgres.OutboxTable, logger)
	retentionSweeper := pkgretention.NewSweeper(
		[]pkgretention.Target{
			pkgoutbox.OutboxRetentionTarget(pgDB, postgres.OutboxTable, logger),
			{
				Name:         "message_processing",
				Prune:        mpPruner.DeleteOlderThan,
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

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error { return healthServer.Shutdown(ctx) })

	// ---- consumers ----

	runConsumer("task_dispatch", taskDispatchConsumer)
	runConsumer("run_cancelled", runCancelledConsumer)
	runSchemaOpConsumer("validation_requested", validationReqConsumer)
	runSchemaOpConsumer("seed_build_requested", seedBuildReqConsumer)
	runConsumer("compile_requested", compileReqConsumer)
	runSchemaOpConsumer("validation_result_teardown", validationResultTeardownConsumer)
	runSchemaOpConsumer("pipeline_run_finished_teardown", pipelineRunFinishedTeardownConsumer)
	runSchemaOpConsumer("release_promoted_teardown", releasePromotedTeardownConsumer)
	runSchemaOpConsumer("release_rejected_teardown", releaseRejectedTeardownConsumer)

	// Everything is initialised and running: readiness may now turn true.
	startup.Done()

	<-lifecycleManager.Done()
	logger.Info("execution-controller service stopped")
}
