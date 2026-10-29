package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/service/runsettle"

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
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/run-controller/adapters/cron"
	"github.com/carolsimone/continuo/run-controller/adapters/http"
	statemetrics "github.com/carolsimone/continuo/run-controller/adapters/metrics"
	"github.com/carolsimone/continuo/run-controller/adapters/postgres"
	statepublisher "github.com/carolsimone/continuo/run-controller/adapters/publisher"
	"github.com/carolsimone/continuo/run-controller/adapters/redis"
	s3adapter "github.com/carolsimone/continuo/run-controller/adapters/s3"
	"github.com/carolsimone/continuo/run-controller/config"
	grpcserver "github.com/carolsimone/continuo/run-controller/internal/grpc"
	"github.com/carolsimone/continuo/run-controller/internal/grpc/runservice"
	"github.com/carolsimone/continuo/run-controller/service/attemptreconcile"
	"github.com/carolsimone/continuo/run-controller/service/commands"
	"github.com/carolsimone/continuo/run-controller/service/cronloop"
	svchandlers "github.com/carolsimone/continuo/run-controller/service/handlers"
	"github.com/carolsimone/continuo/run-controller/service/leasewaker"
	ports "github.com/carolsimone/continuo/run-controller/service/ports"
	"github.com/carolsimone/continuo/run-controller/service/runreads"
	"github.com/carolsimone/continuo/run-controller/service/runstart"
	"github.com/carolsimone/continuo/run-controller/service/schedules"
	"github.com/carolsimone/continuo/run-controller/service/seedreconcile"
	"github.com/carolsimone/continuo/run-controller/service/topologyretention"
	"github.com/carolsimone/continuo/run-controller/service/uow"
	goredis "github.com/redis/go-redis/v9"
)

// outboxHeartbeatStale is the liveness budget for the outbox processor's Run
// loop. An idle loop turns at least once per pkgoutbox.FallbackTick (5s), so 60s
// is comfortably above it: a wedged (not exited) processor trips within a
// minute, while an idle-but-live one never does.
const outboxHeartbeatStale = 60 * time.Second

// releasePromotedHandlerTimeout bounds one release.promoted:v2 delivery: the
// artifact read from object storage, then the load of a whole topology, every
// node and edge, in one transaction.
const releasePromotedHandlerTimeout = 5 * time.Minute

// seedReconcileInterval is how often the periodic seed reconcile runs.
const seedReconcileInterval = 60 * time.Second

// attemptReconcileInterval is how often the attempt reconcile passes.
const attemptReconcileInterval = 60 * time.Second

// leaseWakerInterval is how often the lease waker passes when no release of a
// node lease wakes it sooner.
const leaseWakerInterval = 60 * time.Second

// cronLoopTick is the longest the cron loop sleeps between passes when no
// schedule's next fire, and no change to the schedules, wakes it sooner.
const cronLoopTick = 60 * time.Second

// dbPool bounds the Postgres pool when DB_MAX_OPEN_CONNS / DB_MAX_IDLE_CONNS
// are unset: two connections per stream consumer (a handler's transaction and
// a read outside it), plus the outbox relay, background loops and request
// handlers.
var dbPool = pkgconfig.PoolConfig{MaxOpenConns: 20, MaxIdleConns: 10}

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

	logger.Info("Starting run-controller service")
	if cfg.Maintenance {
		logger.Warn("Maintenance mode is on: cron fires are skipped and RPCs that start runs are refused")
	}
	if len(cfg.IgnoredPoolKeys) > 0 {
		logger.Warn("Ignoring Postgres pool settings run-controller does not read; set DB_MAX_OPEN_CONNS and DB_MAX_IDLE_CONNS instead",
			"ignored", strings.Join(cfg.IgnoredPoolKeys, ", "))
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize lifecycle manager and liveness registry. The lifecycle manager
	// drives the ordered graceful shutdown (stop intake → drain → close infra);
	// the registry tracks background workers and feeds the /ready probe.
	lifecycleManager := lifecycle.NewApplicationLifecycle(logger)
	lifecycleManager.SetupSignalHandlers(cancel, cfg.ShutdownGrace)

	liveReg := liveness.NewRegistry()
	metricsReg := pkgmetrics.New(config.ServiceName)

	// runConsumer starts a tracked stream consumer. Its goroutine is tracked by
	// the lifecycle WaitGroup so shutdown drains in-flight handler invocations
	// before infra is closed. A non-nil return (a genuine exit rather than a
	// clean ctx-cancel stop) flips both readiness and liveness so the unhealthy
	// pod is restarted; a worker heartbeat probe also catches a consumer whose
	// read loop has gone wedged without exiting.
	runConsumer := func(name string, consumer *pkgredis.StreamConsumer) {
		consumer.SetService(config.ServiceName)
		consumer.SetObserver(metricsReg.Consumers())
		liveReg.RegisterWorker(name)
		liveReg.AddWorkerProbe(name+"_heartbeat", 10*time.Second, func(context.Context) error {
			return consumer.Healthy(consumer.HeartbeatBudget())
		})
		lifecycleManager.Go(func() {
			err := consumer.Start(ctx)
			liveReg.WorkerExited(name, err)
			if err != nil {
				logger.Error("Consumer exited", "consumer", name, "error", err)
			}
		})
	}

	// Start HTTP health server before any dependency is reachable. /health is a
	// plain process-up probe; /ready and /livez are both backed by the liveness
	// registry but answer different questions — /ready also fails on a
	// dependency outage (stops traffic), /livez fails only on a dead or wedged
	// consumer (restarts the pod). Serving /livez from the start keeps the
	// liveness probe answering while the boot waits for Redis; the startup gate
	// holds /ready false until every consumer and server below is running.
	startup := liveReg.AddStartupGate()
	healthServer := http.NewServer(cfg.HealthPort, liveReg, logger)
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

	// Initialize PostgreSQL connection
	db, err := pkgdb.Open(ctx, cfg.Postgres, dbPool)
	if err != nil {
		logger.Error("Failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	logger.Info("PostgreSQL connection established")
	metricsReg.WatchDB(db.DB, cfg.Postgres.DB)

	// Register database cleanup
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Closing database connection")
		return db.Close()
	})

	// run-controller reads columns a run-controller database not yet migrated lacks, so it
	// refuses to start until the migration has applied; the pod restarts until
	// it has. Nothing else has started yet.
	if err := svchandlers.CheckSchema(ctx, postgres.NewSchemaColumns(db)); err != nil {
		logger.Error("Refusing to start: the run-controller database is not migrated", "error", err)
		os.Exit(1)
	}

	// Initialize repositories
	runRepo := postgres.NewRunRepository(db, logger)
	taskCollection := postgres.NewTaskCollection(db, logger)
	taskAttemptReadRepo := postgres.NewTaskAttemptReadRepository(db)

	// Initialize Redis client
	redisClient := goredis.NewClient(&goredis.Options{
		Addr:     cfg.Redis.Addr(),
		Password: cfg.Redis.Password,
	})

	// Redis is a separate workload that may still be starting (or its Service
	// not yet resolvable) when this process boots, so wait for it with backoff
	// instead of exiting on the first refused dial.
	err = pkgredis.WaitForRedis(ctx, redisClient, pkgredis.DefaultStartupPolicy(), logger)
	if errors.Is(err, context.Canceled) {
		logger.Info("Shutdown requested while waiting for Redis")
		os.Exit(0)
	}
	if err != nil {
		logger.Error("Failed to connect to Redis", "addr", cfg.Redis.Addr(), "error", err)
		os.Exit(1)
	}
	logger.Info("Redis connection established")
	metricsReg.WatchRedis(redisClient)

	// Register Redis client cleanup
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Closing Redis client")
		return redisClient.Close()
	})

	// Cached dependency probes feed /ready. They run at most once per TTL so the
	// readiness endpoint stays cheap under Kubernetes probe traffic; a failed
	// ping flips readiness until the dependency recovers.
	liveReg.AddProbe("redis", 5*time.Second, func(ctx context.Context) error {
		return redisClient.Ping(ctx).Err()
	})
	liveReg.AddProbe("postgres", 5*time.Second, func(ctx context.Context) error {
		return db.PingContext(ctx)
	})

	// Start outbox processor backed by pkg/outbox. The publisher XADDs each
	// entry's JSONB payload to its stream. Nested non-scalar fields are
	// re-encoded to JSON strings so Redis receives only plain scalars. The
	// relay wakes on the notification a committed insert sends.
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
	outboxPub := statepublisher.NewOutboxPublisher(redisClient, logger)
	outboxProc := pkgoutbox.NewProcessor(
		db,
		postgres.OutboxTable,
		outboxPub,
		nil, // no terminal-failure hook for run-controller
		logger,
		pkgoutbox.ProcessorConfig{Tick: pkgoutbox.FallbackTick, BatchSize: 100, Waker: outboxWaker, Observer: metricsReg.Outbox()},
	)
	metricsReg.WatchOutbox(outboxProc)
	liveReg.RegisterWorker("outbox_processor")
	liveReg.AddWorkerProbe("outbox_processor_heartbeat", 10*time.Second, func(context.Context) error {
		return outboxProc.Healthy(outboxHeartbeatStale)
	})
	lifecycleManager.Go(func() {
		err := outboxProc.Run(ctx)
		if errors.Is(err, context.Canceled) {
			err = nil // clean stop on shutdown
		}
		liveReg.WorkerExited("outbox_processor", err)
		if err != nil {
			logger.Error("Outbox processor exited", "error", err)
		}
	})

	clk := ports.SystemClock{}

	// The parser rebuilds each stored schedule's expression from its cron and
	// timezone. It is built once, so a failure to build it stops the service
	// before any consumer starts.
	cronParser, err := cron.NewParser()
	if err != nil {
		logger.Error("Failed to build the cron parser", "error", err)
		os.Exit(1)
	}

	// UoW factory shared by every stream binding below. Each invocation
	// returns a fresh PostgresUnitOfWork over the same *sqlx.DB, so concurrent
	// message handlers do not share transaction state.
	// The UoW builds tx-bound aggregate adapters (Run, Outbox, TaskCollection,
	// Schedules, LiveScheduleModels) per accessor call.
	uowFactory := func() uow.UnitOfWork {
		return postgres.NewPostgresUnitOfWork(db, clk, logger, cronParser)
	}

	// A run whose tasks run-controller did not plan cannot move on: no status update
	// can dispatch or skip its tasks. Every such run, of any tenant, is
	// cancelled under its own tenant before any consumer starts, so its Jobs
	// are suppressed and its schedule is free to run again.
	settled, err := runsettle.Settle(ctx, uowFactory, logger)
	if err != nil {
		logger.Error("Failed to cancel the active runs run-controller did not plan", "error", err)
		os.Exit(1)
	}
	logger.Info("Active runs run-controller did not plan cancelled", "runs", settled)

	// Retention sweeper — keeps the unbounded-growth tables in check:
	// processed state_outbox rows, message_processing dedup rows (which retain
	// a full payload per consumed message) and stored topologies. Processed
	// outbox rows are kept RETENTION_DAYS; dedup rows, whatever their state,
	// are kept at least 30 days (the replay horizon), longer when
	// RETENTION_DAYS is longer; a promoted release is kept RETENTION_DAYS after
	// it was loaded, and for as long as it is live or a pending or running run
	// names it; a command request, like a dedup row, is kept at least the
	// replay horizon, the longest a caller can still retry, longer when
	// RETENTION_DAYS is longer. All are pruned on the same timer.
	mpPruner := pkgmessageprocessing.NewPruner(db, postgres.OutboxTable, logger)
	topologyPruner := topologyretention.NewPruner(uowFactory, logger)
	commandRequestPruner := commands.NewPruner(uowFactory)
	retentionSweeper := pkgoutbox.NewRetentionSweeper(
		[]pkgoutbox.RetentionTarget{
			pkgoutbox.OutboxRetentionTarget(db, postgres.OutboxTable, logger),
			{
				Name:         "message_processing",
				Prune:        mpPruner.DeleteOlderThan,
				MinRetention: model.ReplayHorizon,
			},
			{
				Name:  "topology_releases",
				Prune: topologyPruner.Prune,
			},
			{
				Name:         "command_requests",
				Prune:        commandRequestPruner.Prune,
				MinRetention: model.ReplayHorizon,
			},
		},
		pkgoutbox.RetentionConfig{
			Retention: time.Duration(cfg.RetentionDays) * 24 * time.Hour,
			Interval:  time.Duration(cfg.RetentionSweepIntervalMin) * time.Minute,
		},
		logger,
	)
	go retentionSweeper.Run(ctx)

	// release.promoted:v2 stores each promoted release's topology in Postgres
	// and moves run-controller's live-release pointer. The handler reads the release's
	// topology artifact from object storage before its transaction opens.
	objectStore, err := objectstore.New(ctx, objectstore.ConfigFromS3(cfg.S3))
	if err != nil {
		logger.Error("Failed to build the object-storage client", "error", err)
		os.Exit(1)
	}
	// The seed reconcile loads the dbt seeds whose live content the warehouse
	// does not hold. It runs inside every release.promoted:v2 transaction that
	// moves a tenant's live release, for that tenant, and every
	// seedReconcileInterval from the loop started below, once per tenant with
	// a live release; maintenance mode stops it creating runs.
	// Every task run-controller plans gets the install's retry budget, and each
	// dispatch its node's timeout, else the install's.
	taskDefaults := run.TaskDefaults{MaxRetries: int32(cfg.TaskMaxRetries), Timeout: cfg.TaskTimeout} //nolint:gosec // G115: TASK_MAX_RETRIES is 0-10, checked at startup.
	runMetrics := statemetrics.NewRunMetrics(metricsReg)
	seedReconciler := seedreconcile.New(uowFactory, cfg.Maintenance, taskDefaults, logger)
	releasePromotedHandler := svchandlers.NewReleasePromotedHandler(s3adapter.NewTopologyArtifactReader(objectStore), seedReconciler, logger)

	// Every run-controller consumer is the same shape: a domain handler wrapped by a
	// redis binding (parser + dedup + UoW transaction), driven by a
	// StreamConsumer on its (stream, group). They are declared in one table and
	// started uniformly via runConsumer below; consumer lifecycle is tied to
	// ctx — the lifecycle manager cancels ctx on shutdown, which exits each
	// Start cleanly. Stream/group names always come from pkg/streams constants.
	// timeout is the handler deadline; 0 keeps pkgredis.DefaultHandlerTimeout.
	consumers := []struct {
		name    string
		stream  string
		group   string
		binding pkgredis.MessageHandler
		timeout time.Duration
	}{
		{
			name:    "task_status_updated",
			stream:  streams.TaskStatusUpdatedV2,
			group:   streams.RunControllerTaskStatusUpdated,
			binding: redis.NewTaskStatusReportBinding(uowFactory, svchandlers.NewAttemptReportHandler(taskDefaults, runMetrics, logger), logger),
		},
		{
			name:    "release_promoted",
			stream:  streams.ReleasePromotedV2,
			group:   streams.RunControllerReleasePromoted,
			binding: redis.NewReleasePromotedBinding(uowFactory, releasePromotedHandler, logger),
			timeout: releasePromotedHandlerTimeout,
		},
	}
	for _, c := range consumers {
		runConsumer(c.name, pkgredis.NewStreamConsumer(redisClient, c.stream, c.group, c.binding, logger,
			pkgredis.WithHandlerTimeout(c.timeout)))
		logger.Info("Stream consumer initialized", "consumer", c.name)
	}

	// The periodic seed reconcile builds what a promotion's own pass left for
	// later, such as a seed whose previous load was still running. Its
	// goroutine is tracked, so shutdown waits for a pass in progress.
	lifecycleManager.Go(func() { seedReconciler.Loop(ctx, seedReconcileInterval) })

	// The lease waker grants each node of each tenant whose write lease is
	// free to the tenant's task that has waited longest for it, and deletes
	// the leases of tasks that ended without releasing them.
	leaseWaker := leasewaker.New(uowFactory, taskDefaults, runMetrics, logger)

	// The attempt reconcile asks execution-controller about every attempt, of
	// any tenant, nothing was heard of within its window and fails every run
	// left with nothing in flight as stalled, each under its own tenant.
	// Under the lock of a run it would fail, it first grants each free node
	// the run's waiting task is next in line for, so a lease wait a grant can
	// serve is never failed; the lease waker pass it runs before each stall
	// scan grants most freed nodes before the scan.
	// Its goroutine is tracked, so shutdown waits for a pass in progress.
	attemptReconciler := attemptreconcile.New(uowFactory, taskDefaults, runMetrics, leaseWaker.Pass, logger)
	lifecycleManager.Go(func() { attemptReconciler.Run(ctx, attemptReconcileInterval) })

	// The lease waker passes at startup, on every notification node_leases'
	// trigger sends when a committed transaction released a lease, and every
	// leaseWakerInterval. Its goroutine is tracked, so shutdown waits for a
	// pass in progress.
	leaseListener, err := pkgoutbox.NewPostgresWaker(ctx, cfg.Postgres.DSN(), postgres.NodeLeasesChannel, logger)
	if errors.Is(err, context.Canceled) {
		logger.Info("Shutdown requested while waiting for the node lease listener")
		os.Exit(0)
	}
	if err != nil {
		logger.Error("Failed to listen for node lease releases", "error", err)
		os.Exit(1)
	}
	// Closing the listener can wait on a connection attempt in progress, so
	// the handler stops waiting at the shutdown deadline.
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error { return leaseListener.CloseContext(ctx) })
	lifecycleManager.Go(func() { leaseWaker.Run(ctx, leaseListener.Wake(), leaseWakerInterval) })

	// The cron loop and the gRPC entry points plan each run for the tenant its
	// entry point chose; the seed reconcile builds its own planner for each
	// pass.
	planner := runstart.Planner{Tasks: taskDefaults}

	// The cron loop fires the due schedules of every tenant, each under the
	// tenant of its row: in one transaction per schedule it starts the run of
	// the latest due slot and moves the schedule's next fire on. It passes at
	// startup, when the earliest next fire comes, on every notification the
	// schedules table's trigger sends once a change to it commits, and every
	// cronLoopTick. Each fire is bounded by cronloop.FireTimeout. Maintenance
	// mode consumes each due slot with no run. Its goroutine is tracked, and
	// shutdown cancels its context before draining it: a fire in progress
	// rolls back, and its slot fires once, late, on a later pass.
	schedulesListener, err := pkgoutbox.NewPostgresWaker(ctx, cfg.Postgres.DSN(), postgres.SchedulesChannel, logger)
	if errors.Is(err, context.Canceled) {
		logger.Info("Shutdown requested while waiting for the schedules listener")
		os.Exit(0)
	}
	if err != nil {
		logger.Error("Failed to listen for schedule changes", "error", err)
		os.Exit(1)
	}
	// Closing the listener can wait on a connection attempt in progress, so
	// the handler stops waiting at the shutdown deadline.
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error { return schedulesListener.CloseContext(ctx) })
	cronLoop := cronloop.New(uowFactory, svchandlers.NewStartRunHandler(planner, logger), runMetrics, cfg.Maintenance, logger)
	lifecycleManager.Go(func() { cronLoop.Run(ctx, schedulesListener.Wake(), cronLoopTick) })

	// The RunService edge acts for the tenant the identity interceptor placed
	// on each request's context; its reads are bound to that tenant per
	// request.
	runReadRepo := postgres.NewRunReadRepository(db)
	nodeRunsOf := func(tenantID string) postgres.NodeRunRepository {
		return postgres.NewNodeRunRepository(db, tenantID, logger)
	}
	// The schedules service edits each tenant's crons one statement at a time
	// on the autocommit db: a save while the cron loop holds the schedule's
	// row for a fire waits for that fire to commit.
	scheduleService := schedules.New(
		postgres.NewScheduleRepository(db, nil, cronParser, logger),
		postgres.NewLiveScheduleModelRepository(db),
		runRepo, cronParser, clk,
	)
	runService := runservice.NewServer(runservice.Reads{
		Runs:           runreads.NewRunDetails(runRepo, postgres.NewTaskReadRepository(taskCollection)),
		Attempts:       runreads.NewAttempts(taskAttemptReadRepo),
		Nodes:          runreads.NewNodes(postgres.NewNodeReadRepository(nodeRunsOf), taskDefaults.Timeout),
		Graphs:         runreads.New(runReadRepo),
		ScheduleGraphs: runreads.NewScheduleGraphs(postgres.NewScheduleGraphRepository(db)),
		Rebases:        runreads.NewRebasePreviewer(planner, uowFactory, runReadRepo),
	}, svchandlers.NewRunCommands(uowFactory, runMetrics, planner, logger), scheduleService, logger)

	// Create gRPC server
	grpcServer, err := grpcserver.NewServer(cfg.GRPCPort, runService, logger, cfg.Maintenance)
	if err != nil {
		logger.Error("Failed to create gRPC server", "error", err)
		os.Exit(1)
	}

	// Register gRPC server cleanup
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		return grpcServer.Shutdown(ctx)
	})

	// Start gRPC server in background
	go func() {
		if err := grpcServer.Start(); err != nil {
			logger.Error("gRPC server error", "error", err)
		}
	}()

	// Register health server cleanup
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		return healthServer.Shutdown(ctx)
	})

	// Everything is initialised and serving: readiness may now turn true.
	startup.Done()

	logger.Info("run-controller service started successfully",
		"grpc_port", cfg.GRPCPort,
		"health_port", cfg.HealthPort,
	)

	// Block until the graceful-shutdown sequence has fully completed. The signal
	// handler cancels ctx (stops intake), drains in-flight goroutines, then runs
	// the infra-close handlers; Done() closes only after that sequence finishes,
	// so there is no fixed sleep racing the shutdown.
	<-lifecycleManager.Done()
	logger.Info("Service stopped")
}
