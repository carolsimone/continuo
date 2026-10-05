package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"time"

	grpcadapter "github.com/carolsimone/continuo/dead-letter-controller/adapters/grpc"
	"github.com/carolsimone/continuo/dead-letter-controller/adapters/http"
	dlmetrics "github.com/carolsimone/continuo/dead-letter-controller/adapters/metrics"
	"github.com/carolsimone/continuo/dead-letter-controller/adapters/postgres"
	redisadapter "github.com/carolsimone/continuo/dead-letter-controller/adapters/redis"
	"github.com/carolsimone/continuo/dead-letter-controller/config"
	"github.com/carolsimone/continuo/dead-letter-controller/service/handlers"
	"github.com/carolsimone/continuo/dead-letter-controller/service/ports"
	"github.com/carolsimone/continuo/dead-letter-controller/service/uow"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgdb "github.com/carolsimone/continuo/pkg/db"
	"github.com/carolsimone/continuo/pkg/lifecycle"
	"github.com/carolsimone/continuo/pkg/liveness"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
)

// outboxHeartbeatStale is the liveness budget for the outbox processor's Run
// loop. An idle loop turns at least once per pkgoutbox.FallbackTick (5s), so 60s
// is comfortably above it: a wedged (not exited) processor trips within a
// minute, while an idle-but-live one never does.
const outboxHeartbeatStale = 60 * time.Second

// expireInterval is how often the expirer deletes dead letters whose original
// message has left the replay horizon.
const expireInterval = time.Hour

// dbPool bounds the Postgres pool when DB_MAX_OPEN_CONNS / DB_MAX_IDLE_CONNS
// are unset: two connections per stream consumer, plus the outbox relay, the
// expirer, gRPC requests and the retention sweeper's lock connection.
var dbPool = pkgconfig.PoolConfig{MaxOpenConns: 12, MaxIdleConns: 5}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	v := &pkgconfig.Validator{}
	cfg := config.Load(v)
	if missing := v.Missing(); len(missing) > 0 {
		logger.Error("missing required env vars", "vars", strings.Join(missing, ", "))
		os.Exit(1)
	}

	logger.Info("Starting dead-letter-controller service")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The lifecycle manager drives the ordered graceful shutdown (stop intake →
	// drain tracked goroutines → close infra); the liveness registry tracks
	// background workers and feeds the /ready and /livez probes.
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

	// Start the HTTP health server before any dependency is reachable. /health
	// is a plain process-up probe; /ready also fails on a dependency outage
	// (stops traffic) while /livez fails only on a dead or wedged worker
	// (restarts the pod). The startup gate holds /ready false until every
	// consumer and server below is running.
	startup := liveReg.AddStartupGate()
	healthServer := http.NewServer(cfg.HTTPPort, liveReg, logger)
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

	// Initialize PostgreSQL connection.
	db, err := pkgdb.Open(ctx, cfg.Postgres, dbPool)
	if err != nil {
		logger.Error("Failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	logger.Info("PostgreSQL connection established")
	metricsReg.WatchDB(db.DB, cfg.Postgres.DB)
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Closing database connection")
		return db.Close()
	})

	// Initialize Redis client.
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

	// Start the outbox relay backed by pkg/outbox. It publishes this service's
	// redrive rows to their original streams through RedrivePublisher, and its
	// own terminal publish failures become outbox.dead_letter:v1 entries. The
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
	// Closing the listener can wait on a connection attempt in progress, so the
	// handler stops waiting at the shutdown deadline.
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error { return outboxWaker.CloseContext(ctx) })
	outboxProc := pkgoutbox.NewProcessor(
		db,
		postgres.OutboxTable,
		redisadapter.NewRedrivePublisher(redisClient),
		nil, // terminal failures dead-letter through the processor's built-in writer
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

	// Retention sweeper — prunes processed dead_letter_outbox rows past the
	// retention window so the table does not grow unbounded.
	retentionSweeper := pkgoutbox.NewRetentionSweeper(
		[]pkgoutbox.RetentionTarget{pkgoutbox.OutboxRetentionTarget(db, postgres.OutboxTable, logger)},
		pkgoutbox.RetentionConfig{},
		logger,
	)
	go retentionSweeper.Run(ctx)

	// Application objects.
	clock := ports.SystemClock{}
	repo := postgres.NewDeadLetterRepository(db)
	obs := dlmetrics.New(metricsReg, repo)
	recorder := handlers.NewRecorder(repo, obs, logger)
	query := handlers.NewQuery(repo)
	redriver := handlers.NewRedriver(func() uow.UnitOfWork { return postgres.NewUnitOfWork(db, logger) }, clock, obs, logger)
	expirer := handlers.NewExpirer(repo, clock, obs, logger)
	lifecycleManager.Go(func() { expirer.Run(ctx, expireInterval) })

	// Stream consumers. The service's own dead-letter consumers run WITHOUT
	// dead-lettering: a store failure must leave the entry pending so it is
	// stored once the store recovers, never dead-lettered onto itself (which
	// would loop).
	consumerBinding := redisadapter.NewConsumerDeadLetterBinding(recorder, clock)
	outboxBinding := redisadapter.NewOutboxDeadLetterBinding(recorder, clock)
	runConsumer("consumer_dead_letters", pkgredis.NewStreamConsumer(redisClient, streams.ConsumerDeadLetterV1,
		streams.DeadLetterControllerConsumerDeadLetters, consumerBinding.Handle, logger, pkgredis.WithoutDeadLetters()))
	runConsumer("outbox_dead_letters", pkgredis.NewStreamConsumer(redisClient, streams.OutboxDeadLetterV1,
		streams.DeadLetterControllerOutboxDeadLetters, outboxBinding.Handle, logger, pkgredis.WithoutDeadLetters()))

	// gRPC server. Serve runs in a plain goroutine the drain step does not wait
	// on; the shutdown handler stops it. Wrapping Start in a tracked
	// lifecycleManager.Go goroutine while a shutdown handler also stops the same
	// receiver would deadlock the drain (TestLifecycleGoNeverWrapsAServerStart).
	grpcServer, err := grpcadapter.NewServer(cfg.GRPCPort, query, redriver, logger)
	if err != nil {
		logger.Error("Failed to create gRPC server", "error", err)
		os.Exit(1)
	}
	lifecycleManager.RegisterShutdownHandler(grpcServer.Shutdown)
	go func() {
		if err := grpcServer.Start(); err != nil {
			logger.Error("gRPC server error", "error", err)
		}
	}()

	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		return healthServer.Shutdown(ctx)
	})

	// Everything is initialised and serving: readiness may now turn true.
	startup.Done()

	logger.Info("dead-letter-controller service started successfully",
		"grpc_port", cfg.GRPCPort,
		"http_port", cfg.HTTPPort,
	)

	// Block until the graceful-shutdown sequence has fully completed.
	<-lifecycleManager.Done()
	logger.Info("Service stopped")
}
