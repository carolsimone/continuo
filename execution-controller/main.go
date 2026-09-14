package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/carolsimone/continuo/execution-controller/adapters/commandcfg"
	"github.com/carolsimone/continuo/execution-controller/adapters/delayqueue"
	"github.com/carolsimone/continuo/execution-controller/adapters/http"
	"github.com/carolsimone/continuo/execution-controller/adapters/k8s"
	"github.com/carolsimone/continuo/execution-controller/adapters/postgres"
	"github.com/carolsimone/continuo/execution-controller/adapters/publisher"
	"github.com/carolsimone/continuo/execution-controller/adapters/redis"
	s3adapter "github.com/carolsimone/continuo/execution-controller/adapters/s3"
	"github.com/carolsimone/continuo/execution-controller/config"
	"github.com/carolsimone/continuo/execution-controller/domain/repository"
	"github.com/carolsimone/continuo/execution-controller/service/deployer"
	"github.com/carolsimone/continuo/execution-controller/service/handlers"
	"github.com/carolsimone/continuo/execution-controller/service/uow"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/lifecycle"
	"github.com/carolsimone/continuo/pkg/liveness"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
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

// schemaOpHeartbeatStale mirrors consumerHeartbeatStale for the schema-op
// consumers: it exceeds schemaOpHandlerTimeout plus a margin so a handler
// legitimately waiting on a Job never trips liveness.
const schemaOpHeartbeatStale = schemaOpHandlerTimeout + 2*time.Minute

// consumerHeartbeatStale is how long a consumer's read loop may make no
// progress before the liveness probe restarts the pod. It exceeds
// consumerHandlerTimeout plus a margin so an in-flight handler never trips it.
const consumerHeartbeatStale = 3 * time.Minute

// outboxTick is the outbox processor cadence. Check tickets and terminal
// announcements flow through this outbox, so the tick bounds the latency of
// every status transition.
const outboxTick = time.Second

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	v := &pkgconfig.Validator{}
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

	startConsumer := func(name string, consumer *pkgredis.StreamConsumer, handlerTimeout, heartbeatStale time.Duration) {
		consumer.SetHandlerTimeout(handlerTimeout)
		liveReg.RegisterWorker(name)
		liveReg.AddWorkerProbe(name+"_heartbeat", 10*time.Second, func(context.Context) error {
			return consumer.Healthy(heartbeatStale)
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
		startConsumer(name, consumer, consumerHandlerTimeout, consumerHeartbeatStale)
	}
	runSchemaOpConsumer := func(name string, consumer *pkgredis.StreamConsumer) {
		startConsumer(name, consumer, schemaOpHandlerTimeout, schemaOpHeartbeatStale)
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

	// ---- infrastructure ----

	pgDB, err := postgres.NewPostgresClient(cfg.Postgres, logger)
	if err != nil {
		logger.Error("Failed to create PostgreSQL client", "error", err)
		os.Exit(1)
	}
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Closing PostgreSQL connection")
		return pgDB.Close()
	})
	liveReg.AddProbe("postgres", 5*time.Second, func(ctx context.Context) error { return pgDB.PingContext(ctx) })

	redisClient := goredis.NewClient(&goredis.Options{Addr: cfg.Redis.Addr(), Password: cfg.Redis.Password})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		logger.Error("Failed to connect to Redis", "error", err)
		os.Exit(1)
	}
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error {
		logger.Info("Closing Redis connection")
		return redisClient.Close()
	})
	liveReg.AddProbe("redis", 5*time.Second, func(ctx context.Context) error { return redisClient.Ping(ctx).Err() })

	// dbt command dialect, resolved per service at Job-build time. A missing
	// file means built-in plain-dbt commands; an invalid file is fatal.
	cmdResolver, err := commandcfg.Load(os.Getenv("DBT_COMMANDS_CONFIG_PATH"), logger)
	if err != nil {
		logger.Error("Invalid dbt commands config", "error", err)
		os.Exit(1)
	}
	k8sClient, err := k8s.NewK8sClient(logger, cmdResolver)
	if err != nil {
		logger.Error("Failed to create K8s client", "error", err)
		os.Exit(1)
	}

	// The candidate-schema lifecycle never connects to the warehouse: it runs a
	// one-shot engine-image Job (ensure_schema/drop_schema) and blocks on it.
	candidateSchemaCreator := k8s.NewCandidateSchemaCreator(k8sClient, cfg.K8sNamespace, logger)
	candidateSchemaCleaner := k8s.NewCandidateSchemaCleaner(k8sClient, cfg.K8sNamespace, logger)

	logUploader := s3adapter.NewS3Client(cfg.S3.EndpointURL, cfg.S3.Bucket, cfg.S3.Region, cfg.S3.AccessKeyID, cfg.S3.SecretAccessKey)

	cancelledSchedulesRepo := postgres.NewCancelledSchedulesRepository(pgDB)

	// A fresh unit of work per inbound message keeps concurrent handlers isolated.
	uowFactory := func() uow.UnitOfWork { return postgres.NewPostgresUnitOfWork(pgDB, logger) }

	// ---- handlers and bindings ----

	queryHandler := handlers.NewQueryModelHandler(logger)
	retryHandler := handlers.NewRetryTaskHandler(logger)
	scheduleCancelledHandler := handlers.NewScheduleCancelledHandler(logger)
	validationReqHandler := handlers.NewValidationRequestedHandler(logger)
	validationNodeHandler := handlers.NewValidationNodeCompletedHandler(logger)
	seedBuildReqHandler := handlers.NewSeedBuildRequestedHandler(logger)
	seedBuildNodeHandler := handlers.NewSeedBuildNodeCompletedHandler(logger)
	compileReqHandler := handlers.NewCompileRequestedHandler(logger)
	compileNodeHandler := handlers.NewCompileNodeCompletedHandler(logger)
	jobStatusHandler := handlers.NewJobStatusHandler(k8sClient, logUploader, &handlers.JobStatusConfig{
		K8sNamespace:          cfg.K8sNamespace,
		CheckDelaySeconds:     cfg.K8sCheckDelaySeconds,
		ErrorMessageMaxLen:    cfg.ErrorMessageMaxLength,
		LogTailLines:          int64(cfg.LogTailLines),
		DefaultTaskMaxRetries: cfg.DefaultTaskMaxRetries,
	}, cancelledSchedulesRepo, logger)

	newConsumer := func(stream, group string, binding pkgredis.MessageHandler, opts ...pkgredis.ConsumerOption) *pkgredis.StreamConsumer {
		c := pkgredis.NewStreamConsumer(redisClient, stream, group, binding, logger, opts...)
		logger.Info("consumer initialized", "stream", stream, "group", group)
		return c
	}

	// schemaOpReclaim keeps the PEL sweep from stealing a message whose handler is
	// legitimately blocked on a schema-op Job.
	schemaOpReclaim := pkgredis.WithReclaimMinIdle(schemaOpHandlerTimeout + time.Minute)

	queryConsumer := newConsumer(streams.QueryModelV1, streams.ExecutorQueryModel,
		redis.NewQueryModelBinding(uowFactory, queryHandler, logger))
	retryConsumer := newConsumer(streams.RetryTaskV1, streams.ExecutorRetry,
		redis.NewRetryTaskBinding(uowFactory, retryHandler, logger))
	scheduleCancelledConsumer := newConsumer(streams.ScheduleCancelledV1, streams.ExecutorScheduleCancelled,
		redis.NewScheduleCancelledBinding(uowFactory, scheduleCancelledHandler, logger))
	validationReqConsumer := newConsumer(streams.ValidationRequestedV1, streams.ExecutorValidationRequested,
		redis.NewValidationRequestedBinding(uowFactory, validationReqHandler, candidateSchemaCreator, logger), schemaOpReclaim)
	validationNodeConsumer := newConsumer(streams.ValidationNodeCompletedV1, streams.ExecutorValidationNodeCompleted,
		redis.NewValidationNodeCompletedBinding(uowFactory, validationNodeHandler, logger))
	seedBuildReqConsumer := newConsumer(streams.SeedBuildRequestedV1, streams.ExecutorSeedBuildRequested,
		redis.NewSeedBuildRequestedBinding(uowFactory, seedBuildReqHandler, candidateSchemaCreator, logger), schemaOpReclaim)
	seedBuildNodeConsumer := newConsumer(streams.SeedBuildNodeCompletedV1, streams.ExecutorSeedBuildNodeCompleted,
		redis.NewSeedBuildNodeCompletedBinding(uowFactory, seedBuildNodeHandler, logger))
	compileReqConsumer := newConsumer(streams.CompileRequestedV1, streams.ExecutorCompileRequested,
		redis.NewCompileRequestedBinding(uowFactory, compileReqHandler, logger))
	compileNodeConsumer := newConsumer(streams.CompileNodeCompletedV1, streams.ExecutorCompileNodeCompleted,
		redis.NewCompileNodeCompletedBinding(uowFactory, compileNodeHandler, logger))
	validationResultTeardownConsumer := newConsumer(streams.ValidationResultV1, streams.ExecutorValidationResultTeardown,
		redis.NewValidationResultTeardownBinding(candidateSchemaCleaner, logger), schemaOpReclaim)
	pipelineRunFinishedTeardownConsumer := newConsumer(streams.PipelineRunFinishedV1, streams.ExecutorPipelineRunFinished,
		redis.NewPipelineRunFinishedTeardownBinding(candidateSchemaCleaner, logger), schemaOpReclaim)
	releasePromotedTeardownConsumer := newConsumer(streams.ReleasePromotedV1, streams.ExecutorReleasePromoted,
		redis.NewReleasePromotedTeardownBinding(candidateSchemaCleaner, logger), schemaOpReclaim)
	releaseRejectedTeardownConsumer := newConsumer(streams.ReleaseRejectedV1, streams.ExecutorReleaseRejected,
		redis.NewReleaseRejectedTeardownBinding(candidateSchemaCleaner, logger), schemaOpReclaim)
	deployedConsumer := newConsumer(streams.NodeDeployedV1, streams.K8sDeployed,
		redis.NewNodeDeployedBinding(uowFactory, jobStatusHandler, logger))
	checkConsumer := newConsumer(streams.CheckK8sV1, streams.K8sCheckStatus,
		redis.NewCheckK8sBinding(uowFactory, jobStatusHandler, logger))

	// ---- background workers ----

	outboxProcessor := pkgoutbox.NewProcessor(pgDB, "execution_outbox", publisher.NewOutboxPublisher(redisClient, logger), nil, logger,
		// PerAggregateFIFO: rows sharing an aggregate publish in insertion order,
		// so a task's RUNNING announcement leaves before its later rows.
		pkgoutbox.ProcessorConfig{Tick: outboxTick, BatchSize: 100, PerAggregateFIFO: true})
	runWorker("outbox_processor", outboxProcessor.Run)

	deployDispatcher := deployer.NewDispatcher(
		pgDB, k8s.NewDeployer(k8sClient, cfg.K8sNamespace),
		func(exec pkgoutbox.Executor) repository.DeploymentRepository {
			return postgres.NewDeploymentsRepository(exec, logger)
		},
		func(exec pkgoutbox.Executor) repository.ValidationAggregateRepository {
			return postgres.NewValidationAggregateRepository(exec)
		},
		cfg.MaxConcurrentJobs, logger,
		deployer.DispatcherConfig{Tick: 5 * time.Second, BatchSize: 50},
	)
	runWorker("deploy_dispatcher", deployDispatcher.Run)

	// Every second, atomically move due check tickets from the delay queue onto
	// check.k8s:v1. A missed tick loses nothing: the queue is durable.
	promoter := delayqueue.NewPromoter(redisClient, logger)
	runWorker("delayqueue_promoter", func(ctx context.Context) error { return promoter.Run(ctx, time.Second) })

	runWorker("cancelled_schedules_sweeper", func(ctx context.Context) error {
		ticker := time.NewTicker(time.Duration(cfg.CancelledSchedulesSweepIntervalMin) * time.Minute)
		defer ticker.Stop()
		ttl := time.Duration(cfg.CancelledSchedulesTTLHours) * time.Hour
		for {
			select {
			case <-ticker.C:
				if n, err := cancelledSchedulesRepo.DeleteExpired(ctx, ttl); err != nil {
					logger.Error("cancelled_schedules sweep failed", "error", err)
				} else if n > 0 {
					logger.Info("Swept expired cancelled_schedules rows", "count", n)
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	healthServer := http.NewHealthServer(cfg.HTTPPort, liveReg, logger)
	go func() {
		if err := healthServer.Start(); err != nil {
			logger.Error("Health server error", "error", err)
		}
	}()
	lifecycleManager.RegisterShutdownHandler(func(ctx context.Context) error { return healthServer.Shutdown(ctx) })

	// ---- consumers ----

	runConsumer("query_model", queryConsumer)
	runConsumer("retry_task", retryConsumer)
	runConsumer("schedule_cancelled", scheduleCancelledConsumer)
	runSchemaOpConsumer("validation_requested", validationReqConsumer)
	runConsumer("validation_node_completed", validationNodeConsumer)
	runSchemaOpConsumer("seed_build_requested", seedBuildReqConsumer)
	runConsumer("seed_build_node_completed", seedBuildNodeConsumer)
	runConsumer("compile_requested", compileReqConsumer)
	runConsumer("compile_node_completed", compileNodeConsumer)
	runSchemaOpConsumer("validation_result_teardown", validationResultTeardownConsumer)
	runSchemaOpConsumer("pipeline_run_finished_teardown", pipelineRunFinishedTeardownConsumer)
	runSchemaOpConsumer("release_promoted_teardown", releasePromotedTeardownConsumer)
	runSchemaOpConsumer("release_rejected_teardown", releaseRejectedTeardownConsumer)
	runConsumer("node_deployed", deployedConsumer)
	runConsumer("check_k8s", checkConsumer)

	<-lifecycleManager.Done()
	logger.Info("execution-controller service stopped")
}
