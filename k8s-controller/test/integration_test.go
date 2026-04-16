package test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/k8s-controller/adapters/grpc"
	"github.com/carolsimone/continuo/k8s-controller/adapters/k8s"
	"github.com/carolsimone/continuo/k8s-controller/adapters/postgres"
	"github.com/carolsimone/continuo/k8s-controller/adapters/redis"
	"github.com/carolsimone/continuo/k8s-controller/domain/command"
	"github.com/carolsimone/continuo/k8s-controller/domain/model"
	"github.com/carolsimone/continuo/k8s-controller/service/handlers"
	"github.com/carolsimone/continuo/k8s-controller/service/uow"
	"github.com/carolsimone/continuo/k8s-controller/test/fakes"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// TestIntegration_EndToEnd tests the full flow against docker-compose services
func TestIntegration_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	redisAddr := getEnvOrDefault("REDIS_HOST", "localhost") + ":" + getEnvOrDefault("REDIS_PORT", "6379")

	// Connect to Redis (assuming docker-compose is running)
	redisClient := goredis.NewClient(&goredis.Options{
		Addr: redisAddr,
	})
	defer redisClient.Close()

	// Ping Redis to ensure it's available
	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available (is docker-compose running?): %v", err)
	}

	stateAddr := getEnvOrDefault("STATE_SERVICE_GRPC_ADDR", "localhost:50051")

	// Verify state service TCP port is reachable before creating gRPC client
	// (grpc.NewStateClient is lazy and never returns a connection error)
	if err := checkTCPReachable(stateAddr, 2*time.Second); err != nil {
		t.Skipf("State service not available (is docker-compose running?): %v", err)
	}

	// Connect to state service gRPC
	stateClient, err := grpc.NewStateClient(stateAddr, logger)
	if err != nil {
		t.Skipf("State service not available (is docker-compose running?): %v", err)
	}
	defer stateClient.Close()

	// Initialize K8s client (will fail gracefully if not in cluster)
	k8sClient, err := k8s.NewK8sClient(logger)
	if err != nil {
		t.Logf("K8s client not available (expected in CI): %v", err)
		t.Skip("Skipping test that requires K8s access")
	}

	pgHost := getEnvOrDefault("POSTGRES_HOST", "localhost")

	// Connect to PostgreSQL for UnitOfWork
	pgDB, err := postgres.NewPostgresClient(pgHost, 5432, "runner", "continuo_svc", "runner", logger)
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}
	defer pgDB.Close()

	// Initialize UnitOfWork
	unitOfWork := uow.NewPostgresUnitOfWork(pgDB, logger)

	// Create handler
	config := &handlers.HandlerConfig{
		K8sNamespace:       "default",
		CheckDelaySeconds:  5, // Shorter for testing
		ErrorMessageMaxLen: 4096,
		LogTailLines:       50,
	}
	logUploader := &fakes.FakeLogUploader{}
	handler := handlers.NewCheckStatusHandler(k8sClient, stateClient, unitOfWork, logUploader, config, logger)

	// Create a test task in the state service
	taskID := uuid.New()
	scheduleID := uuid.New()

	// Create scheduler first (task_tracker has FK to scheduler_tracker)
	createSchedulerReq := &statev1.CreateSchedulerRequest{
		ScheduleId:   scheduleID.String(),
		ScheduleName: "test-schedule",
		Status:       statev1.SchedulerStatus_SCHEDULER_STATUS_PENDING,
	}
	_, err = stateClient.CreateScheduler(ctx, createSchedulerReq)
	if err != nil {
		t.Fatalf("Failed to create test scheduler: %v", err)
	}

	createTaskReq := &statev1.CreateTaskRequest{
		TaskId:      taskID.String(),
		ScheduleId:  scheduleID.String(),
		ServiceName: "test-service",
		SchemaName:  "test-schema",
		TableName:   "test-table",
		MaxRetries:  3,
		Status:      statev1.TaskStatus_TASK_STATUS_PENDING,
	}

	_, err = stateClient.CreateTask(ctx, createTaskReq)
	if err != nil {
		t.Fatalf("Failed to create test task: %v", err)
	}

	// Clean up task after test
	defer func() {
		deleteReq := &statev1.DeleteTaskRequest{TaskId: taskID.String()}
		stateClient.DeleteTask(ctx, deleteReq)
	}()

	// Test handling a non-existent job (should be marked as unknown/failed)
	t.Run("HandleUnknownJob", func(t *testing.T) {
		cmd := command.CheckJobStatus{
			TaskID:       taskID,
			ScheduleID:   scheduleID,
			ScheduleName: "test-schedule",
			ServiceName:  "test-service",
			Schema:       "test-schema",
			TableName:    "test-table",
			JobName:      "non-existent-job-" + uuid.New().String(),
		}

		err := handler.Handle(ctx, cmd)
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}

		// Verify task was marked as failed
		time.Sleep(1 * time.Second) // Give gRPC time to process
		task, err := stateClient.GetTask(ctx, taskID)
		if err != nil {
			t.Fatalf("Failed to get task: %v", err)
		}

		if task.Status != statev1.TaskStatus_TASK_STATUS_FAILED {
			t.Errorf("Expected task status to be FAILED, got: %v", task.Status)
		}
	})
}

// TestIntegration_RedisStreams tests Redis stream operations
func TestIntegration_RedisStreams(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	redisAddr := getEnvOrDefault("REDIS_HOST", "localhost") + ":" + getEnvOrDefault("REDIS_PORT", "6379")

	// Connect to Redis
	redisClient := goredis.NewClient(&goredis.Options{
		Addr: redisAddr,
	})
	defer redisClient.Close()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	// Create producer
	testCheckStream := fmt.Sprintf("check.k8s:v1.test.%d", time.Now().Unix())
	testRetryStream := fmt.Sprintf("retry.task:v1.test.%d", time.Now().Unix())
	testFailedStream := fmt.Sprintf("task.failed:v1.test.%d", time.Now().Unix())

	// Clean up streams after test
	defer func() {
		redisClient.Del(ctx, testCheckStream)
		redisClient.Del(ctx, testRetryStream)
		redisClient.Del(ctx, testFailedStream)
	}()

	t.Run("CreateMultiProducer", func(t *testing.T) {
		producer := redis.NewMultiProducer(redisClient, testCheckStream, testRetryStream, testFailedStream, logger)

		// Verify producer was created successfully
		if producer == nil {
			t.Error("Expected producer to be created")
		}
	})

	t.Run("VerifyStreamCreation", func(t *testing.T) {
		// Verify streams can be checked for existence
		_, err := redisClient.Exists(ctx, testRetryStream).Result()
		if err != nil {
			t.Errorf("Failed to check stream existence: %v", err)
		}
	})
}

// TestIntegration_StateServiceGRPC tests gRPC operations against state service
func TestIntegration_StateServiceGRPC(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	stateAddr := getEnvOrDefault("STATE_SERVICE_GRPC_ADDR", "localhost:50051")

	// Verify state service TCP port is reachable before creating gRPC client
	// (grpc.NewStateClient is lazy and never returns a connection error)
	if err := checkTCPReachable(stateAddr, 2*time.Second); err != nil {
		t.Skipf("State service not available: %v", err)
	}

	// Connect to state service
	stateClient, err := grpc.NewStateClient(stateAddr, logger)
	if err != nil {
		t.Skipf("State service not available: %v", err)
	}
	defer stateClient.Close()

	taskID := uuid.New()
	task2ID := uuid.New() // used for UpdateTaskWithRetry (needs its own RUNNING task)
	scheduleID := uuid.New()

	// Create scheduler first (task_tracker has FK to scheduler_tracker)
	createSchedulerReq := &statev1.CreateSchedulerRequest{
		ScheduleId:   scheduleID.String(),
		ScheduleName: "test-schedule",
		Status:       statev1.SchedulerStatus_SCHEDULER_STATUS_PENDING,
	}
	_, err = stateClient.CreateScheduler(ctx, createSchedulerReq)
	if err != nil {
		t.Fatalf("Failed to create test scheduler: %v", err)
	}

	// Create task2 upfront (RUNNING) so UpdateTaskWithRetry can transition it to FAILED.
	// k8s-controller is only authorised for RUNNING→SUCCEEDED and RUNNING→FAILED.
	_, err = stateClient.CreateTask(ctx, &statev1.CreateTaskRequest{
		TaskId:      task2ID.String(),
		ScheduleId:  scheduleID.String(),
		ServiceName: "test-service",
		SchemaName:  "test-schema",
		TableName:   "test-table2",
		MaxRetries:  3,
		Status:      statev1.TaskStatus_TASK_STATUS_RUNNING,
	})
	if err != nil {
		t.Fatalf("Failed to create task2: %v", err)
	}
	defer func() {
		stateClient.DeleteTask(ctx, &statev1.DeleteTaskRequest{TaskId: task2ID.String()})
	}()

	t.Run("CreateAndGetTask", func(t *testing.T) {
		// Create task with RUNNING status — k8s-controller acts on running K8s jobs.
		createReq := &statev1.CreateTaskRequest{
			TaskId:      taskID.String(),
			ScheduleId:  scheduleID.String(),
			ServiceName: "test-service",
			SchemaName:  "test-schema",
			TableName:   "test-table",
			MaxRetries:  3,
			Status:      statev1.TaskStatus_TASK_STATUS_RUNNING,
		}

		_, err := stateClient.CreateTask(ctx, createReq)
		if err != nil {
			t.Fatalf("Failed to create task: %v", err)
		}

		// Get task
		task, err := stateClient.GetTask(ctx, taskID)
		if err != nil {
			t.Fatalf("Failed to get task: %v", err)
		}

		if task.TaskId != taskID.String() {
			t.Errorf("Expected task ID %s, got: %s", taskID.String(), task.TaskId)
		}

		if task.RetryCount != 0 {
			t.Errorf("Expected retry count 0, got: %d", task.RetryCount)
		}

		if task.MaxRetries != 3 {
			t.Errorf("Expected max retries 3, got: %d", task.MaxRetries)
		}
	})

	t.Run("UpdateTaskStatus", func(t *testing.T) {
		// k8s-controller transitions RUNNING → SUCCEEDED when a K8s job completes.
		err := stateClient.UpdateTaskStatus(ctx, taskID, statev1.TaskStatus_TASK_STATUS_SUCCEEDED)
		if err != nil {
			t.Errorf("Failed to update task status: %v", err)
		}

		// Verify update
		task, err := stateClient.GetTask(ctx, taskID)
		if err != nil {
			t.Fatalf("Failed to get task: %v", err)
		}

		if task.Status != statev1.TaskStatus_TASK_STATUS_SUCCEEDED {
			t.Errorf("Expected status SUCCEEDED, got: %v", task.Status)
		}
	})

	t.Run("UpdateTaskWithRetry", func(t *testing.T) {
		// k8s-controller transitions RUNNING → FAILED and increments retry_count.
		// Uses task2 which is in RUNNING state; taskID is already SUCCEEDED.
		err := stateClient.UpdateTaskWithRetry(ctx, task2ID, statev1.TaskStatus_TASK_STATUS_FAILED, 1)
		if err != nil {
			t.Errorf("Failed to update task with retry: %v", err)
		}

		// Verify update
		task, err := stateClient.GetTask(ctx, task2ID)
		if err != nil {
			t.Fatalf("Failed to get task: %v", err)
		}

		if task.Status != statev1.TaskStatus_TASK_STATUS_FAILED {
			t.Errorf("Expected status FAILED, got: %v", task.Status)
		}

		if task.RetryCount != 1 {
			t.Errorf("Expected retry count 1, got: %d", task.RetryCount)
		}
	})

	t.Run("CreateTaskExecution", func(t *testing.T) {
		now := time.Now()
		params := &grpc.TaskExecutionParams{
			TaskID:           taskID.String(),
			StartedAt:        &now,
			CompletedAt:      &now,
			ExecutionSeconds: 10.5,
			K8sJobName:       "test-job",
			ErrorMessage:     "test error",
		}

		err := stateClient.CreateTaskExecution(ctx, params)
		if err != nil {
			t.Errorf("Failed to create task execution: %v", err)
		}
	})

	// Clean up
	t.Run("DeleteTask", func(t *testing.T) {
		deleteReq := &statev1.DeleteTaskRequest{TaskId: taskID.String()}
		_, err := stateClient.DeleteTask(ctx, deleteReq)
		if err != nil {
			t.Errorf("Failed to delete task: %v", err)
		}
	})
}

// TestIntegration_GetPodLogs_FailedContainer verifies that GetPodLogs correctly
// fetches logs from a real failing pod in the kind cluster.
func TestIntegration_GetPodLogs_FailedContainer(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	k8sClient, err := k8s.NewK8sClient(logger)
	if err != nil {
		t.Skipf("K8s client not available: %v", err)
	}

	// Build a raw clientset to create the job (K8sClient doesn't expose CreateJob)
	cfg, err := rest.InClusterConfig()
	if err != nil {
		t.Skipf("Not in cluster: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Skipf("Cannot create clientset: %v", err)
	}

	jobName := fmt.Sprintf("test-log-fetch-%d", time.Now().UnixNano())
	namespace := "default"
	const wantLog = "FATAL: intentional test failure from GetPodLogs test"

	// Create the failing job
	backoffLimit := int32(0)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: namespace},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:    "fail",
							Image:   "alpine:3.19",
							Command: []string{"sh", "-c", fmt.Sprintf("echo '%s'; exit 1", wantLog)},
						},
					},
				},
			},
		},
	}
	_, err = clientset.BatchV1().Jobs(namespace).Create(ctx, job, metav1.CreateOptions{})
	require.NoError(t, err)

	t.Cleanup(func() {
		propagation := metav1.DeletePropagationForeground
		clientset.BatchV1().Jobs(namespace).Delete(ctx, jobName, metav1.DeleteOptions{
			PropagationPolicy: &propagation,
		})
	})

	// Poll until the job reaches Failed state (timeout 60s)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		result, err := k8sClient.GetJobStatus(ctx, namespace, jobName)
		require.NoError(t, err)
		if result.Status == model.JobStatusFailed {
			break
		}
		time.Sleep(2 * time.Second)
	}

	// Fetch logs
	fullLog, tail, err := k8sClient.GetPodLogs(ctx, namespace, jobName, 50)
	require.NoError(t, err)

	require.NotEmpty(t, fullLog, "fullLog must not be empty")
	require.NotEmpty(t, tail, "tail must not be empty")
	require.Contains(t, fullLog, wantLog, "fullLog must contain the known error string")
	require.Contains(t, tail, wantLog, "tail must contain the known error string")
}
