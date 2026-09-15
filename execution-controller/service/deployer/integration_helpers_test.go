//go:build integration

package deployer_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/command"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/serialization"
	executortest "github.com/carolsimone/continuo/execution-controller/test"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// setupPostgres spins a Postgres testcontainer, applies executor's Flyway
// migrations, and returns a connected *sqlx.DB plus a cleanup func.
func setupPostgres(t *testing.T) (*sqlx.DB, func()) {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER": "testuser", "POSTGRES_PASSWORD": "testpass", "POSTGRES_DB": "testdb",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req, Started: true,
	})
	require.NoError(t, err)
	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "5432")
	require.NoError(t, err)
	if host == "localhost" {
		host = "127.0.0.1"
	}
	connStr := "host=" + host + " port=" + port.Port() + " user=testuser password=testpass dbname=testdb sslmode=disable"
	var db *sqlx.DB
	for i := 0; i < 10; i++ {
		if db, err = sqlx.Connect("postgres", connStr); err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.NoError(t, err)
	require.NoError(t, executortest.ApplyMigrations(db.DB))
	return db, func() {
		_ = db.Close()
		_ = container.Terminate(ctx)
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// checkCommandFor reads the mode=validation deployments row for (releaseID,
// nodeID) and rebuilds the command.CheckJobStatus the job-status handler would
// decode off a check.k8s:v1 ticket for it — the same fields the dispatcher's
// writeFirstCheck stamps from the row's task_id/schedule_id columns and
// job_params.
func checkCommandFor(t *testing.T, db *sqlx.DB, releaseID, nodeID string) command.CheckJobStatus {
	t.Helper()
	var row struct {
		TaskID     uuid.UUID `db:"task_id"`
		ScheduleID uuid.UUID `db:"schedule_id"`
		JobParams  []byte    `db:"job_params"`
	}
	require.NoError(t, db.Get(&row,
		`SELECT task_id, schedule_id, job_params FROM deployments WHERE mode='validation' AND release_id=$1 AND node_id=$2`,
		releaseID, nodeID))
	var dto serialization.ValidationDeployTaskDTO
	require.NoError(t, json.Unmarshal(row.JobParams, &dto))
	vcmd := dto.ToDomain()
	return command.CheckJobStatus{
		TaskID:      row.TaskID,
		ScheduleID:  row.ScheduleID,
		ServiceName: vcmd.ServiceName,
		SchemaName:  vcmd.SchemaName,
		TableName:   vcmd.TableName,
		JobName:     vcmd.JobName,
		NodeType:    vcmd.NodeType,
		ImageTag:    vcmd.ImageTag,
	}
}

// checkCommandForTask reads dep's (production) deployments row — the most
// recently created one for its task_id, so it still resolves once a retry has
// queued a second row for the same task — and rebuilds the
// command.CheckJobStatus the job-status handler would decode off a
// check.k8s:v1 ticket for it, including the task-level retry budget the
// dispatcher's writeFirstCheck stamps from the command.
func checkCommandForTask(t *testing.T, db *sqlx.DB, dep *model.Deployment) command.CheckJobStatus {
	t.Helper()
	taskID, err := uuid.Parse(dep.Command().TaskID)
	require.NoError(t, err)
	var row struct {
		ScheduleID uuid.UUID `db:"schedule_id"`
		JobParams  []byte    `db:"job_params"`
	}
	require.NoError(t, db.Get(&row,
		`SELECT schedule_id, job_params FROM deployments WHERE task_id=$1 AND mode='production' ORDER BY created_at DESC LIMIT 1`,
		taskID))
	var dto serialization.DeployTaskDTO
	require.NoError(t, json.Unmarshal(row.JobParams, &dto))
	cmd := dto.ToDomain()
	return command.CheckJobStatus{
		TaskID:       taskID,
		ScheduleID:   row.ScheduleID,
		ScheduleName: cmd.ScheduleName,
		ServiceName:  cmd.ServiceName,
		SchemaName:   cmd.SchemaName,
		TableName:    cmd.TableName,
		JobName:      cmd.JobName,
		NodeType:     cmd.NodeType,
		ImageTag:     cmd.ImageTag,
		Operation:    cmd.Operation,
		RetryCount:   int32(cmd.TaskRetryCount),
		MaxRetries:   int32(cmd.TaskMaxRetries),
	}
}
