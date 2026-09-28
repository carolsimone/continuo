package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFullRefreshOperationSingleNode verifies the full_refresh operation end to
// end for a service-1 model (table_a) and seed (seed_table_1). service-1 is
// routed through the customname-dbt dialect in the e2e ConfigMap, so each Job
// must run customname-dbt with --full-refresh, the run must succeed, and the
// :Run node must record operation="full_refresh".
func TestFullRefreshOperationSingleNode(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	cleanupTestData(t, ctx, clients, "single-node-full-refresh-op")
	seedTopology(t, ctx, clients)

	for _, table := range []string{"table_a", "seed_table_1"} {
		t.Run(table, func(t *testing.T) {
			resp, err := clients.stateClient.TriggerSingleNodeRun(ctx, &statev1.TriggerSingleNodeRunRequest{
				ServiceName: "service-1", SchemaName: "e2e_schema", TableName: table,
				MetadataSource: "latest", Operation: "full_refresh",
			})
			require.NoError(t, err)
			runID, err := uuid.Parse(resp.RunId)
			require.NoError(t, err)
			defer cleanupSingleNodeRun(t, ctx, clients, runID, resp.ScheduleName)

			verifySchedulerSucceeded(t, ctx, clients, runID)
			assert.Equal(t, "full_refresh", queryNeo4jRunOperation(t, clients, runID))

			jobs, err := getK8sJobs(ctx, "table_name="+table+",schedule="+resp.ScheduleName)
			require.NoError(t, err)
			require.NotEmpty(t, jobs.Items, "%s must have been dispatched", table)
			cmd := jobs.Items[0].Spec.Template.Spec.Containers[0].Command
			assert.Equal(t, "customname-dbt", cmd[0])
			assert.Contains(t, strings.Join(cmd, " "), "--full-refresh")
		})
	}
}

// TestFullRefreshRejectedOnSchedule verifies a whole-schedule full refresh is
// refused at the state API.
func TestFullRefreshRejectedOnSchedule(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	_, err := clients.stateClient.TriggerSchedule(ctx, &statev1.TriggerScheduleRequest{
		ScheduleName: "e2e-schedule", Operation: "full_refresh",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "single node")
}
