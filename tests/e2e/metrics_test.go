package e2e

import (
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMetrics_GoServicesServePrometheusMetrics scrapes /metrics on every Go
// service the e2e stack runs in compose. execution-controller runs in kind;
// the chart lint covers its metrics port.
func TestMetrics_GoServicesServePrometheusMetrics(t *testing.T) {
	withConsumersAndOutbox := []string{"continuo_consumer_pending{", "continuo_outbox_backlog{", "go_sql_open_connections{"}
	cases := []struct {
		host string
		want []string
	}{
		{getEnv("STATE_HOST", "state"), withConsumersAndOutbox},
		{getEnv("ORCHESTRATOR_HOST", "orchestrator"), withConsumersAndOutbox},
		{getEnv("RELEASE_CONTROLLER_HOST", "release-controller"), withConsumersAndOutbox},
		{getEnv("REMEDIATION_HOST", "remediation"), withConsumersAndOutbox},
		{getEnv("AGENT_REMEDIATION_HOST", "agent-remediation"), withConsumersAndOutbox},
		{getEnv("AGENT_CHAT_HOST", "agent-chat"), []string{"go_sql_open_connections{"}},
	}
	client := &http.Client{Timeout: 10 * time.Second}
	for _, tc := range cases {
		t.Run(tc.host, func(t *testing.T) {
			resp, err := client.Get(fmt.Sprintf("http://%s:9464/metrics", tc.host))
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			for _, want := range tc.want {
				assert.Contains(t, string(body), want)
			}
		})
	}
}
