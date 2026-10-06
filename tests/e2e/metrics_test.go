package e2e

import (
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
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
		{getEnv("DEAD_LETTER_HOST", "dead-letter-controller"), withConsumersAndOutbox},
		{getEnv("AGENT_CHAT_HOST", "agent-chat"), []string{"go_sql_open_connections{"}},
	}
	for _, tc := range cases {
		t.Run(tc.host, func(t *testing.T) {
			body := scrapeMetrics(t, tc.host)
			for _, want := range tc.want {
				assert.Contains(t, body, want)
			}
		})
	}
}

// Every series a service exposes carries a service label, so the samples below
// are matched with their label set, e.g.
// continuo_stream_trim_last_success_timestamp_seconds{service="dead-letter-controller"} 1.7e+09.

// trimLastSuccessPattern captures the sample value of the trim loop's
// last-success gauge from Prometheus exposition text.
var trimLastSuccessPattern = regexp.MustCompile(`(?m)^continuo_stream_trim_last_success_timestamp_seconds\{[^}]*\} (\S+)$`)

// queryModelLengthPattern matches the stream-length sample for query.model:v1,
// e.g. continuo_stream_length{service="dead-letter-controller",stream="query.model:v1"} 12.
var queryModelLengthPattern = regexp.MustCompile(
	`(?m)^continuo_stream_length\{[^}]*stream="` + regexp.QuoteMeta(streams.QueryModelV1) + `"[^}]*\} \S+$`)

// TestMetrics_DeadLetterControllerReportsStreamTrimming asserts the trim loop
// dead-letter-controller runs is observable: the loop runs once at startup, so
// its last-success gauge is positive, and every stream it bounds reports a
// length, query.model:v1 among them.
func TestMetrics_DeadLetterControllerReportsStreamTrimming(t *testing.T) {
	host := getEnv("DEAD_LETTER_HOST", "dead-letter-controller")
	var body string
	require.Eventually(t, func() bool {
		var err error
		body, err = fetchMetrics(host)
		if err != nil {
			return false
		}
		m := trimLastSuccessPattern.FindStringSubmatch(body)
		if m == nil {
			return false
		}
		v, err := strconv.ParseFloat(m[1], 64)
		return err == nil && v > 0
	}, 2*time.Minute, 2*time.Second,
		"continuo_stream_trim_last_success_timestamp_seconds must be positive once the trim loop has run")

	assert.Regexp(t, queryModelLengthPattern, body, "no continuo_stream_length sample for %s", streams.QueryModelV1)
}

// scrapeMetrics returns the Prometheus exposition text served by host.
func scrapeMetrics(t *testing.T, host string) string {
	t.Helper()
	body, err := fetchMetrics(host)
	require.NoError(t, err)
	return body
}

// fetchMetrics is scrapeMetrics without test assertions, for callers that retry
// on a failed scrape.
func fetchMetrics(host string) (string, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://%s:9464/metrics", host))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET /metrics on %s: status %d", host, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}
