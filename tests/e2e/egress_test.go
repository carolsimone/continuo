package e2e

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// egressProxyHost is egress-proxy as the e2e container reaches it.
func egressProxyHost() string { return getEnv("EGRESS_PROXY_HOST", "egress-proxy") }

// connectStatus sends one CONNECT for target to egress-proxy and returns the
// status code it answers with.
func connectStatus(t *testing.T, target string) int {
	t.Helper()
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(egressProxyHost(), "3128"), 10*time.Second)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(30*time.Second)))
	_, err = fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target)
	require.NoError(t, err)
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodConnect})
	require.NoError(t, err)
	defer resp.Body.Close()
	return resp.StatusCode
}

func TestEgress_ProxyRefusesHostNotOnAllowlist(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	assert.Equal(t, http.StatusForbidden, connectStatus(t, "unlisted.example.com:443"))
}

func TestEgress_ProxyAdmitsListedHost(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	assert.Equal(t, http.StatusOK, connectStatus(t, "stub-llm:9100"))
}

// egressAllowedPattern captures the value of each allowed sample of
// continuo_egress_requests_total, e.g.
// continuo_egress_requests_total{decision="allowed",reason="ok",service="egress-proxy"} 3.
var egressAllowedPattern = regexp.MustCompile(`(?m)^continuo_egress_requests_total\{[^}]*decision="allowed"[^}]*\} (\S+)$`)

// egressAllowed returns how many requests and tunnels egress-proxy has allowed,
// summed over every reason. egress-proxy exports each decision/reason series
// at 0 from startup, so a scrape with no allowed sample means the series is
// missing, not that nothing was allowed.
func egressAllowed(t *testing.T) float64 {
	t.Helper()
	samples := egressAllowedPattern.FindAllStringSubmatch(scrapeMetrics(t, egressProxyHost()), -1)
	require.NotEmpty(t, samples,
		"egress-proxy exposes no continuo_egress_requests_total{decision=\"allowed\"} series")
	var total float64
	for _, m := range samples {
		v, err := strconv.ParseFloat(m[1], 64)
		require.NoError(t, err, "unparseable sample value %q", m[1])
		total += v
	}
	return total
}

// TestEgress_ChatTrafficGoesThroughProxy proves agent-chat reaches its LLM
// through egress-proxy: one chat turn raises the proxy's allowed count.
func TestEgress_ChatTrafficGoesThroughProxy(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	before := egressAllowed(t)

	conn := dialChatWS(t)
	thread := expectFrame(t, conn, "thread")
	require.NotEmpty(t, thread.ThreadID)
	sendJSON(t, conn, chatMessage{Type: "user_message", Text: fmt.Sprintf("how is %s", testScheduleName)})
	final := expectFrame(t, conn, "final")
	require.Contains(t, final.Text, "DONE")

	assert.Greater(t, egressAllowed(t), before, "a chat turn must reach stub-llm through egress-proxy")
}
