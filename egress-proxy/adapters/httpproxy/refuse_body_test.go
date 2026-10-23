package httpproxy

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stalledBodyBound is how long a refusal may take, on top of the proxy's header
// timeout, before the test calls it unbounded.
const stalledBodyBound = 2 * time.Second

// refuseStalledBody sends request, which promises a body of one byte that never
// follows, and returns the status of the answer, how long the answer took and
// how long the connection took to close. The proxy must answer at once and
// close within the header timeout it was given: a refusal does not wait on a
// body nobody will send.
func refuseStalledBody(t *testing.T, proxyAddr, request string) (status int, answeredAfter, closedAfter time.Duration) {
	t.Helper()
	c, err := net.Dial("tcp", proxyAddr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	// The read deadline is the test's own watchdog, far past the proxy's.
	require.NoError(t, c.SetReadDeadline(time.Now().Add(5*time.Second)))
	start := time.Now()
	_, err = io.WriteString(c, request)
	require.NoError(t, err)

	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, nil)
	require.NoError(t, err, "the refusal must reach the client without waiting for the body")
	answeredAfter = time.Since(start)
	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	// Whatever follows the answer, the proxy closes the connection: a client
	// that stalls its body does not keep a socket.
	_, err = br.ReadByte()
	require.ErrorIs(t, err, io.EOF, "the proxy must close the connection after the refusal")
	return resp.StatusCode, answeredAfter, time.Since(start)
}

func TestRefuse_DoesNotWaitForAnUnsentBody(t *testing.T) {
	allowed := map[string]string{"model.internal:8000": startEcho(t)}
	cases := []struct {
		name    string
		options Options
		hold    bool // takes the only slot first
		request string
		status  int
		record  string
	}{
		{
			name:    "forwarded request to a host that is not allowed",
			request: "POST http://unlisted.example/upload HTTP/1.1\r\nHost: unlisted.example\r\nContent-Length: 1\r\n\r\n",
			status:  http.StatusForbidden,
			record:  "denied:not_allowed",
		},
		{
			name:    "request that is neither CONNECT nor absolute-form",
			request: "POST /upload HTTP/1.1\r\nHost: unlisted.example\r\nContent-Length: 1\r\n\r\n",
			status:  http.StatusMethodNotAllowed,
			record:  "denied:unsupported",
		},
		{
			name:    "forwarded request with a chunked body that never arrives",
			request: "POST http://unlisted.example/upload HTTP/1.1\r\nHost: unlisted.example\r\nTransfer-Encoding: chunked\r\n\r\n",
			status:  http.StatusForbidden,
			record:  "denied:not_allowed",
		},
		{
			name:    "forwarded request when every slot is taken",
			options: Options{MaxTunnels: 1},
			hold:    true,
			request: "POST http://model.internal:8000/upload HTTP/1.1\r\nHost: model.internal:8000\r\nContent-Length: 1\r\n\r\n",
			status:  http.StatusServiceUnavailable,
			record:  "denied:capacity",
		},
		{
			name:    "forwarded request with a protocol upgrade",
			request: "POST http://model.internal:8000/upload HTTP/1.1\r\nHost: model.internal:8000\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nContent-Length: 1\r\n\r\n",
			status:  http.StatusMethodNotAllowed,
			record:  "denied:unsupported",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.options.HeaderTimeout = 300 * time.Millisecond
			_, obs, addr := startProxy(t, &fakeAuth{targets: allowed}, tc.options)
			if tc.hold {
				_, _, first := connectVia(t, addr, "model.internal:8000", "")
				require.Equal(t, http.StatusOK, first.StatusCode)
			}
			status, _, closedAfter := refuseStalledBody(t, addr, tc.request)
			assert.Equal(t, tc.status, status)
			assert.Less(t, closedAfter, tc.options.HeaderTimeout+stalledBodyBound,
				"the connection outlived the header timeout")
			assert.Contains(t, obs.snapshot().requests, tc.record)
		})
	}
}

// The answer does not wait for the read of what is left of the body to give
// up: a client that stalls its body is told no at once, and only its socket
// is closed late.
func TestRefuse_AnswersBeforeTheBodyReadGivesUp(t *testing.T) {
	const headerTimeout = 3 * time.Second
	_, _, addr := startProxy(t, &fakeAuth{targets: map[string]string{}}, Options{HeaderTimeout: headerTimeout})
	status, answeredAfter, closedAfter := refuseStalledBody(t, addr,
		"POST http://unlisted.example/upload HTTP/1.1\r\nHost: unlisted.example\r\nContent-Length: 1\r\n\r\n")
	assert.Equal(t, http.StatusForbidden, status)
	assert.Less(t, answeredAfter, headerTimeout/2, "the answer waited for the body read")
	assert.Less(t, closedAfter, headerTimeout+stalledBodyBound)
}

// A refusal of a request without a body keeps the connection for the client's
// next request: only an unread body closes it.
func TestRefuse_KeepsTheConnectionWhenThereIsNoBody(t *testing.T) {
	_, _, addr := startProxy(t, &fakeAuth{targets: map[string]string{}}, Options{})
	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	require.NoError(t, c.SetDeadline(time.Now().Add(5*time.Second)))
	br := bufio.NewReader(c)
	for range 2 {
		_, err = io.WriteString(c, "GET http://unlisted.example/ HTTP/1.1\r\nHost: unlisted.example\r\n\r\n")
		require.NoError(t, err)
		resp, err := http.ReadResponse(br, nil)
		require.NoError(t, err)
		_, err = io.Copy(io.Discard, resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	}
}
