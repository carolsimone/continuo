package httpproxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/carolsimone/continuo/egress-proxy/domain/allowlist"
	"github.com/carolsimone/continuo/egress-proxy/service/authorize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ Authorizer = (*authorize.Authorizer)(nil)

// fakeAuth admits the "host:port" keys of targets and dials the real address
// each maps to; err, when set, is what Connect returns instead, and connect,
// when set, replaces the dial.
type fakeAuth struct {
	targets  map[string]string
	err      error
	connect  func(ctx context.Context, target string) (net.Conn, error)
	connects atomic.Int32
}

func (f *fakeAuth) Allowed(host string, port uint16) bool {
	_, ok := f.targets[net.JoinHostPort(host, strconv.Itoa(int(port)))]
	return ok
}

func (f *fakeAuth) Connect(ctx context.Context, host string, port uint16) (net.Conn, error) {
	f.connects.Add(1)
	if f.err != nil {
		return nil, f.err
	}
	target := f.targets[net.JoinHostPort(host, strconv.Itoa(int(port)))]
	if f.connect != nil {
		return f.connect(ctx, target)
	}
	var d net.Dialer
	return d.DialContext(ctx, "tcp", target)
}

// blockUntilCancelled is a fakeAuth.connect that closes entered, waits for
// its context to end, then fails the way Authorizer.Connect does when its
// dial is cancelled. It also gives up when the test ends, so a proxy that
// never cancels the context fails the test instead of hanging it.
func blockUntilCancelled(t *testing.T, entered chan struct{}) func(context.Context, string) (net.Conn, error) {
	return connectUntilCancelled(t, entered, make(chan struct{}))
}

// connectUntilCancelled is blockUntilCancelled that also closes ended once
// its context has ended.
func connectUntilCancelled(t *testing.T, entered, ended chan struct{}) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, target string) (net.Conn, error) {
		close(entered)
		select {
		case <-ctx.Done():
			close(ended)
		case <-t.Context().Done():
			return nil, errors.New("the test ended before Connect was cancelled")
		}
		return nil, fmt.Errorf("%w: %s: %w", authorize.ErrDial, target, ctx.Err())
	}
}

type fakeObserver struct {
	mu             sync.Mutex
	requests       []string
	up, down       int64
	opened, closed int
}

func (o *fakeObserver) Request(allowed bool, reason string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	decision := "denied"
	if allowed {
		decision = "allowed"
	}
	o.requests = append(o.requests, decision+":"+reason)
}

func (o *fakeObserver) Transferred(direction string, n int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if direction == DirectionUp {
		o.up += n
	} else {
		o.down += n
	}
}

func (o *fakeObserver) TunnelOpened() { o.mu.Lock(); o.opened++; o.mu.Unlock() }
func (o *fakeObserver) TunnelClosed() { o.mu.Lock(); o.closed++; o.mu.Unlock() }

type observed struct {
	requests       []string
	up, down       int64
	opened, closed int
}

func (o *fakeObserver) snapshot() observed {
	o.mu.Lock()
	defer o.mu.Unlock()
	return observed{append([]string(nil), o.requests...), o.up, o.down, o.opened, o.closed}
}

// syncBuffer collects log output written by the proxy's goroutines while the
// test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(b)
}

// entries decodes the JSON log lines written so far.
func (s *syncBuffer) entries(t *testing.T) []map[string]any {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(s.buf.String()), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &entry))
		out = append(out, entry)
	}
	return out
}

// logged reports whether a log line with message msg has been written.
func (s *syncBuffer) logged(msg string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Contains(s.buf.String(), `"msg":"`+msg+`"`)
}

func startProxy(t *testing.T, auth Authorizer, opts Options) (*Proxy, *fakeObserver, string) {
	t.Helper()
	return startProxyLogging(t, auth, opts, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func startProxyLogging(t *testing.T, auth Authorizer, opts Options, logger *slog.Logger) (*Proxy, *fakeObserver, string) {
	t.Helper()
	if opts.MaxTunnels == 0 {
		opts.MaxTunnels = 8
	}
	if opts.IdleTimeout == 0 {
		opts.IdleTimeout = time.Minute
	}
	if opts.HeaderTimeout == 0 {
		opts.HeaderTimeout = 5 * time.Second
	}
	obs := &fakeObserver{}
	p := New(auth, obs, logger, opts)
	// The test proxy listens on loopback alone. A listener on every
	// interface, as Listen opens, can share its port with another process's
	// loopback listener on some systems, which then receives the test's
	// connections.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p.listener = ln
	go func() { _ = p.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = p.Shutdown(ctx)
	})
	return p, obs, ln.Addr().String()
}

func TestListen_ListensOnEveryInterface(t *testing.T) {
	p := New(&fakeAuth{}, &fakeObserver{}, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{MaxTunnels: 1})
	addr, err := p.Listen(0)
	require.NoError(t, err)
	defer p.listener.Close()
	tcp, ok := addr.(*net.TCPAddr)
	require.True(t, ok)
	assert.True(t, tcp.IP.IsUnspecified(), "listening on %s", tcp)
	assert.NotZero(t, tcp.Port)
}

// startEcho returns the address of a TCP server that echoes every byte.
func startEcho(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}()
		}
	}()
	return ln.Addr().String()
}

// reply is a proxy response with its body read in full.
type reply struct {
	StatusCode int
	Body       string
}

// readReply reads one response to a req request from br, body included. The
// bytes after a 2xx reply to CONNECT are the tunnel's, not a body: they stay
// in br, and closing the unread body reads none of them.
func readReply(t *testing.T, br *bufio.Reader, req *http.Request) reply {
	t.Helper()
	resp, err := http.ReadResponse(br, req)
	require.NoError(t, err)
	defer resp.Body.Close()
	if req != nil && req.Method == http.MethodConnect && resp.StatusCode/100 == 2 {
		return reply{StatusCode: resp.StatusCode}
	}
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return reply{StatusCode: resp.StatusCode, Body: string(body)}
}

// startServer returns the address of a TCP server that runs serve on each
// connection it accepts and closes the connection when serve returns.
func startServer(t *testing.T, serve func(net.Conn)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				serve(c)
			}()
		}
	}()
	return ln.Addr().String()
}

// connectVia sends "CONNECT target" to the proxy, followed by extra in the
// same write, and returns the connection, its reader and the response.
func connectVia(t *testing.T, proxyAddr, target, extra string) (net.Conn, *bufio.Reader, reply) {
	t.Helper()
	c, err := net.Dial("tcp", proxyAddr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	_, err = fmt.Fprintf(c, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n%s", target, target, extra)
	require.NoError(t, err)
	br := bufio.NewReader(c)
	return c, br, readReply(t, br, &http.Request{Method: http.MethodConnect})
}

// sendConnect sends "CONNECT target" to the proxy without reading the reply.
func sendConnect(t *testing.T, proxyAddr, target string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", proxyAddr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	_, err = fmt.Fprintf(c, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target)
	require.NoError(t, err)
	return c
}

// exchange sends text to the proxy and returns the response's header, and
// its status and body read in full. The exchange fails the test after five
// seconds rather than hang it.
func exchange(t *testing.T, proxyAddr, text string) (http.Header, reply) {
	t.Helper()
	c, err := net.Dial("tcp", proxyAddr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	require.NoError(t, c.SetDeadline(time.Now().Add(5*time.Second)))
	_, err = io.WriteString(c, text)
	require.NoError(t, err)
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.Header, reply{StatusCode: resp.StatusCode, Body: string(body)}
}

// rawRequest sends text to the proxy and returns the response.
func rawRequest(t *testing.T, proxyAddr, text string) reply {
	t.Helper()
	_, r := exchange(t, proxyAddr, text)
	return r
}

// awaitClosed fails the test unless ch is closed within two seconds.
func awaitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting: " + what)
	}
}

// waitFor fails the test unless ch is closed within two seconds.
func waitFor(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	awaitClosed(t, ch, "Connect to be called")
}

// shutdown runs p.Shutdown with the given grace and fails the test unless it
// returns within a second of the grace ending.
func shutdown(t *testing.T, p *Proxy, grace time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Shutdown(ctx) }()
	select {
	case err := <-done:
		return err
	case <-time.After(grace + time.Second):
		t.Fatal("Shutdown did not return after the grace ended")
		return nil
	}
}

// readUntilClosed reads c until the proxy closes it and returns what arrived.
// It fails the test if c is still open after two seconds.
func readUntilClosed(t *testing.T, c net.Conn) string {
	t.Helper()
	require.NoError(t, c.SetReadDeadline(time.Now().Add(2*time.Second)))
	data, err := io.ReadAll(c)
	var ne net.Error
	if errors.As(err, &ne) {
		require.False(t, ne.Timeout(), "the proxy must close the connection, not leave it open")
	}
	return string(data)
}

func TestTunnel_RelaysBothWaysAndRecordsBytes(t *testing.T) {
	auth := &fakeAuth{targets: map[string]string{"api.example.com:443": startEcho(t)}}
	_, obs, addr := startProxy(t, auth, Options{})
	conn, br, resp := connectVia(t, addr, "api.example.com:443", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	_, err := conn.Write([]byte("ping"))
	require.NoError(t, err)
	got := make([]byte, 4)
	_, err = io.ReadFull(br, got)
	require.NoError(t, err)
	assert.Equal(t, "ping", string(got))
	require.NoError(t, conn.Close())

	require.Eventually(t, func() bool {
		s := obs.snapshot()
		return len(s.requests) == 1 && s.requests[0] == "allowed:ok" &&
			s.up == 4 && s.down == 4 && s.opened == 1 && s.closed == 1
	}, 2*time.Second, 10*time.Millisecond)
}

func TestTunnel_ForwardsBytesSentBehindTheConnectRequest(t *testing.T) {
	auth := &fakeAuth{targets: map[string]string{"api.example.com:443": startEcho(t)}}
	_, _, addr := startProxy(t, auth, Options{})
	_, br, resp := connectVia(t, addr, "api.example.com:443", "hello")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	got := make([]byte, 5)
	_, err := io.ReadFull(br, got)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(got))
}

// Bytes sent behind the CONNECT request reach the destination once, ahead of
// the bytes the client sends after the 200.
func TestTunnel_SendsBufferedBytesOnceAndFirst(t *testing.T) {
	auth := &fakeAuth{targets: map[string]string{"api.example.com:443": startEcho(t)}}
	_, obs, addr := startProxy(t, auth, Options{})
	conn, br, resp := connectVia(t, addr, "api.example.com:443", "hello")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_, err := conn.Write([]byte(" world"))
	require.NoError(t, err)
	got := make([]byte, len("hello world"))
	_, err = io.ReadFull(br, got)
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(got))

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(100*time.Millisecond)))
	_, err = br.ReadByte()
	var ne net.Error
	require.ErrorAs(t, err, &ne, "nothing is echoed twice")
	assert.True(t, ne.Timeout())

	require.NoError(t, conn.Close())
	require.Eventually(t, func() bool {
		s := obs.snapshot()
		return s.closed == 1 && s.up == int64(len("hello world")) && s.down == int64(len("hello world"))
	}, 2*time.Second, 10*time.Millisecond)
}

func TestTunnel_RefusesHostNotOnAllowlist(t *testing.T) {
	auth := &fakeAuth{targets: map[string]string{}}
	_, obs, addr := startProxy(t, auth, Options{})
	_, _, resp := connectVia(t, addr, "evil.example.com:443", "")
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.Equal(t, []string{"denied:not_allowed"}, obs.snapshot().requests)
	assert.Equal(t, int32(0), auth.connects.Load())
}

// countingResolver and countingDialer stand behind the real Authorizer and
// count the lookups and dials that reach them.
type countingResolver struct{ calls atomic.Int32 }

func (r *countingResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	r.calls.Add(1)
	return nil, errors.New("no lookup expected")
}

type countingDialer struct{ calls atomic.Int32 }

func (d *countingDialer) DialContext(context.Context, string, string) (net.Conn, error) {
	d.calls.Add(1)
	return nil, errors.New("no dial expected")
}

// countingAuthorizer passes every call to the real Authorizer and counts the
// calls to Connect.
type countingAuthorizer struct {
	*authorize.Authorizer
	connects atomic.Int32
}

func (a *countingAuthorizer) Connect(ctx context.Context, host string, port uint16) (net.Conn, error) {
	a.connects.Add(1)
	return a.Authorizer.Connect(ctx, host, port)
}

// An IP-literal target never matches the allowlist, whose entries are names:
// it is refused before Connect, so before any lookup or dial. The proxy runs
// on the real Authorizer and allowlist here.
func TestTunnel_RefusesIPLiteralTargets(t *testing.T) {
	allow, err := allowlist.New([]allowlist.Entry{{Host: "api.example.com", Port: 443}})
	require.NoError(t, err)
	resolver, dialer := &countingResolver{}, &countingDialer{}
	auth := &countingAuthorizer{Authorizer: authorize.New(allow, resolver, dialer)}
	_, obs, addr := startProxy(t, auth, Options{})

	targets := []string{
		"10.0.0.1:443",
		"[::1]:443",
		"169.254.169.254:443",
		"[::ffff:169.254.169.254]:443",
		"127.1:443",
		"2130706433:443",
	}
	var want []string
	for _, target := range targets {
		_, _, resp := connectVia(t, addr, target, "")
		assert.Equal(t, http.StatusForbidden, resp.StatusCode, target)
		want = append(want, "denied:not_allowed")
	}
	assert.Equal(t, want, obs.snapshot().requests)
	assert.Zero(t, auth.connects.Load(), "Connect is never called for an IP literal")
	assert.Zero(t, resolver.calls.Load(), "no lookup for an IP literal")
	assert.Zero(t, dialer.calls.Load(), "no dial for an IP literal")
}

func TestTunnel_RefusesMalformedTargets(t *testing.T) {
	auth := &fakeAuth{targets: map[string]string{"api.example.com:443": "127.0.0.1:1"}}
	_, obs, addr := startProxy(t, auth, Options{})

	var want []string
	for _, target := range []string{
		"api.example.com",
		"api.example.com:",
		"api.example.com:0",
		"api.example.com:65536",
		"api.example.com:99999",
		":443",
	} {
		_, _, resp := connectVia(t, addr, target, "")
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode, target)
		want = append(want, "denied:bad_request")
	}
	// A path-form target carries no authority; the Host header does not stand
	// in for it.
	resp := rawRequest(t, addr, "CONNECT /rpc HTTP/1.1\r\nHost: api.example.com:443\r\n\r\n")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "path-form CONNECT")
	want = append(want, "denied:bad_request")

	assert.Equal(t, want, obs.snapshot().requests)
	assert.Equal(t, int32(0), auth.connects.Load())
}

func TestTunnel_MapsAuthorizerErrors(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		status int
		record string
	}{
		{"private address", &authorize.Denied{Reason: authorize.ReasonPrivateAddress, Host: "api.example.com", Port: 443, Detail: "10.0.0.1 is private"}, http.StatusForbidden, "denied:private_address"},
		{"metadata", &authorize.Denied{Reason: authorize.ReasonMetadata, Host: "api.example.com", Port: 443, Detail: "169.254.169.254 is a cloud metadata address"}, http.StatusForbidden, "denied:metadata"},
		{"resolve failed", &authorize.Denied{Reason: authorize.ReasonResolveFailed, Host: "api.example.com", Port: 443, Detail: "no such host"}, http.StatusBadGateway, "denied:resolve_failed"},
		{"dial failed", fmt.Errorf("%w: api.example.com:443: connection refused", authorize.ErrDial), http.StatusBadGateway, "denied:dial_failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			auth := &fakeAuth{targets: map[string]string{"api.example.com:443": "127.0.0.1:1"}, err: tc.err}
			_, obs, addr := startProxy(t, auth, Options{})
			_, _, resp := connectVia(t, addr, "api.example.com:443", "")
			assert.Equal(t, tc.status, resp.StatusCode)
			assert.NotContains(t, resp.Body, "10.0.0.1", "the client never sees resolved addresses")
			assert.NotContains(t, resp.Body, "169.254.169.254")
			assert.Equal(t, "egress-proxy: "+strings.TrimPrefix(tc.record, "denied:")+"\n", resp.Body)
			assert.Equal(t, []string{tc.record}, obs.snapshot().requests)
		})
	}
}

// Each denial is logged at WARN as "egress denied" with its detail, which the
// client never sees.
func TestTunnel_LogsDenialWithDetail(t *testing.T) {
	var logs syncBuffer
	auth := &fakeAuth{
		targets: map[string]string{"api.example.com:443": "127.0.0.1:1"},
		err:     &authorize.Denied{Reason: authorize.ReasonPrivateAddress, Host: "api.example.com", Port: 443, Detail: "10.0.0.1 is private"},
	}
	_, _, addr := startProxyLogging(t, auth, Options{}, slog.New(slog.NewJSONHandler(&logs, nil)))
	conn, _, resp := connectVia(t, addr, "api.example.com:443", "")
	require.Equal(t, http.StatusForbidden, resp.StatusCode)

	var denials []map[string]any
	for _, e := range logs.entries(t) {
		if e["msg"] == "egress denied" {
			denials = append(denials, e)
		}
	}
	require.Len(t, denials, 1)
	d := denials[0]
	assert.Equal(t, "WARN", d["level"])
	assert.Equal(t, "api.example.com", d["host"])
	assert.Equal(t, float64(443), d["port"])
	assert.Equal(t, conn.LocalAddr().String(), d["client"])
	assert.Equal(t, http.MethodConnect, d["method"])
	assert.Equal(t, "private_address", d["reason"])
	assert.Equal(t, "10.0.0.1 is private", d["detail"])
}

func TestTunnel_RefusesBeyondCapacity(t *testing.T) {
	auth := &fakeAuth{targets: map[string]string{"api.example.com:443": startEcho(t)}}
	_, obs, addr := startProxy(t, auth, Options{MaxTunnels: 1})
	_, _, first := connectVia(t, addr, "api.example.com:443", "")
	require.Equal(t, http.StatusOK, first.StatusCode)
	_, _, second := connectVia(t, addr, "api.example.com:443", "")
	assert.Equal(t, http.StatusServiceUnavailable, second.StatusCode)
	assert.Contains(t, obs.snapshot().requests, "denied:capacity")
}

func TestTunnel_ClientClosingMidTunnelReleasesItsSlot(t *testing.T) {
	auth := &fakeAuth{targets: map[string]string{"api.example.com:443": startEcho(t)}}
	p, obs, addr := startProxy(t, auth, Options{MaxTunnels: 1})
	conn, br, resp := connectVia(t, addr, "api.example.com:443", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_, err := conn.Write([]byte("ping"))
	require.NoError(t, err)
	_, err = io.ReadFull(br, make([]byte, 4))
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	require.Eventually(t, func() bool { return len(p.slots) == 0 }, 2*time.Second, 10*time.Millisecond)
	_, _, next := connectVia(t, addr, "api.example.com:443", "")
	assert.Equal(t, http.StatusOK, next.StatusCode)
	assert.NotContains(t, obs.snapshot().requests, "denied:capacity")
}

// The 200 that opens a tunnel is sent only once Connect has succeeded: while
// Connect is in flight the client receives nothing.
func TestTunnel_SendsNothingBeforeConnectSucceeds(t *testing.T) {
	echo := startEcho(t)
	entered, release := make(chan struct{}), make(chan struct{})
	auth := &fakeAuth{
		targets: map[string]string{"api.example.com:443": echo},
		connect: func(ctx context.Context, target string) (net.Conn, error) {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			var d net.Dialer
			return d.DialContext(ctx, "tcp", target)
		},
	}
	_, _, addr := startProxy(t, auth, Options{})
	c := sendConnect(t, addr, "api.example.com:443")
	waitFor(t, entered)

	require.NoError(t, c.SetReadDeadline(time.Now().Add(200*time.Millisecond)))
	n, err := c.Read(make([]byte, 1))
	assert.Zero(t, n, "no byte reaches the client while Connect is in flight")
	var ne net.Error
	require.ErrorAs(t, err, &ne)
	assert.True(t, ne.Timeout())

	close(release)
	require.NoError(t, c.SetReadDeadline(time.Now().Add(2*time.Second)))
	resp := readReply(t, bufio.NewReader(c), &http.Request{Method: http.MethodConnect})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// A failed Connect answers with its error status and nothing else: no 200
// precedes or follows it before the proxy closes the connection.
func TestTunnel_FailedConnectSendsOnlyItsErrorStatus(t *testing.T) {
	auth := &fakeAuth{
		targets: map[string]string{"api.example.com:443": "127.0.0.1:1"},
		err:     fmt.Errorf("%w: api.example.com:443: connection refused", authorize.ErrDial),
	}
	_, _, addr := startProxy(t, auth, Options{})
	c := sendConnect(t, addr, "api.example.com:443")
	reply := readUntilClosed(t, c)
	assert.True(t, strings.HasPrefix(reply, "HTTP/1.1 502 "), reply)
	assert.Equal(t, 1, strings.Count(reply, "HTTP/1.1 "), "one status line: %q", reply)
	assert.True(t, strings.HasSuffix(reply, "\r\n\r\negress-proxy: dial_failed\n"), reply)
}

// A client that goes away while Connect is in flight is neither a denial nor
// a dial failure: nothing is recorded and no denial is logged.
func TestTunnel_ClientLeavingDuringConnectIsNotRecorded(t *testing.T) {
	var logs syncBuffer
	entered := make(chan struct{})
	auth := &fakeAuth{
		targets: map[string]string{"api.example.com:443": "127.0.0.1:1"},
		connect: blockUntilCancelled(t, entered),
	}
	p, obs, addr := startProxyLogging(t, auth, Options{}, slog.New(slog.NewJSONHandler(&logs, nil)))
	c := sendConnect(t, addr, "api.example.com:443")
	waitFor(t, entered)
	require.NoError(t, c.Close())

	// The tunnel slot is released as the handler returns.
	require.Eventually(t, func() bool { return len(p.slots) == 0 }, 2*time.Second, 10*time.Millisecond)
	assert.Empty(t, obs.snapshot().requests)
	var messages []any
	for _, e := range logs.entries(t) {
		messages = append(messages, e["msg"])
	}
	assert.Contains(t, messages, "request cancelled")
	assert.NotContains(t, messages, "egress denied")
}

func TestTunnel_ClosesIdleTunnel(t *testing.T) {
	auth := &fakeAuth{targets: map[string]string{"api.example.com:443": startEcho(t)}}
	_, obs, addr := startProxy(t, auth, Options{IdleTimeout: 150 * time.Millisecond})
	conn, br, resp := connectVia(t, addr, "api.example.com:443", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	start := time.Now()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	_, err := br.ReadByte()
	require.Error(t, err)
	assert.Less(t, time.Since(start), 2*time.Second, "closed by the idle timeout, not the read deadline")
	require.Eventually(t, func() bool { return obs.snapshot().closed == 1 }, 2*time.Second, 10*time.Millisecond)
}

// The idle timeout counts from the latest byte in either direction, so a
// tunnel that only receives keeps no overall limit: a destination sending one
// byte every 30 ms outlives four idle timeouts and every byte arrives.
func TestTunnel_TrickleDownKeepsTunnelOpen(t *testing.T) {
	const idle, every, drips = 150 * time.Millisecond, 30 * time.Millisecond, 24
	upstream := startServer(t, func(c net.Conn) {
		for range drips {
			if _, err := c.Write([]byte("x")); err != nil {
				return
			}
			time.Sleep(every)
		}
	})
	auth := &fakeAuth{targets: map[string]string{"api.example.com:443": upstream}}
	_, _, addr := startProxy(t, auth, Options{IdleTimeout: idle})
	conn, br, resp := connectVia(t, addr, "api.example.com:443", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	start := time.Now()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	got, err := io.ReadAll(br)
	require.NoError(t, err)
	assert.Equal(t, strings.Repeat("x", drips), string(got), "every byte arrives")
	assert.Greater(t, time.Since(start), 4*idle, "the tunnel outlives the idle timeout")
}

// A tunnel that only sends stays open the same way: a client sending one
// byte every 30 ms to a destination that never replies outlives four idle
// timeouts and every byte arrives.
func TestTunnel_TrickleUpKeepsTunnelOpen(t *testing.T) {
	const idle, every, drips = 150 * time.Millisecond, 30 * time.Millisecond, 24
	received := make(chan int64, 1)
	upstream := startServer(t, func(c net.Conn) {
		n, _ := io.Copy(io.Discard, c)
		received <- n
	})
	auth := &fakeAuth{targets: map[string]string{"api.example.com:443": upstream}}
	_, _, addr := startProxy(t, auth, Options{IdleTimeout: idle})
	conn, _, resp := connectVia(t, addr, "api.example.com:443", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	start := time.Now()
	for i := range drips {
		_, err := conn.Write([]byte("y"))
		require.NoError(t, err, "write %d", i)
		time.Sleep(every)
	}
	require.NoError(t, conn.(*net.TCPConn).CloseWrite())
	select {
	case n := <-received:
		assert.Equal(t, int64(drips), n, "every byte arrives")
	case <-time.After(2 * time.Second):
		t.Fatal("the destination never saw the end of the stream")
	}
	assert.Greater(t, time.Since(start), 4*idle, "the tunnel outlives the idle timeout")
}

// A client that half-closes its side after its request still gets the reply
// the destination sends once it has read the whole request.
func TestTunnel_HalfCloseStillCarriesTheReply(t *testing.T) {
	upstream := startServer(t, func(c net.Conn) {
		request, err := io.ReadAll(c)
		if err != nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
		_, _ = c.Write([]byte("reply to " + string(request)))
	})
	auth := &fakeAuth{targets: map[string]string{"api.example.com:443": upstream}}
	_, _, addr := startProxy(t, auth, Options{})
	conn, br, resp := connectVia(t, addr, "api.example.com:443", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_, err := conn.Write([]byte("request"))
	require.NoError(t, err)
	require.NoError(t, conn.(*net.TCPConn).CloseWrite())

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	got, err := io.ReadAll(br)
	require.NoError(t, err)
	assert.Equal(t, "reply to request", string(got))
}

// A client that resets its connection ends the tunnel at once, even when the
// destination stays open and silent: its slot is free for the next CONNECT.
func TestTunnel_ClientResetFreesItsSlot(t *testing.T) {
	upstream := startServer(t, func(c net.Conn) {
		_, _ = io.Copy(io.Discard, c)
		<-t.Context().Done()
	})
	auth := &fakeAuth{targets: map[string]string{"api.example.com:443": upstream}}
	p, obs, addr := startProxy(t, auth, Options{MaxTunnels: 1})
	conn, _, resp := connectVia(t, addr, "api.example.com:443", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, conn.(*net.TCPConn).SetLinger(0))
	require.NoError(t, conn.Close())

	require.Eventually(t, func() bool { return len(p.slots) == 0 }, 2*time.Second, 10*time.Millisecond,
		"the tunnel slot is released well before the one-minute idle timeout")
	_, _, next := connectVia(t, addr, "api.example.com:443", "")
	assert.Equal(t, http.StatusOK, next.StatusCode)
	assert.NotContains(t, obs.snapshot().requests, "denied:capacity")
}

func TestServer_ClosesConnectionThatSendsNoHeaders(t *testing.T) {
	_, _, addr := startProxy(t, &fakeAuth{}, Options{HeaderTimeout: 150 * time.Millisecond})
	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.SetReadDeadline(time.Now().Add(3*time.Second)))
	start := time.Now()
	_, err = io.ReadAll(c)
	if ne, ok := err.(net.Error); ok {
		require.False(t, ne.Timeout(), "the proxy must close the connection, not leave it open")
	}
	assert.Less(t, time.Since(start), 2*time.Second)
}

// A keep-alive client connection with no request in flight is closed after
// the idle timeout.
func TestServer_ClosesIdleKeepAliveConnection(t *testing.T) {
	_, _, addr := startProxy(t, &fakeAuth{}, Options{IdleTimeout: 150 * time.Millisecond})
	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	_, err = io.WriteString(c, "GET /relative HTTP/1.1\r\nHost: api.example.com\r\n\r\n")
	require.NoError(t, err)
	br := bufio.NewReader(c)
	resp := readReply(t, br, nil)
	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)

	start := time.Now()
	require.NoError(t, c.SetReadDeadline(time.Now().Add(3*time.Second)))
	_, err = br.ReadByte()
	assert.ErrorIs(t, err, io.EOF)
	assert.Less(t, time.Since(start), 2*time.Second, "closed by the idle timeout, not the read deadline")
}

func TestServe_RefusesOtherRequests(t *testing.T) {
	_, obs, addr := startProxy(t, &fakeAuth{targets: map[string]string{"api.example.com:443": "127.0.0.1:1"}}, Options{})
	for _, text := range []string{
		"GET https://api.example.com/ HTTP/1.1\r\nHost: api.example.com\r\n\r\n",
		"GET /relative HTTP/1.1\r\nHost: api.example.com\r\n\r\n",
	} {
		resp := rawRequest(t, addr, text)
		assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode, strings.SplitN(text, "\r\n", 2)[0])
	}
	assert.Equal(t, []string{"denied:unsupported", "denied:unsupported"}, obs.snapshot().requests)
}

func TestShutdown_ClosesOpenTunnelsWhenGraceEnds(t *testing.T) {
	auth := &fakeAuth{targets: map[string]string{"api.example.com:443": startEcho(t)}}
	p, _, addr := startProxy(t, auth, Options{})
	conn, br, resp := connectVia(t, addr, "api.example.com:443", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, p.Shutdown(ctx))

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, err := br.ReadByte()
	require.Error(t, err)
	if ne, ok := err.(net.Error); ok {
		require.False(t, ne.Timeout(), "Shutdown must close the open tunnel")
	}
}

// A Connect still in flight when the grace ends is cancelled: Shutdown
// returns promptly, the client gets no reply, and nothing is recorded.
func TestShutdown_CancelsConnectInFlightWhenGraceEnds(t *testing.T) {
	entered := make(chan struct{})
	auth := &fakeAuth{
		targets: map[string]string{"api.example.com:443": "127.0.0.1:1"},
		connect: blockUntilCancelled(t, entered),
	}
	p, obs, addr := startProxy(t, auth, Options{})
	c := sendConnect(t, addr, "api.example.com:443")
	waitFor(t, entered)

	err := shutdown(t, p, 100*time.Millisecond)
	assert.ErrorIs(t, err, context.DeadlineExceeded, "the request was still being served when the grace ended")

	assert.Empty(t, readUntilClosed(t, c), "closed without a reply")
	assert.Empty(t, obs.snapshot().requests, "a cancelled Connect is neither a denial nor a dial failure")
}

// A Connect that succeeds after the grace has ended gives a tunnel that is
// closed at once, without a 200.
func TestShutdown_ClosesTunnelOpenedAfterGraceEnds(t *testing.T) {
	echo := startEcho(t)
	entered := make(chan struct{})
	auth := &fakeAuth{
		targets: map[string]string{"api.example.com:443": echo},
		connect: func(ctx context.Context, target string) (net.Conn, error) {
			close(entered)
			select {
			case <-ctx.Done():
			case <-t.Context().Done():
				return nil, errors.New("the test ended before Connect was cancelled")
			}
			var d net.Dialer
			return d.DialContext(context.Background(), "tcp", target)
		},
	}
	p, obs, addr := startProxy(t, auth, Options{})
	c := sendConnect(t, addr, "api.example.com:443")
	waitFor(t, entered)

	_ = shutdown(t, p, 100*time.Millisecond)

	assert.Empty(t, readUntilClosed(t, c), "closed without a 200")
	s := obs.snapshot()
	assert.Empty(t, s.requests)
	assert.Zero(t, s.opened)
}
