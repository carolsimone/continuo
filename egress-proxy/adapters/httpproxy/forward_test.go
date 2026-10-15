package httpproxy

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/netip"
	"net/textproto"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/egress-proxy/domain/allowlist"
	"github.com/carolsimone/continuo/egress-proxy/service/authorize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func proxiedClient(proxyAddr string) *http.Client {
	return &http.Client{
		Transport: &http.Transport{Proxy: http.ProxyURL(&url.URL{Scheme: "http", Host: proxyAddr})},
		Timeout:   5 * time.Second,
	}
}

// received is what a destination saw of one request.
type received struct {
	Host       string
	RequestURI string
	Header     http.Header
	Body       string
}

// destination is an HTTP server standing in for an allowlisted host. It keeps
// what every request it receives looked like, body included, and answers
// with answer, or with an empty 200 when answer is nil.
type destination struct {
	addr string
	mu   sync.Mutex
	got  []received
}

func startDestination(t *testing.T, answer func(http.ResponseWriter, received)) *destination {
	t.Helper()
	d := &destination{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got := received{Host: r.Host, RequestURI: r.RequestURI, Header: r.Header.Clone(), Body: string(body)}
		d.mu.Lock()
		d.got = append(d.got, got)
		d.mu.Unlock()
		if answer != nil {
			answer(w, got)
		}
	}))
	t.Cleanup(srv.Close)
	d.addr = srv.Listener.Addr().String()
	return d
}

func (d *destination) requests() []received {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]received(nil), d.got...)
}

// get is the text of an absolute-form GET for target with the given Host
// header and extra header lines.
func get(target, host, extra string) string {
	return "GET " + target + " HTTP/1.1\r\nHost: " + host + "\r\n" + extra + "\r\n"
}

// fixedResolver answers every lookup with addrs.
type fixedResolver struct{ addrs []netip.Addr }

func (r fixedResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	return r.addrs, nil
}

func TestForward_PlainHTTPStripsHopByHopHeaders(t *testing.T) {
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		_, _ = io.WriteString(w, "models")
	}))
	defer srv.Close()
	auth := &fakeAuth{targets: map[string]string{"model.internal:8000": srv.Listener.Addr().String()}}
	_, obs, addr := startProxy(t, auth, Options{})

	req, err := http.NewRequest(http.MethodGet, "http://model.internal:8000/v1/models", nil)
	require.NoError(t, err)
	req.Header.Set("Connection", "X-Hop")
	req.Header.Set("X-Hop", "1")
	req.Header.Set("X-Keep", "1")
	req.Header.Set("Proxy-Authorization", "Basic c2VjcmV0")
	resp, err := proxiedClient(addr).Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "models", string(body))
	assert.Equal(t, "1", got.Get("X-Keep"))
	assert.Empty(t, got.Get("X-Hop"))
	assert.Empty(t, got.Get("Proxy-Authorization"))
	assert.Empty(t, got.Get("X-Forwarded-For"))
	assert.Empty(t, got.Get("Via"))
	require.Eventually(t, func() bool {
		s := obs.snapshot()
		return len(s.requests) == 1 && s.requests[0] == "allowed:ok" && s.down == 6
	}, 2*time.Second, 10*time.Millisecond)
}

// Proxy credentials and connection options stop at the proxy, and so do the
// forwarding headers a client sent; the proxy adds none of its own. Headers
// meant for the destination, Authorization among them, pass unchanged. On the
// way back, the destination's hop-by-hop headers stop at the proxy too.
func TestForward_StripsProxyAndForwardingHeaders(t *testing.T) {
	dest := startDestination(t, func(w http.ResponseWriter, _ received) {
		w.Header().Set("Proxy-Authenticate", `Basic realm="destination"`)
		w.Header().Set("Connection", "X-Reply-Hop")
		w.Header().Set("X-Reply-Hop", "1")
		w.Header().Set("X-Reply-Keep", "1")
	})
	auth := &fakeAuth{targets: map[string]string{"model.internal:8000": dest.addr}}
	_, _, addr := startProxy(t, auth, Options{})

	header, resp := exchange(t, addr, get("http://model.internal:8000/v1/models", "model.internal:8000",
		"Proxy-Connection: keep-alive\r\n"+
			"Proxy-Authorization: Basic c2VjcmV0\r\n"+
			"Proxy-Authenticate: Basic realm=\"agent\"\r\n"+
			"Keep-Alive: timeout=5\r\n"+
			"Connection: X-Hop\r\n"+
			"X-Hop: 1\r\n"+
			"Forwarded: for=10.0.0.1\r\n"+
			"X-Forwarded-For: 10.0.0.1\r\n"+
			"X-Forwarded-Host: internal.example\r\n"+
			"X-Forwarded-Proto: https\r\n"+
			"X-Forwarded-Port: 443\r\n"+
			"x-forwarded-prefix: /admin\r\n"+
			"X-Forwarded-Server: internal\r\n"+
			"Via: 1.1 agent-side\r\n"+
			"Authorization: Bearer agent-token\r\n"+
			"X-Keep: 1\r\n"))
	require.Equal(t, http.StatusOK, resp.StatusCode)

	got := dest.requests()
	require.Len(t, got, 1)
	for _, name := range []string{
		"Proxy-Connection", "Proxy-Authorization", "Proxy-Authenticate", "Keep-Alive", "X-Hop",
		"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto",
		"X-Forwarded-Port", "X-Forwarded-Prefix", "X-Forwarded-Server",
	} {
		assert.Empty(t, got[0].Header.Values(name), name)
	}
	assert.Equal(t, []string{"1.1 agent-side"}, got[0].Header.Values("Via"), "the client's Via passes; the proxy adds none")
	assert.Equal(t, []string{"Bearer agent-token"}, got[0].Header.Values("Authorization"))
	assert.Equal(t, []string{"1"}, got[0].Header.Values("X-Keep"))

	assert.Empty(t, header.Values("Proxy-Authenticate"), "a destination never asks the agent for proxy credentials")
	assert.Empty(t, header.Values("X-Reply-Hop"))
	assert.Empty(t, header.Values("Via"))
	assert.Equal(t, []string{"1"}, header.Values("X-Reply-Keep"))
}

// A streamed reply reaches the client chunk by chunk: the first event arrives
// while the server is still holding the second.
func TestForward_StreamsResponsesAsTheyArrive(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: one\n\n")
		w.(http.Flusher).Flush()
		<-release
		_, _ = io.WriteString(w, "data: two\n\n")
	}))
	defer srv.Close()
	defer close(release)
	auth := &fakeAuth{targets: map[string]string{"model.internal:8000": srv.Listener.Addr().String()}}
	_, _, addr := startProxy(t, auth, Options{})

	resp, err := proxiedClient(addr).Get("http://model.internal:8000/v1/chat/completions")
	require.NoError(t, err)
	defer resp.Body.Close()
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	require.NoError(t, err, "the first event must arrive before the server finishes")
	assert.Equal(t, "data: one\n", line)
}

// A reply of declared length, which is neither an event stream nor chunked,
// is passed on write by write too: its headers and first write arrive while
// the destination is still holding the rest.
func TestForward_FlushesEachWriteOfAFixedLengthReply(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	dest := startDestination(t, func(w http.ResponseWriter, _ received) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "20")
		_, _ = io.WriteString(w, "0123456789")
		w.(http.Flusher).Flush()
		<-release
		_, _ = io.WriteString(w, "abcdefghij")
	})
	auth := &fakeAuth{targets: map[string]string{"model.internal:8000": dest.addr}}
	_, _, addr := startProxy(t, auth, Options{})

	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.SetReadDeadline(time.Now().Add(time.Second)))
	_, err = io.WriteString(c, get("http://model.internal:8000/v1/embeddings", "model.internal:8000", ""))
	require.NoError(t, err)
	br := bufio.NewReader(c)
	tp := textproto.NewReader(br)
	status, err := tp.ReadLine()
	require.NoError(t, err, "the headers must arrive before the reply is complete")
	assert.Equal(t, "HTTP/1.1 200 OK", status)
	header, err := tp.ReadMIMEHeader()
	require.NoError(t, err)
	assert.Equal(t, "20", header.Get("Content-Length"))
	first := make([]byte, 10)
	_, err = io.ReadFull(br, first)
	require.NoError(t, err, "the first write must arrive before the reply is complete")
	assert.Equal(t, "0123456789", string(first))
}

func TestForward_RefusesHostNotOnAllowlist(t *testing.T) {
	_, obs, addr := startProxy(t, &fakeAuth{targets: map[string]string{}}, Options{})
	resp, err := proxiedClient(addr).Get("http://other.internal:8000/")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.Equal(t, []string{"denied:not_allowed"}, obs.snapshot().requests)
}

// A request the Authorizer refuses at connect time is recorded once, as
// denied, and its reply names only the reason.
func TestForward_DeniedDialIsRecordedOnce(t *testing.T) {
	auth := &fakeAuth{
		targets: map[string]string{"model.internal:8000": "127.0.0.1:1"},
		err:     &authorize.Denied{Reason: authorize.ReasonPrivateAddress, Host: "model.internal", Port: 8000, Detail: "10.0.0.9 is private"},
	}
	_, obs, addr := startProxy(t, auth, Options{})
	resp, err := proxiedClient(addr).Get("http://model.internal:8000/")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.NotContains(t, string(body), "10.0.0.9")
	assert.Equal(t, "egress-proxy: private_address\n", string(body))
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, []string{"denied:private_address"}, obs.snapshot().requests)
}

// A destination that takes the request and closes without answering is an
// upstream error, recorded once.
func TestForward_TransportFailureIsRecordedOnce(t *testing.T) {
	dest := startServer(t, func(c net.Conn) {
		_, _ = http.ReadRequest(bufio.NewReader(c))
	})
	auth := &fakeAuth{targets: map[string]string{"model.internal:8000": dest}}
	_, obs, addr := startProxy(t, auth, Options{})

	_, resp := exchange(t, addr, get("http://model.internal:8000/", "model.internal:8000", ""))
	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
	assert.Equal(t, "egress-proxy: upstream_error\n", resp.Body)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, []string{"denied:upstream_error"}, obs.snapshot().requests)
	assert.Equal(t, int32(1), auth.connects.Load())
}

// A URL without a port goes to port 80: it is admitted by an entry for port
// 80 and by no other.
func TestForward_DefaultsToPort80(t *testing.T) {
	dest := startDestination(t, func(w http.ResponseWriter, _ received) { _, _ = io.WriteString(w, "on 80") })
	auth := &fakeAuth{targets: map[string]string{"model.internal:80": dest.addr, "other.internal:8000": dest.addr}}
	_, obs, addr := startProxy(t, auth, Options{})

	resp, err := proxiedClient(addr).Get("http://model.internal/")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "on 80", string(body))

	refused, err := proxiedClient(addr).Get("http://other.internal/")
	require.NoError(t, err)
	defer refused.Body.Close()
	assert.Equal(t, http.StatusForbidden, refused.StatusCode)

	require.Eventually(t, func() bool {
		s := obs.snapshot().requests
		return len(s) == 2 && s[0] == "allowed:ok" && s[1] == "denied:not_allowed"
	}, 2*time.Second, 10*time.Millisecond)
	assert.Equal(t, int32(1), auth.connects.Load())
}

func TestForward_RefusesMalformedTargets(t *testing.T) {
	auth := &fakeAuth{targets: map[string]string{
		"model.internal:80":   "127.0.0.1:1",
		"model.internal:8000": "127.0.0.1:1",
	}}
	_, obs, addr := startProxy(t, auth, Options{})

	var want []string
	for _, target := range []string{
		"http://model.internal:0/",
		"http://model.internal:65536/",
		"http://model.internal:99999/",
		"http://model.internal:/",
		"http:///v1/models",
	} {
		_, resp := exchange(t, addr, get(target, "model.internal:8000", ""))
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode, target)
		assert.Equal(t, "egress-proxy: bad_request\n", resp.Body, target)
		want = append(want, "denied:bad_request")
	}
	// A port that is not a number fails net/http's own request parsing: the
	// server answers 400 before the proxy's handler runs, so nothing is
	// recorded.
	_, resp := exchange(t, addr, get("http://model.internal:http/", "model.internal", ""))
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	assert.Equal(t, want, obs.snapshot().requests)
	assert.Zero(t, auth.connects.Load())
}

// An IP-literal host never matches the allowlist, whose entries are names: it
// is refused before Connect, so before any lookup or dial. The proxy runs on
// the real Authorizer and allowlist here.
func TestForward_RefusesIPLiteralHosts(t *testing.T) {
	allow, err := allowlist.New([]allowlist.Entry{{Host: "model.internal", Port: 80}, {Host: "model.internal", Port: 8000}})
	require.NoError(t, err)
	resolver, dialer := &countingResolver{}, &countingDialer{}
	auth := &countingAuthorizer{Authorizer: authorize.New(allow, resolver, dialer)}
	_, obs, addr := startProxy(t, auth, Options{})

	var want []string
	for _, target := range []string{
		"http://[::1]:80/",
		"http://127.0.0.1/",
		"http://10.0.0.1:8000/",
		"http://169.254.169.254/latest/meta-data/",
		"http://[::ffff:127.0.0.1]/",
		"http://127.1/",
		"http://2130706433/",
	} {
		_, resp := exchange(t, addr, get(target, "model.internal", ""))
		assert.Equal(t, http.StatusForbidden, resp.StatusCode, target)
		assert.Equal(t, "egress-proxy: not_allowed\n", resp.Body, target)
		want = append(want, "denied:not_allowed")
	}
	assert.Equal(t, want, obs.snapshot().requests)
	assert.Zero(t, auth.connects.Load(), "Connect is never called for an IP literal")
	assert.Zero(t, resolver.calls.Load(), "no lookup for an IP literal")
	assert.Zero(t, dialer.calls.Load(), "no dial for an IP literal")
}

// The URL's host is both what the allowlist checks and what the destination
// is told it is: a Host header naming another host neither reaches the
// destination nor admits the request.
func TestForward_HostHeaderNeverChoosesTheDestination(t *testing.T) {
	dest := startDestination(t, nil)
	auth := &fakeAuth{targets: map[string]string{"allowed.internal:8000": dest.addr}}
	_, _, addr := startProxy(t, auth, Options{})
	_, resp := exchange(t, addr, get("http://allowed.internal:8000/v1/models", "evil.example", ""))
	require.Equal(t, http.StatusOK, resp.StatusCode)
	got := dest.requests()
	require.Len(t, got, 1)
	assert.Equal(t, "allowed.internal:8000", got[0].Host)
	assert.Equal(t, "/v1/models", got[0].RequestURI)

	// Only the Host header names an allowlisted host here.
	other := &fakeAuth{targets: map[string]string{"evil.example:8000": dest.addr}}
	_, obs, otherAddr := startProxy(t, other, Options{})
	_, resp = exchange(t, otherAddr, get("http://allowed.internal:8000/", "evil.example:8000", ""))
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.Equal(t, []string{"denied:not_allowed"}, obs.snapshot().requests)
	assert.Zero(t, other.connects.Load())
	assert.Len(t, dest.requests(), 1)
}

// The outbound request names the URL's host whatever Host the inbound one
// carries, carries no userinfo, and has no X-Forwarded-* header.
func TestForwarder_RewriteTargetsTheURLHost(t *testing.T) {
	fw := newForwarder(&fakeAuth{}, nil, nil)
	in := httptest.NewRequest(http.MethodGet, "http://user:pw@allowed.internal:8000/v1/models", nil)
	in.Host = "evil.example"
	in.Header.Set("X-Forwarded-Port", "443")
	in.Header.Set("X-Forwarded-Server", "internal")
	in.Header.Set("Forwarded", "for=10.0.0.1")
	out := in.Clone(context.Background())

	fw.Rewrite(&httputil.ProxyRequest{In: in, Out: out})

	assert.Equal(t, "allowed.internal:8000", out.Host)
	assert.Equal(t, "allowed.internal:8000", out.URL.Host)
	assert.Equal(t, "http", out.URL.Scheme)
	assert.Equal(t, "/v1/models", out.URL.Path)
	assert.Nil(t, out.URL.User)
	assert.Empty(t, out.Header.Values("X-Forwarded-Port"))
	assert.Empty(t, out.Header.Values("X-Forwarded-Server"))
	assert.Empty(t, out.Header.Values("Forwarded"))
}

// Userinfo in the URL changes neither the host that is checked nor the
// credentials the destination sees: the request goes out with the
// Authorization header the client set, if any, and no other.
func TestForward_UserinfoNeitherChangesTheCheckNorAddsCredentials(t *testing.T) {
	dest := startDestination(t, nil)
	auth := &fakeAuth{targets: map[string]string{"allowed.internal:8000": dest.addr}}
	_, obs, addr := startProxy(t, auth, Options{})

	_, resp := exchange(t, addr, get("http://user:pw@allowed.internal:8000/v1/models", "allowed.internal:8000", ""))
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_, resp = exchange(t, addr, get("http://user:pw@allowed.internal:8000/v1/models", "allowed.internal:8000",
		"Authorization: Bearer agent-token\r\n"))
	require.Equal(t, http.StatusOK, resp.StatusCode)
	// The allowlisted name in the userinfo does not admit another host.
	_, resp = exchange(t, addr, get("http://allowed.internal:8000@evil.example:8000/", "evil.example:8000", ""))
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)

	got := dest.requests()
	require.Len(t, got, 2)
	assert.Empty(t, got[0].Header.Values("Authorization"), "no credentials are made from the userinfo")
	assert.Equal(t, "/v1/models", got[0].RequestURI)
	assert.Equal(t, "allowed.internal:8000", got[0].Host)
	assert.Equal(t, []string{"Bearer agent-token"}, got[1].Header.Values("Authorization"))
	require.Eventually(t, func() bool {
		s := obs.snapshot().requests
		return len(s) == 3 && s[0] == "allowed:ok" && s[1] == "allowed:ok" && s[2] == "denied:not_allowed"
	}, 2*time.Second, 10*time.Millisecond)
}

// A request that asks to switch protocols is refused before anything is sent
// to the destination: the proxy never relays an upgraded connection.
func TestForward_RefusesProtocolUpgrades(t *testing.T) {
	dest := startDestination(t, nil)
	auth := &fakeAuth{targets: map[string]string{"model.internal:8000": dest.addr}}
	_, obs, addr := startProxy(t, auth, Options{})

	var want []string
	for _, extra := range []string{
		"Connection: Upgrade\r\nUpgrade: websocket\r\n",
		"Upgrade: h2c\r\n",
		"Connection: keep-alive, upgrade\r\n",
	} {
		_, resp := exchange(t, addr, get("http://model.internal:8000/ws", "model.internal:8000", extra))
		assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode, extra)
		assert.Equal(t, "egress-proxy: unsupported\n", resp.Body, extra)
		want = append(want, "denied:unsupported")
	}
	assert.Equal(t, want, obs.snapshot().requests)
	assert.Empty(t, dest.requests(), "the destination never receives the request")
	assert.Zero(t, auth.connects.Load())
}

// A destination that answers 101 to a request that asked for no protocol
// switch is refused, and its connection is closed rather than left open.
func TestForward_RefusesAProtocolSwitchItDidNotAskFor(t *testing.T) {
	closed := make(chan struct{})
	closeOnce := sync.OnceFunc(func() { close(closed) })
	dest := startServer(t, func(c net.Conn) {
		br := bufio.NewReader(c)
		if _, err := http.ReadRequest(br); err != nil {
			return
		}
		_, _ = io.WriteString(c, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
		_, _ = io.Copy(io.Discard, br)
		closeOnce()
	})
	auth := &fakeAuth{targets: map[string]string{"model.internal:8000": dest}}
	_, obs, addr := startProxy(t, auth, Options{})

	_, resp := exchange(t, addr, get("http://model.internal:8000/", "model.internal:8000", ""))
	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
	assert.Equal(t, "egress-proxy: upstream_error\n", resp.Body)
	awaitClosed(t, closed, "the proxy must close the switched connection")
	assert.Equal(t, []string{"denied:upstream_error"}, obs.snapshot().requests)
}

// A request body reaches the destination byte for byte, whether its length
// is declared or it is chunked, and its bytes are counted up; the reply's
// body bytes are counted down.
func TestForward_ForwardsRequestBodyAndCountsBytes(t *testing.T) {
	dest := startDestination(t, func(w http.ResponseWriter, got received) {
		sum := sha256.Sum256([]byte(got.Body))
		_, _ = io.WriteString(w, hex.EncodeToString(sum[:]))
	})
	auth := &fakeAuth{targets: map[string]string{"model.internal:8000": dest.addr}}
	_, obs, addr := startProxy(t, auth, Options{})

	payload := bytes.Repeat([]byte(`{"prompt":"0123456789"}`), 3000)
	sum := sha256.Sum256(payload)
	for name, body := range map[string]io.Reader{
		"declared length": bytes.NewReader(payload),
		"chunked":         io.MultiReader(bytes.NewReader(payload)),
	} {
		resp, err := proxiedClient(addr).Post("http://model.internal:8000/v1/chat/completions", "application/json", body)
		require.NoError(t, err, name)
		reply, err := io.ReadAll(resp.Body)
		require.NoError(t, err, name)
		require.NoError(t, resp.Body.Close())
		assert.Equal(t, http.StatusOK, resp.StatusCode, name)
		assert.Equal(t, hex.EncodeToString(sum[:]), string(reply), name)
	}

	got := dest.requests()
	require.Len(t, got, 2)
	for _, r := range got {
		assert.Equal(t, string(payload), r.Body)
	}
	require.Eventually(t, func() bool {
		s := obs.snapshot()
		return len(s.requests) == 2 && s.requests[0] == "allowed:ok" && s.requests[1] == "allowed:ok" &&
			s.up == int64(2*len(payload)) && s.down == int64(2*hex.EncodedLen(sha256.Size))
	}, 2*time.Second, 10*time.Millisecond)
}

// A redirect is the client's to follow: the proxy passes it on untouched and
// sends nothing to the host it names.
func TestForward_DoesNotFollowRedirects(t *testing.T) {
	dest := startDestination(t, func(w http.ResponseWriter, _ received) {
		w.Header().Set("Location", "http://evil.example/collect")
		w.WriteHeader(http.StatusFound)
	})
	auth := &fakeAuth{targets: map[string]string{"model.internal:8000": dest.addr}}
	_, obs, addr := startProxy(t, auth, Options{})

	header, resp := exchange(t, addr, get("http://model.internal:8000/start", "model.internal:8000", ""))
	assert.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Equal(t, "http://evil.example/collect", header.Get("Location"))
	require.Eventually(t, func() bool {
		s := obs.snapshot().requests
		return len(s) == 1 && s[0] == "allowed:ok"
	}, 2*time.Second, 10*time.Millisecond)
	assert.Len(t, dest.requests(), 1)
	assert.Equal(t, int32(1), auth.connects.Load(), "no second connection is opened")
}

// Accept-Encoding and the reply's body pass through as they are: the proxy
// neither asks for compression nor undoes it.
func TestForward_PassesEncodingThrough(t *testing.T) {
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	_, err := io.WriteString(zw, "a compressed reply")
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	dest := startDestination(t, func(w http.ResponseWriter, got received) {
		if got.Header.Get("Accept-Encoding") == "gzip" {
			w.Header().Set("Content-Encoding", "gzip")
			_, _ = w.Write(compressed.Bytes())
			return
		}
		_, _ = io.WriteString(w, "plain")
	})
	auth := &fakeAuth{targets: map[string]string{"model.internal:8000": dest.addr}}
	_, _, addr := startProxy(t, auth, Options{})

	_, plain := exchange(t, addr, get("http://model.internal:8000/", "model.internal:8000", ""))
	assert.Equal(t, "plain", plain.Body)
	header, zipped := exchange(t, addr, get("http://model.internal:8000/", "model.internal:8000", "Accept-Encoding: gzip\r\n"))
	assert.Equal(t, "gzip", header.Get("Content-Encoding"))
	assert.Equal(t, compressed.String(), zipped.Body, "the compressed bytes arrive as sent")

	got := dest.requests()
	require.Len(t, got, 2)
	assert.Empty(t, got[0].Header.Values("Accept-Encoding"), "the proxy asks for no encoding the client did not")
	assert.Equal(t, []string{"gzip"}, got[1].Header.Values("Accept-Encoding"))
}

// With the real Authorizer, a name that resolves to a public and a private
// address is refused for an entry without allowPrivate: nothing is dialled
// and the client sees only the reason.
func TestForward_RefusesNameResolvingToAPrivateAddress(t *testing.T) {
	allow, err := allowlist.New([]allowlist.Entry{{Host: "model.example.com", Port: 80}})
	require.NoError(t, err)
	resolver := fixedResolver{addrs: []netip.Addr{netip.MustParseAddr("160.79.104.10"), netip.MustParseAddr("10.0.0.9")}}
	dialer := &countingDialer{}
	_, obs, addr := startProxy(t, authorize.New(allow, resolver, dialer), Options{})

	_, resp := exchange(t, addr, get("http://model.example.com/v1/models", "model.example.com", ""))
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.Equal(t, "egress-proxy: private_address\n", resp.Body)
	assert.NotContains(t, resp.Body, "10.0.0.9")
	assert.NotContains(t, resp.Body, "160.79.104.10")
	assert.Equal(t, []string{"denied:private_address"}, obs.snapshot().requests)
	assert.Zero(t, dialer.calls.Load(), "nothing is dialled")
}

// A destination that sends its headers and part of its body, then drops the
// connection, has answered: the request is recorded once, as allowed, with
// the bytes that reached the client.
func TestForward_RecordsAnAnswerOnceWhenTheReplyBreaksOff(t *testing.T) {
	dest := startServer(t, func(c net.Conn) {
		if _, err := http.ReadRequest(bufio.NewReader(c)); err != nil {
			return
		}
		_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\npartial")
	})
	auth := &fakeAuth{targets: map[string]string{"model.internal:8000": dest}}
	_, obs, addr := startProxy(t, auth, Options{})

	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.SetDeadline(time.Now().Add(2*time.Second)))
	_, err = io.WriteString(c, get("http://model.internal:8000/", "model.internal:8000", ""))
	require.NoError(t, err)
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF, "the proxy ends the reply short, as the destination did")
	assert.Equal(t, "partial", string(body))

	require.Eventually(t, func() bool {
		s := obs.snapshot()
		return len(s.requests) == 1 && s.down == int64(len("partial"))
	}, 2*time.Second, 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, []string{"allowed:ok"}, obs.snapshot().requests)
}

// A client that goes away before the destination answers is neither a
// denial nor an upstream failure: its Connect is cancelled, nothing is
// recorded and no denial is logged.
func TestForward_ClientLeavingBeforeAnAnswerIsNotRecorded(t *testing.T) {
	var logs syncBuffer
	entered, ended := make(chan struct{}), make(chan struct{})
	auth := &fakeAuth{
		targets: map[string]string{"model.internal:8000": "127.0.0.1:1"},
		connect: connectUntilCancelled(t, entered, ended),
	}
	_, obs, addr := startProxyLogging(t, auth, Options{}, slog.New(slog.NewJSONHandler(&logs, nil)))
	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	_, err = io.WriteString(c, get("http://model.internal:8000/", "model.internal:8000", ""))
	require.NoError(t, err)
	waitFor(t, entered)
	require.NoError(t, c.Close())

	awaitClosed(t, ended, "the client leaving must cancel its Connect")
	require.Eventually(t, func() bool { return logs.logged("request cancelled") }, 2*time.Second, 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	assert.Empty(t, obs.snapshot().requests)
	assert.False(t, logs.logged("egress denied"))
	assert.False(t, logs.logged("request forwarded"))
}

// A Connect still in flight for a forwarded request when the shutdown grace
// ends is cancelled: Shutdown returns promptly, the client gets no reply, and
// nothing is recorded.
func TestShutdown_CancelsForwardedConnectInFlightWhenGraceEnds(t *testing.T) {
	entered, ended := make(chan struct{}), make(chan struct{})
	auth := &fakeAuth{
		targets: map[string]string{"model.internal:8000": "127.0.0.1:1"},
		connect: connectUntilCancelled(t, entered, ended),
	}
	p, obs, addr := startProxy(t, auth, Options{})
	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	_, err = io.WriteString(c, get("http://model.internal:8000/", "model.internal:8000", ""))
	require.NoError(t, err)
	waitFor(t, entered)

	err = shutdown(t, p, 100*time.Millisecond)
	assert.ErrorIs(t, err, context.DeadlineExceeded, "the request was still being served when the grace ended")
	awaitClosed(t, ended, "the grace ending must cancel the Connect")
	assert.Empty(t, readUntilClosed(t, c), "closed without a reply")
	assert.Empty(t, obs.snapshot().requests)
}

// The path and query reach the destination as the client wrote them, a query
// net/url cannot parse included.
func TestForward_KeepsThePathAndQueryAsWritten(t *testing.T) {
	dest := startDestination(t, nil)
	auth := &fakeAuth{targets: map[string]string{"model.internal:8000": dest.addr}}
	_, _, addr := startProxy(t, auth, Options{})

	for _, uri := range []string{
		"/v1/models?limit=10&after=a%2Fb",
		"/search?q=a;b&x=%zz",
		"/a%2Fb/c?",
	} {
		_, resp := exchange(t, addr, get("http://model.internal:8000"+uri, "model.internal:8000", ""))
		require.Equal(t, http.StatusOK, resp.StatusCode, uri)
	}
	got := dest.requests()
	require.Len(t, got, 3)
	assert.Equal(t, "/v1/models?limit=10&after=a%2Fb", got[0].RequestURI)
	assert.Equal(t, "/search?q=a;b&x=%zz", got[1].RequestURI)
	assert.Equal(t, "/a%2Fb/c?", got[2].RequestURI)
}

// Each forwarded request is logged at INFO once it ends, with its target,
// client, method, status and byte counts.
func TestForward_LogsTheForwardedRequest(t *testing.T) {
	var logs syncBuffer
	dest := startDestination(t, func(w http.ResponseWriter, _ received) { _, _ = io.WriteString(w, "models") })
	auth := &fakeAuth{targets: map[string]string{"model.internal:8000": dest.addr}}
	_, _, addr := startProxyLogging(t, auth, Options{}, slog.New(slog.NewJSONHandler(&logs, nil)))

	_, resp := exchange(t, addr, "POST http://model.internal:8000/v1/models HTTP/1.1\r\n"+
		"Host: model.internal:8000\r\nContent-Length: 5\r\n\r\nhello")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Eventually(t, func() bool { return logs.logged("request forwarded") }, 2*time.Second, 10*time.Millisecond)

	var forwarded []map[string]any
	for _, e := range logs.entries(t) {
		if e["msg"] == "request forwarded" {
			forwarded = append(forwarded, e)
		}
	}
	require.Len(t, forwarded, 1)
	e := forwarded[0]
	assert.Equal(t, "INFO", e["level"])
	assert.Equal(t, "model.internal", e["host"])
	assert.Equal(t, float64(8000), e["port"])
	assert.NotEmpty(t, e["client"])
	assert.Equal(t, http.MethodPost, e["method"])
	assert.Equal(t, float64(http.StatusOK), e["status"])
	assert.Equal(t, float64(5), e["bytes_up"])
	assert.Equal(t, float64(6), e["bytes_down"])
	assert.Equal(t, false, e["aborted"])
}

// The forwarder's transport reaches destinations only through the
// Authorizer, never through another proxy, speaks only HTTP/1.1, and puts no
// overall limit on how long a reply may take.
func TestForwarder_TransportSettings(t *testing.T) {
	fw := newForwarder(&fakeAuth{}, nil, nil)
	tr, ok := fw.Transport.(*http.Transport)
	require.True(t, ok)
	assert.Nil(t, tr.Proxy, "no proxy, whatever HTTP_PROXY says")
	assert.NotNil(t, tr.DialContext)
	assert.Nil(t, tr.DialTLSContext)
	assert.False(t, tr.ForceAttemptHTTP2)
	require.NotNil(t, tr.Protocols)
	assert.True(t, tr.Protocols.HTTP1())
	assert.False(t, tr.Protocols.HTTP2())
	assert.False(t, tr.Protocols.UnencryptedHTTP2())
	assert.Equal(t, 16, tr.MaxIdleConnsPerHost)
	assert.Equal(t, 90*time.Second, tr.IdleConnTimeout)
	assert.Zero(t, tr.ResponseHeaderTimeout, "a reply may take minutes to start")
	assert.True(t, tr.DisableCompression, "Accept-Encoding and the body pass through")
	assert.NotNil(t, fw.Rewrite)
	assert.Equal(t, time.Duration(-1), fw.FlushInterval)
}

// A forwarded request still waiting on a silent destination when the
// shutdown grace ends is cancelled: Shutdown returns, the client's
// connection is closed without a reply, the destination's connection is
// closed too, and nothing is recorded.
func TestShutdown_EndsForwardedRequestWaitingOnSilentDestination(t *testing.T) {
	arrived, gone := make(chan struct{}), make(chan struct{})
	dest := startServer(t, func(c net.Conn) {
		br := bufio.NewReader(c)
		if _, err := http.ReadRequest(br); err != nil {
			return
		}
		close(arrived)
		_, _ = io.Copy(io.Discard, br)
		close(gone)
	})
	auth := &fakeAuth{targets: map[string]string{"model.internal:8000": dest}}
	p, obs, addr := startProxy(t, auth, Options{})
	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	_, err = io.WriteString(c, get("http://model.internal:8000/v1/chat/completions", "model.internal:8000", ""))
	require.NoError(t, err)
	awaitClosed(t, arrived, "the request must reach the destination")

	err = shutdown(t, p, 100*time.Millisecond)
	assert.ErrorIs(t, err, context.DeadlineExceeded, "the request was still being served when the grace ended")
	assert.Empty(t, readUntilClosed(t, c), "closed without a reply")
	awaitClosed(t, gone, "the destination's connection must be closed")
	assert.Empty(t, obs.snapshot().requests)
}

// Shutdown closes the kept-alive connections to destinations that the
// forwarder holds for reuse.
func TestShutdown_ClosesIdleDestinationConnections(t *testing.T) {
	gone := make(chan struct{})
	dest := startServer(t, func(c net.Conn) {
		br := bufio.NewReader(c)
		if _, err := http.ReadRequest(br); err != nil {
			return
		}
		_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
		// The connection stays open for a next request until the proxy
		// closes it.
		_, _ = io.Copy(io.Discard, br)
		close(gone)
	})
	auth := &fakeAuth{targets: map[string]string{"model.internal:8000": dest}}
	p, _, addr := startProxy(t, auth, Options{})
	_, resp := exchange(t, addr, get("http://model.internal:8000/", "model.internal:8000", ""))
	require.Equal(t, "ok", resp.Body)

	select {
	case <-gone:
		t.Fatal("the destination's connection must be kept for reuse until Shutdown")
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, shutdown(t, p, time.Second))
	awaitClosed(t, gone, "Shutdown must close the idle destination connection")
}
