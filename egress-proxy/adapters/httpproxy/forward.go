package httpproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"strings"
	"sync/atomic"
	"time"

	"github.com/carolsimone/continuo/egress-proxy/service/authorize"
)

// errSwitchedProtocols fails a 101 reply. The proxy forwards no request that
// asks to switch protocols, so a destination that switches anyway is refused
// rather than relayed as an open connection.
var errSwitchedProtocols = errors.New("the destination switched protocols on a request that asked for none")

// requestContextKey carries a forwarded request's own context to the dial the
// transport makes for it. The transport dials with a context that keeps the
// request's values but not its cancellation.
type requestContextKey struct{}

// newForwarder returns the reverse proxy that carries absolute-form
// plain-HTTP requests. Its transport opens every connection through
// Authorizer.Connect and never through another proxy. failed answers a
// request it cannot carry; errorLog receives its internal errors.
func newForwarder(auth Authorizer, failed func(http.ResponseWriter, *http.Request, error), errorLog *log.Logger) *httputil.ReverseProxy {
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	return &httputil.ReverseProxy{
		// Before Rewrite runs, ReverseProxy has removed the request's
		// hop-by-hop headers, Proxy-Authorization, Proxy-Authenticate and
		// Proxy-Connection among them, and its Forwarded, X-Forwarded-For,
		// X-Forwarded-Host and X-Forwarded-Proto headers. With Rewrite set it
		// adds no forwarding header of its own. It removes the reply's
		// hop-by-hop headers the same way.
		Rewrite: func(pr *httputil.ProxyRequest) {
			// The destination is told the host the allowlist admitted, the
			// URL's, never a Host header naming another.
			pr.Out.Host = pr.Out.URL.Host
			// No userinfo goes out with the URL: the destination sees the
			// Authorization header the client set, if any, and no other.
			pr.Out.URL.User = nil
			// ReverseProxy re-encodes a query net/url cannot parse, dropping
			// the parameters it cannot read. The proxy decides nothing on the
			// query, so it sends the query as the client wrote it.
			pr.Out.URL.RawQuery = pr.In.URL.RawQuery
			dropForwardingHeaders(pr.Out.Header)
			ctx := pr.Out.Context()
			pr.Out = pr.Out.WithContext(context.WithValue(ctx, requestContextKey{}, ctx))
		},
		Transport: &http.Transport{
			// Never another proxy, whatever HTTP_PROXY says.
			Proxy: nil,
			// The transport dials the request URL's host and port; the
			// Authorizer checks them again, resolves the host and connects
			// to one of its checked addresses. The dial is cancelled when the
			// request it is made for ends, so a client that leaves, or the
			// end of the shutdown grace, stops its lookup and connect.
			DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
				host, port, err := splitHostPort(addr)
				if err != nil {
					return nil, err
				}
				if request, ok := ctx.Value(requestContextKey{}).(context.Context); ok {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					defer cancel()
					defer context.AfterFunc(request, cancel)()
				}
				return auth.Connect(ctx, host, port)
			},
			// Accept-Encoding and the reply body pass through as they are:
			// the transport neither asks for gzip nor decompresses.
			DisableCompression: true,
			ForceAttemptHTTP2:  false,
			Protocols:          &protocols,
			// There is no limit on how long a reply may take: a streamed LLM
			// reply runs for minutes.
			MaxIdleConnsPerHost: 16,
			IdleConnTimeout:     90 * time.Second,
		},
		// Each write is passed on as it arrives, so a streamed LLM reply
		// reaches the agent token by token whatever its framing.
		FlushInterval:  -1,
		ModifyResponse: refuseProtocolSwitch,
		ErrorHandler:   failed,
		ErrorLog:       errorLog,
	}
}

// refuseProtocolSwitch fails a 101 reply, which ReverseProxy would otherwise
// relay as an open connection. ReverseProxy then closes the destination's
// connection and calls the error handler.
func refuseProtocolSwitch(res *http.Response) error {
	if res.StatusCode == http.StatusSwitchingProtocols {
		return errSwitchedProtocols
	}
	return nil
}

// xForwarded is the prefix the de facto forwarding headers share.
const xForwarded = "X-Forwarded-"

// dropForwardingHeaders removes Forwarded and every X-Forwarded-* header, so
// no forwarding header the client wrote reaches the destination.
func dropForwardingHeaders(h http.Header) {
	h.Del("Forwarded")
	for name := range h {
		if len(name) >= len(xForwarded) && strings.EqualFold(name[:len(xForwarded)], xForwarded) {
			delete(h, name)
		}
	}
}

// asksForUpgrade reports whether a request asks to switch protocols: it
// carries an Upgrade header, or names "upgrade" in its Connection header.
func asksForUpgrade(h http.Header) bool {
	if len(h.Values("Upgrade")) > 0 {
		return true
	}
	for _, v := range h.Values("Connection") {
		for token := range strings.SplitSeq(v, ",") {
			if strings.EqualFold(strings.TrimSpace(token), "upgrade") {
				return true
			}
		}
	}
	return false
}

// forward carries an absolute-form http:// request to an allowlisted host and
// streams the reply back. It holds one of the slots CONNECT tunnels share and
// is counted among the handlers Shutdown waits for.
func (p *Proxy) forward(w http.ResponseWriter, r *http.Request) {
	if !p.enter() {
		p.cancelled(r, r.URL.Host, 0, "the proxy is shutting down")
		closeWithoutReply(w)
		return
	}
	defer p.open.Done()

	host, port, err := urlTarget(r.URL)
	if err != nil {
		p.refuse(w, r, r.URL.Host, 0, http.StatusBadRequest, ReasonBadRequest, err.Error())
		return
	}
	// A protocol switch would turn the request into a two-way byte stream
	// outside the tunnel handling and its idle rule, so it is refused before
	// anything is sent.
	if asksForUpgrade(r.Header) {
		p.refuse(w, r, host, port, http.StatusMethodNotAllowed, ReasonUnsupported, "protocol upgrades are not forwarded")
		return
	}
	if !p.auth.Allowed(host, port) {
		p.refuse(w, r, host, port, http.StatusForbidden, string(authorize.ReasonNotAllowed), "host and port are not on the allowlist")
		return
	}
	if !p.takeSlot(w, r, host, port) {
		return
	}
	defer p.releaseSlot()
	p.carry(w, r, host, port)
}

// carry forwards an admitted request and records it once: as denied when the
// connection fails or no byte moves before the reply's headers, as allowed
// with its bytes once the destination has answered, even when the reply then
// breaks off, and not at all when the request ends before an answer because
// its client left or the shutdown grace ended.
//
// A request no byte has crossed, either way, for the idle timeout is ended:
// its outbound request is cancelled, which closes the destination's
// connection, and the client connection's deadlines are set in the past, so
// a read of a stalled upload or a write to a client that stopped reading
// returns at once.
func (p *Proxy) carry(w http.ResponseWriter, r *http.Request, host string, port uint16) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	client := http.NewResponseController(w)
	rec := &recorder{ResponseWriter: w}
	rec.idle = startIdleTimer(p.opts.IdleTimeout, func() {
		rec.expired.Store(true)
		_ = client.SetReadDeadline(longAgo)
		_ = client.SetWriteDeadline(longAgo)
		cancel()
	})
	body := &countingBody{ReadCloser: r.Body, touch: rec.idle.touch}
	r = r.WithContext(ctx)
	r.Body = body
	start := time.Now()
	completed := false
	// ReverseProxy panics with http.ErrAbortHandler when the reply breaks off
	// after its headers were sent. The request is recorded here, as that
	// panic passes on to the server, which closes the client connection.
	defer func() {
		rec.idle.stop()
		if rec.failed {
			return
		}
		up, down := body.n.Load(), rec.n
		p.obs.Request(true, ReasonOK)
		p.obs.Transferred(DirectionUp, up)
		p.obs.Transferred(DirectionDown, down)
		p.logger.Info("request forwarded",
			slog.String("host", host),
			slog.Int("port", int(port)),
			slog.String("client", r.RemoteAddr),
			slog.String("method", r.Method),
			slog.Int("status", rec.status),
			slog.String("duration", time.Since(start).String()),
			slog.Int64("bytes_up", up),
			slog.Int64("bytes_down", down),
			slog.Bool("aborted", !completed),
			slog.Bool("idle_timeout", rec.expired.Load()))
	}()
	p.forwarder.ServeHTTP(rec, r)
	completed = true
}

// forwardFailed answers a forwarded request the transport could not carry, or
// whose reply was refused, and marks it so carry records nothing more. Every
// such failure comes before the reply's headers. A request the idle timeout
// ended is answered 504 upstream_error and its client connection closed;
// any other failure is classified by upstreamFailed.
func (p *Proxy) forwardFailed(w http.ResponseWriter, r *http.Request, err error) {
	host, port, _ := urlTarget(r.URL)
	rec, ok := w.(*recorder)
	if !ok {
		p.upstreamFailed(w, r, host, port, err)
		return
	}
	rec.failed = true
	// Once stop returns the idle timeout has fired or never will.
	rec.idle.stop()
	if rec.expired.Load() {
		// The idle timeout set the client connection's write deadline in the
		// past; the reply gets one idle timeout to be written.
		_ = http.NewResponseController(rec.ResponseWriter).SetWriteDeadline(time.Now().Add(p.opts.IdleTimeout))
		rec.Header().Set("Connection", "close")
		// The idle timeout set the read deadline in the past, which ends any read
		// of what is left of the request body at once; refuse would replace it.
		p.deny(rec, r, host, port, http.StatusGatewayTimeout, ReasonUpstreamError,
			fmt.Sprintf("no byte moved either way for %s", p.opts.IdleTimeout))
		return
	}
	p.upstreamFailed(w, r, host, port, err)
}

// closeIdleUpstreams closes the connections to destinations the forwarder
// keeps open for reuse, and every one that falls idle from now on.
func (p *Proxy) closeIdleUpstreams() {
	if t, ok := p.forwarder.Transport.(interface{ CloseIdleConnections() }); ok {
		t.CloseIdleConnections()
	}
}

// recorder notes a forwarded reply's status and body bytes, and whether
// forwarding failed. Each status and each chunk of body the destination sends
// counts as progress for the idle timeout as it arrives, before it is written
// to the client, so a write blocked on a client that stopped reading lets the
// timeout run. Unwrap lets ReverseProxy flush through it, and lets the
// failure path take over the client connection.
type recorder struct {
	http.ResponseWriter
	idle    *idleTimer
	expired atomic.Bool // the idle timeout ended the request
	status  int
	n       int64
	failed  bool
}

func (r *recorder) WriteHeader(code int) {
	r.idle.touch()
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Write(b []byte) (int, error) {
	r.idle.touch()
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.n += int64(n)
	return n, err
}

func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// countingBody counts the request body bytes the transport reads from the
// client, and calls touch whenever some arrive. The count is the bytes read,
// not the bytes the destination took: when the destination answers before it
// has read the whole body, the recorded bytes_up is a lower bound of what
// the client sent. The count is atomic because the transport may still be
// reading when the reply ends.
type countingBody struct {
	io.ReadCloser
	touch func()
	n     atomic.Int64
}

func (c *countingBody) Read(b []byte) (int, error) {
	n, err := c.ReadCloser.Read(b)
	if n > 0 {
		c.touch()
	}
	c.n.Add(int64(n))
	return n, err
}
