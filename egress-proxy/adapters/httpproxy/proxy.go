// Package httpproxy is egress-proxy's listener. It tunnels CONNECT requests
// and forwards absolute-form plain-HTTP requests to the hosts its Authorizer
// admits, and refuses every other request.
package httpproxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/carolsimone/continuo/egress-proxy/service/authorize"
	"github.com/carolsimone/continuo/egress-proxy/service/ports"
)

// Reasons the listener gives, next to the authorize package's.
const (
	ReasonOK            = "ok"
	ReasonBadRequest    = "bad_request"
	ReasonUnsupported   = "unsupported"
	ReasonCapacity      = "capacity"
	ReasonDialFailed    = "dial_failed"
	ReasonUpstreamError = "upstream_error"
)

// Directions of relayed bytes.
const (
	DirectionUp   = "up"   // from the agent to the destination
	DirectionDown = "down" // from the destination to the agent
)

// Authorizer decides whether a host and port may be reached and opens the
// connection to it.
type Authorizer interface {
	// Allowed reports whether host:port is on the allowlist, without a lookup.
	Allowed(host string, port uint16) bool
	// Connect resolves host, checks every address it resolves to and connects
	// to one of them.
	Connect(ctx context.Context, host string, port uint16) (net.Conn, error)
}

// Options bound the listener's resource use.
type Options struct {
	// MaxTunnels caps the CONNECT tunnels open at once.
	MaxTunnels int
	// IdleTimeout closes a tunnel no byte has crossed, either way, for this
	// long, and a client connection that has waited this long without
	// sending its next request.
	IdleTimeout time.Duration
	// HeaderTimeout bounds reading a request's line and headers.
	HeaderTimeout time.Duration
}

// Proxy serves the proxy protocol on one listener.
type Proxy struct {
	auth      Authorizer
	obs       ports.Observer
	logger    *slog.Logger
	opts      Options
	slots     chan struct{}
	server    *http.Server
	forwarder *httputil.ReverseProxy
	listener  net.Listener

	// cancelRequests cancels the context every request's context derives
	// from, which ends any Connect in flight.
	cancelRequests context.CancelFunc

	// mu guards tunnels, stopped and closing, and every open.Add: a tunnel
	// handler is counted only while stopped is false, so no Add races
	// Shutdown's Wait.
	mu      sync.Mutex
	tunnels map[*tunnel]struct{}
	stopped bool // Shutdown is waiting for tunnel handlers; no new one starts
	closing bool // the shutdown grace has ended; a tunnel is closed on sight
	open    sync.WaitGroup
}

// New returns a Proxy; Listen and Serve start it.
func New(auth Authorizer, obs ports.Observer, logger *slog.Logger, opts Options) *Proxy {
	requests, cancelRequests := context.WithCancel(context.Background())
	p := &Proxy{
		auth:           auth,
		obs:            obs,
		logger:         logger,
		opts:           opts,
		slots:          make(chan struct{}, opts.MaxTunnels),
		cancelRequests: cancelRequests,
		tunnels:        map[*tunnel]struct{}{},
	}
	p.forwarder = newForwarder(auth, p.forwardFailed)
	p.server = &http.Server{
		Handler:           http.HandlerFunc(p.serve),
		ReadHeaderTimeout: opts.HeaderTimeout,
		IdleTimeout:       opts.IdleTimeout,
		BaseContext:       func(net.Listener) context.Context { return requests },
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
	return p
}

// Listen binds the proxy port, so a taken port stops the service at startup.
// Port 0 picks a free port.
func (p *Proxy) Listen(port int) (net.Addr, error) {
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return nil, fmt.Errorf("listen on proxy port %d: %w", port, err)
	}
	p.listener = ln
	return ln.Addr(), nil
}

// Serve answers proxy requests until Shutdown, then returns nil.
func (p *Proxy) Serve() error {
	if err := p.server.Serve(p.listener); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown stops accepting requests and waits for forwarded requests and open
// tunnels to finish. When ctx ends first, it cancels every request still
// being served, a Connect in flight among them, closes the open tunnels and
// waits for their handlers to return, which they do at once. It returns ctx's
// error when a request other than an open tunnel was still being served as
// ctx ended.
func (p *Proxy) Shutdown(ctx context.Context) error {
	err := p.server.Shutdown(ctx)
	p.mu.Lock()
	p.stopped = true
	p.mu.Unlock()
	done := make(chan struct{})
	go func() {
		p.open.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		p.closeAll()
		<-done
	}
	p.cancelRequests()
	return err
}

// closeAll ends what is still running when the shutdown grace is over. Every
// tunnel open now, or tracked from now on, is closed; then every request's
// context is cancelled, so a Connect in flight returns, and a tunnel it
// opens anyway is closed before its client receives the 200.
func (p *Proxy) closeAll() {
	p.mu.Lock()
	p.closing = true
	closed := len(p.tunnels)
	for t := range p.tunnels {
		t.close()
	}
	p.mu.Unlock()
	p.cancelRequests()
	if closed > 0 {
		p.logger.Info("shutdown grace ended, closed open tunnels", slog.Int("tunnels", closed))
	}
}

func (p *Proxy) serve(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodConnect:
		p.tunnel(w, r)
	case r.URL.IsAbs() && r.URL.Scheme == "http":
		p.forward(w, r)
	default:
		p.refuse(w, r, r.Host, 0, http.StatusMethodNotAllowed, ReasonUnsupported,
			"only CONNECT and absolute-form http:// requests are proxied")
	}
}

// refuse answers a request the proxy will not carry, records it and logs it.
// The client sees only the reason; the detail, which may hold resolved
// addresses, goes only to the log.
func (p *Proxy) refuse(w http.ResponseWriter, r *http.Request, host string, port uint16, status int, reason, detail string) {
	p.obs.Request(false, reason)
	p.logger.Warn("egress denied",
		slog.String("host", host),
		slog.Int("port", int(port)),
		slog.String("client", r.RemoteAddr),
		slog.String("method", r.Method),
		slog.String("reason", reason),
		slog.String("detail", detail))
	http.Error(w, "egress-proxy: "+reason, status)
}

// upstreamFailed answers a request whose connection to host:port failed. A
// request whose context had already ended, because the client left or the
// shutdown grace ended, is closed without a reply and is not recorded: its
// error says nothing about the destination.
func (p *Proxy) upstreamFailed(w http.ResponseWriter, r *http.Request, host string, port uint16, err error) {
	status, reason, detail, ok := classify(r.Context(), err)
	if !ok {
		p.cancelled(r, host, port, err.Error())
		closeWithoutReply(w)
		return
	}
	p.refuse(w, r, host, port, status, reason, detail)
}

// cancelled logs a request abandoned because its client left or the proxy is
// shutting down.
func (p *Proxy) cancelled(r *http.Request, host string, port uint16, detail string) {
	p.logger.Info("request cancelled",
		slog.String("host", host),
		slog.Int("port", int(port)),
		slog.String("client", r.RemoteAddr),
		slog.String("method", r.Method),
		slog.String("detail", detail))
}

// classify maps an error from Authorizer.Connect, or from a transport that
// called it with ctx, to a status, a reason and a detail for the log. ok is
// false when ctx had ended: the error then follows from the cancellation and
// is not a verdict on the destination.
func classify(ctx context.Context, err error) (status int, reason, detail string, ok bool) {
	if ctx.Err() != nil {
		return 0, "", "", false
	}
	var denied *authorize.Denied
	switch {
	case errors.As(err, &denied):
		if denied.Reason == authorize.ReasonResolveFailed {
			return http.StatusBadGateway, string(denied.Reason), denied.Detail, true
		}
		return http.StatusForbidden, string(denied.Reason), denied.Detail, true
	case errors.Is(err, authorize.ErrDial):
		return http.StatusBadGateway, ReasonDialFailed, err.Error(), true
	default:
		return http.StatusBadGateway, ReasonUpstreamError, err.Error(), true
	}
}

// closeWithoutReply ends a request without answering it: it takes over the
// client connection and closes it. Returning without a reply would let
// net/http answer 200, which a CONNECT client reads as an open tunnel.
func closeWithoutReply(w http.ResponseWriter) {
	conn, _, err := http.NewResponseController(w).Hijack()
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	_ = conn.Close()
}

// splitHostPort parses "host:port" with a non-empty host and a port from 1 to
// 65535.
func splitHostPort(hostport string) (string, uint16, error) {
	host, portText, err := net.SplitHostPort(hostport)
	if err != nil {
		return "", 0, err
	}
	if host == "" {
		return "", 0, fmt.Errorf("no host in %q", hostport)
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return "", 0, fmt.Errorf("invalid port %q", portText)
	}
	return host, uint16(port), nil
}

// urlPort is the port of an http:// URL: the explicit one, or 80.
func urlPort(u *url.URL) (uint16, error) {
	if u.Port() == "" {
		return 80, nil
	}
	_, port, err := splitHostPort(u.Host)
	return port, err
}
