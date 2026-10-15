package httpproxy

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/carolsimone/continuo/egress-proxy/service/authorize"
)

// established is the reply that opens a tunnel.
const established = "HTTP/1.1 200 Connection established\r\n\r\n"

// tunnel is one open CONNECT tunnel.
type tunnel struct {
	client   net.Conn
	upstream net.Conn
}

func (t *tunnel) close() {
	_ = t.client.Close()
	_ = t.upstream.Close()
}

// enter counts a tunnel handler in, unless Shutdown is already waiting for
// tunnel handlers to return.
func (p *Proxy) enter() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return false
	}
	p.open.Add(1)
	return true
}

// track registers an open tunnel so Shutdown can close it. Once the shutdown
// grace has ended it registers nothing and returns false.
func (p *Proxy) track(t *tunnel) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closing {
		return false
	}
	p.tunnels[t] = struct{}{}
	return true
}

func (p *Proxy) untrack(t *tunnel) {
	p.mu.Lock()
	delete(p.tunnels, t)
	p.mu.Unlock()
}

// tunnel answers a CONNECT request: it checks the host, takes a tunnel slot,
// connects through the Authorizer, then relays bytes both ways until either
// side closes or the tunnel is idle. The client receives nothing until
// Connect has succeeded.
func (p *Proxy) tunnel(w http.ResponseWriter, r *http.Request) {
	if !p.enter() {
		p.cancelled(r, r.URL.Host, 0, "the proxy is shutting down")
		closeWithoutReply(w)
		return
	}
	defer p.open.Done()

	// The target is the request's authority form; a path-form target has no
	// host there, and the Host header does not stand in for it.
	host, port, err := splitHostPort(r.URL.Host)
	if err != nil {
		p.refuse(w, r, r.URL.Host, 0, http.StatusBadRequest, ReasonBadRequest, err.Error())
		return
	}
	if !p.auth.Allowed(host, port) {
		p.refuse(w, r, host, port, http.StatusForbidden, string(authorize.ReasonNotAllowed), "host and port are not on the allowlist")
		return
	}
	select {
	case p.slots <- struct{}{}:
		defer func() { <-p.slots }()
	default:
		p.refuse(w, r, host, port, http.StatusServiceUnavailable, ReasonCapacity, fmt.Sprintf("%d tunnels already open", cap(p.slots)))
		return
	}
	upstream, err := p.auth.Connect(r.Context(), host, port)
	if err != nil {
		p.upstreamFailed(w, r, host, port, err)
		return
	}
	// Hijack also clears the header read deadline the server set.
	client, buffered, err := http.NewResponseController(w).Hijack()
	if err != nil {
		_ = upstream.Close()
		p.logger.Error("tunnel hijack failed", slog.String("host", host), slog.Int("port", int(port)), slog.Any("error", err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	t := &tunnel{client: client, upstream: upstream}
	if !p.track(t) {
		t.close()
		p.cancelled(r, host, port, "the shutdown grace ended while connecting")
		return
	}
	defer p.untrack(t)
	if err := p.establish(client); err != nil {
		t.close()
		p.cancelled(r, host, port, "the client left before the tunnel opened: "+err.Error())
		return
	}
	p.obs.Request(true, ReasonOK)
	p.obs.TunnelOpened()
	defer p.obs.TunnelClosed()

	// Bytes the client sent right behind the CONNECT request, typically a TLS
	// ClientHello, are already in the server's read buffer. They are taken
	// out of it, all of them served from the buffer, and sent upstream ahead
	// of what the relay then reads from the connection.
	pending := make([]byte, buffered.Reader.Buffered())
	n, _ := io.ReadFull(buffered.Reader, pending)
	start := time.Now()
	up, down := relay(client, upstream, pending[:n], p.opts.IdleTimeout)
	p.obs.Transferred(DirectionUp, up)
	p.obs.Transferred(DirectionDown, down)
	p.logger.Info("tunnel closed",
		slog.String("host", host),
		slog.Int("port", int(port)),
		slog.String("client", r.RemoteAddr),
		slog.String("duration", time.Since(start).String()),
		slog.Int64("bytes_up", up),
		slog.Int64("bytes_down", down))
}

// establish sends the reply that opens the tunnel. A client that does not
// read it within the idle timeout gives up its tunnel slot.
func (p *Proxy) establish(client net.Conn) error {
	if err := client.SetWriteDeadline(time.Now().Add(p.opts.IdleTimeout)); err != nil {
		return err
	}
	if _, err := io.WriteString(client, established); err != nil {
		return err
	}
	return client.SetWriteDeadline(time.Time{})
}

// relay sends pending to upstream, then copies bytes between client and
// upstream until both directions end or no byte has moved either way for
// idle. It closes both connections and returns the bytes sent up and down.
func relay(client, upstream net.Conn, pending []byte, idle time.Duration) (up, down int64) {
	closeBoth := sync.OnceFunc(func() {
		_ = client.Close()
		_ = upstream.Close()
	})
	defer closeBoth()

	// last is the time of the latest byte, as an offset from start, so the
	// idle check runs on the monotonic clock.
	start := time.Now()
	var last atomic.Int64
	touch := func() { last.Store(int64(time.Since(start))) }
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		tick := time.NewTicker(max(idle/4, 5*time.Millisecond))
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				if time.Since(start)-time.Duration(last.Load()) >= idle {
					closeBoth()
					return
				}
			}
		}
	}()

	if len(pending) > 0 {
		n, err := upstream.Write(pending)
		up += int64(n)
		if err != nil {
			return up, 0
		}
		touch()
	}

	// A direction whose source ended cleanly half-closes its destination, so
	// the other direction can still carry a reply. Any other end, a reset, a
	// failed write, or a close by the idle check or Shutdown, closes both.
	pipe := func(dst, src net.Conn) int64 {
		n, eof := copyActive(dst, src, touch)
		if eof {
			closeWrite(dst)
		} else {
			closeBoth()
		}
		return n
	}
	var wg sync.WaitGroup
	var upN, downN int64
	wg.Add(2)
	go func() {
		defer wg.Done()
		upN = pipe(upstream, client)
	}()
	go func() {
		defer wg.Done()
		downN = pipe(client, upstream)
	}()
	wg.Wait()
	return up + upN, downN
}

// copyActive copies src to dst, calling touch whenever bytes arrive. It
// returns the bytes written, and whether src ended with io.EOF after every
// byte read from it was written.
func copyActive(dst io.Writer, src io.Reader, touch func()) (int64, bool) {
	buf := make([]byte, 32*1024)
	var total int64
	for {
		n, err := src.Read(buf)
		if n > 0 {
			touch()
			w, werr := dst.Write(buf[:n])
			total += int64(w)
			if werr != nil {
				return total, false
			}
		}
		if err != nil {
			return total, errors.Is(err, io.EOF)
		}
	}
}

// closeWrite ends one direction while the other may still be sending: a
// half-close on TCP, a full close otherwise.
func closeWrite(c net.Conn) {
	if cw, ok := c.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
		return
	}
	_ = c.Close()
}
