// Package ports declares the collaborators egress-proxy's application layer
// depends on.
package ports

import (
	"context"
	"net"
	"net/netip"
)

// Resolver looks up the addresses of a host name.
type Resolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

// Dialer opens a connection to one address.
type Dialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// Observer records the proxy's decisions and the traffic it relays.
type Observer interface {
	// Request records one proxied request or tunnel, allowed or refused, and
	// its reason.
	Request(allowed bool, reason string)
	// Transferred records n bytes relayed in direction "up" (to the
	// destination) or "down" (to the agent).
	Transferred(direction string, n int64)
	// TunnelOpened and TunnelClosed bracket one CONNECT tunnel.
	TunnelOpened()
	TunnelClosed()
}
