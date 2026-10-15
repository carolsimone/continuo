// Package authorize decides whether egress-proxy may connect to a host and
// port, and to which addresses.
package authorize

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"

	"github.com/carolsimone/continuo/egress-proxy/domain/addressclass"
	"github.com/carolsimone/continuo/egress-proxy/domain/allowlist"
	"github.com/carolsimone/continuo/egress-proxy/service/ports"
)

// Reason names why a request was refused.
type Reason string

const (
	// ReasonNotAllowed: the host and port are not on the allowlist.
	ReasonNotAllowed Reason = "not_allowed"
	// ReasonPrivateAddress: the name resolved to a private, loopback or
	// unspecified address its entry does not allow.
	ReasonPrivateAddress Reason = "private_address"
	// ReasonMetadata: the name resolved to a cloud metadata address.
	ReasonMetadata Reason = "metadata"
	// ReasonResolveFailed: the name did not resolve.
	ReasonResolveFailed Reason = "resolve_failed"
)

// Denied is the error a refused request returns.
type Denied struct {
	Reason Reason
	Host   string
	Port   uint16
	Detail string
}

func (d *Denied) Error() string {
	return fmt.Sprintf("%s:%d refused (%s): %s", d.Host, d.Port, d.Reason, d.Detail)
}

// ErrDial wraps a failure to connect to every checked address.
var ErrDial = errors.New("dial failed")

// Authorizer checks requests against an allowlist and the address check.
type Authorizer struct {
	allow    *allowlist.Allowlist
	resolver ports.Resolver
	dialer   ports.Dialer
}

// New returns an Authorizer for allow.
func New(allow *allowlist.Allowlist, resolver ports.Resolver, dialer ports.Dialer) *Authorizer {
	return &Authorizer{allow: allow, resolver: resolver, dialer: dialer}
}

// Allowed reports whether host:port is on the allowlist, without a lookup.
func (a *Authorizer) Allowed(host string, port uint16) bool {
	_, ok := a.allow.Match(host, port)
	return ok
}

// Authorize returns the addresses egress-proxy may connect to for host:port,
// in the order the resolver gave them. Every address must pass the address
// check: a cloud metadata, loopback or unspecified address is always refused,
// and a private one unless the matching entry allows private addresses.
func (a *Authorizer) Authorize(ctx context.Context, host string, port uint16) ([]netip.Addr, error) {
	entry, ok := a.allow.Match(host, port)
	if !ok {
		return nil, &Denied{Reason: ReasonNotAllowed, Host: host, Port: port, Detail: "host and port are not on the allowlist"}
	}
	resolved, err := a.resolver.LookupNetIP(ctx, "ip", allowlist.Normalize(host))
	if err != nil {
		return nil, &Denied{Reason: ReasonResolveFailed, Host: host, Port: port, Detail: err.Error()}
	}
	if len(resolved) == 0 {
		return nil, &Denied{Reason: ReasonResolveFailed, Host: host, Port: port, Detail: "no addresses"}
	}
	addrs := make([]netip.Addr, 0, len(resolved))
	for _, addr := range resolved {
		if !addr.IsValid() {
			return nil, &Denied{Reason: ReasonResolveFailed, Host: host, Port: port, Detail: "resolver returned an invalid address"}
		}
		addr = addr.Unmap()
		switch class := addressclass.Of(addr); class {
		case addressclass.Public:
		case addressclass.Private:
			if !entry.AllowPrivate {
				return nil, &Denied{Reason: ReasonPrivateAddress, Host: host, Port: port, Detail: addr.String() + " is private"}
			}
		case addressclass.Metadata:
			return nil, &Denied{Reason: ReasonMetadata, Host: host, Port: port, Detail: addr.String() + " is a cloud metadata address"}
		default:
			return nil, &Denied{Reason: ReasonPrivateAddress, Host: host, Port: port, Detail: addr.String() + " is " + class.String()}
		}
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

// Connect authorizes host:port and connects to the first checked address that
// accepts, without looking the name up again.
func (a *Authorizer) Connect(ctx context.Context, host string, port uint16) (net.Conn, error) {
	addrs, err := a.Authorize(ctx, host, port)
	if err != nil {
		return nil, err
	}
	var errs []error
	for _, addr := range addrs {
		conn, err := a.dialer.DialContext(ctx, "tcp", netip.AddrPortFrom(addr, port).String())
		if err == nil {
			return conn, nil
		}
		errs = append(errs, err)
	}
	return nil, fmt.Errorf("%w: %s:%d: %w", ErrDial, host, port, errors.Join(errs...))
}
