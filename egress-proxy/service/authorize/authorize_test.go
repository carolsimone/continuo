package authorize

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"testing"

	"github.com/carolsimone/continuo/egress-proxy/domain/allowlist"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeResolver struct {
	answers map[string][]string
	// raw, when set, is returned as is for every host.
	raw   []netip.Addr
	err   error
	calls int
}

func (r *fakeResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	r.calls++
	if r.err != nil {
		return nil, r.err
	}
	if r.raw != nil {
		return r.raw, nil
	}
	var out []netip.Addr
	for _, a := range r.answers[host] {
		out = append(out, netip.MustParseAddr(a))
	}
	return out, nil
}

// fakeDialer records every address it is asked for and fails those in refuse.
type fakeDialer struct {
	mu     sync.Mutex
	dialed []string
	refuse map[string]bool
}

func (d *fakeDialer) DialContext(_ context.Context, _, address string) (net.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dialed = append(d.dialed, address)
	if d.refuse[address] {
		return nil, errors.New("connection refused")
	}
	client, server := net.Pipe()
	_ = server.Close()
	return client, nil
}

func newAuthorizer(t *testing.T, r *fakeResolver, d *fakeDialer, entries ...allowlist.Entry) *Authorizer {
	t.Helper()
	a, err := allowlist.New(entries)
	require.NoError(t, err)
	return New(a, r, d)
}

func requireDenied(t *testing.T, err error, want Reason) {
	t.Helper()
	var denied *Denied
	require.True(t, errors.As(err, &denied), "error %v is not a *Denied", err)
	assert.Equal(t, want, denied.Reason)
}

func TestAuthorize_RefusesHostNotOnAllowlistWithoutLookup(t *testing.T) {
	r := &fakeResolver{}
	a := newAuthorizer(t, r, &fakeDialer{}, allowlist.Entry{Host: "api.anthropic.com", Port: 443})
	_, err := a.Authorize(context.Background(), "evil.example.com", 443)
	requireDenied(t, err, ReasonNotAllowed)
	assert.Equal(t, 0, r.calls)
	assert.False(t, a.Allowed("evil.example.com", 443))
	assert.True(t, a.Allowed("API.anthropic.com.", 443))
}

func TestAuthorize_PublicAddressesPass(t *testing.T) {
	r := &fakeResolver{answers: map[string][]string{"api.anthropic.com": {"160.79.104.10", "::ffff:160.79.104.11"}}}
	a := newAuthorizer(t, r, &fakeDialer{}, allowlist.Entry{Host: "api.anthropic.com", Port: 443})
	addrs, err := a.Authorize(context.Background(), "API.Anthropic.com.", 443)
	require.NoError(t, err)
	assert.Equal(t, []netip.Addr{netip.MustParseAddr("160.79.104.10"), netip.MustParseAddr("160.79.104.11")}, addrs, "unmapped, in resolver order")
}

func TestAuthorize_AddressCheck(t *testing.T) {
	cases := []struct {
		name         string
		addrs        []string
		allowPrivate bool
		want         Reason // "" means allowed
	}{
		{"private refused", []string{"10.0.0.5"}, false, ReasonPrivateAddress},
		{"private allowed by the entry", []string{"10.0.0.5"}, true, ""},
		{"one private among public refused", []string{"160.79.104.10", "192.168.1.9"}, false, ReasonPrivateAddress},
		{"IPv4-mapped private refused", []string{"::ffff:10.0.0.5"}, false, ReasonPrivateAddress},
		{"NAT64 private refused", []string{"64:ff9b::a00:5"}, false, ReasonPrivateAddress},
		{"metadata refused even when private is allowed", []string{"169.254.169.254"}, true, ReasonMetadata},
		{"link-local refused even when private is allowed", []string{"169.254.170.23"}, true, ReasonPrivateAddress},
		{"Alibaba metadata refused even when private is allowed", []string{"100.100.100.200"}, true, ReasonMetadata},
		{"NAT64 metadata refused", []string{"64:ff9b::a9fe:a9fe"}, true, ReasonMetadata},
		{"loopback refused even when private is allowed", []string{"127.0.0.1"}, true, ReasonPrivateAddress},
		{"unspecified refused even when private is allowed", []string{"0.0.0.0"}, true, ReasonPrivateAddress},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &fakeResolver{answers: map[string][]string{"model.internal": tc.addrs}}
			a := newAuthorizer(t, r, &fakeDialer{}, allowlist.Entry{Host: "model.internal", Port: 8000, AllowPrivate: tc.allowPrivate})
			_, err := a.Authorize(context.Background(), "model.internal", 8000)
			if tc.want == "" {
				require.NoError(t, err)
				return
			}
			requireDenied(t, err, tc.want)
		})
	}
}

func TestAuthorize_ResolveFailures(t *testing.T) {
	a := newAuthorizer(t, &fakeResolver{err: errors.New("no such host")}, &fakeDialer{}, allowlist.Entry{Host: "api.anthropic.com", Port: 443})
	_, err := a.Authorize(context.Background(), "api.anthropic.com", 443)
	requireDenied(t, err, ReasonResolveFailed)

	a = newAuthorizer(t, &fakeResolver{answers: map[string][]string{}}, &fakeDialer{}, allowlist.Entry{Host: "api.anthropic.com", Port: 443})
	_, err = a.Authorize(context.Background(), "api.anthropic.com", 443)
	requireDenied(t, err, ReasonResolveFailed)
}

func TestAuthorize_InvalidAddressFromResolverIsRefused(t *testing.T) {
	r := &fakeResolver{raw: []netip.Addr{{}}}
	a := newAuthorizer(t, r, &fakeDialer{}, allowlist.Entry{Host: "api.anthropic.com", Port: 443})
	_, err := a.Authorize(context.Background(), "api.anthropic.com", 443)
	requireDenied(t, err, ReasonResolveFailed)
	var denied *Denied
	require.True(t, errors.As(err, &denied))
	assert.Equal(t, "resolver returned an invalid address", denied.Detail)
}

// A spelling that is not a valid ASCII host name never matches an entry, so it
// is refused before any lookup.
func TestAuthorize_InvalidSpellingIsRefusedWithoutLookup(t *testing.T) {
	r := &fakeResolver{answers: map[string][]string{"docker.io": {"160.79.104.10"}}}
	a := newAuthorizer(t, r, &fakeDialer{}, allowlist.Entry{Host: "docker.io", Port: 443})
	for _, host := range []string{"doc\u212Aer.io", "docker.io\x00", "docker..io", ""} {
		_, err := a.Authorize(context.Background(), host, 443)
		requireDenied(t, err, ReasonNotAllowed)
		assert.False(t, a.Allowed(host, 443))
	}
	assert.Equal(t, 0, r.calls)
}

// Connect dials only the addresses it checked, in order, after one lookup: a
// name cannot change its address between the check and the connection.
func TestConnect_DialsOnlyCheckedAddresses(t *testing.T) {
	r := &fakeResolver{answers: map[string][]string{"api.anthropic.com": {"160.79.104.10", "160.79.104.11"}}}
	d := &fakeDialer{refuse: map[string]bool{"160.79.104.10:443": true}}
	a := newAuthorizer(t, r, d, allowlist.Entry{Host: "api.anthropic.com", Port: 443})
	conn, err := a.Connect(context.Background(), "api.anthropic.com", 443)
	require.NoError(t, err)
	_ = conn.Close()
	assert.Equal(t, []string{"160.79.104.10:443", "160.79.104.11:443"}, d.dialed)
	assert.Equal(t, 1, r.calls)
}

func TestConnect_DeniedNeverDials(t *testing.T) {
	r := &fakeResolver{answers: map[string][]string{"api.anthropic.com": {"10.0.0.1"}}}
	d := &fakeDialer{}
	a := newAuthorizer(t, r, d, allowlist.Entry{Host: "api.anthropic.com", Port: 443})
	_, err := a.Connect(context.Background(), "api.anthropic.com", 443)
	requireDenied(t, err, ReasonPrivateAddress)
	assert.Empty(t, d.dialed)
}

func TestConnect_EveryDialFailing(t *testing.T) {
	r := &fakeResolver{answers: map[string][]string{"api.anthropic.com": {"160.79.104.10"}}}
	d := &fakeDialer{refuse: map[string]bool{"160.79.104.10:443": true}}
	a := newAuthorizer(t, r, d, allowlist.Entry{Host: "api.anthropic.com", Port: 443})
	_, err := a.Connect(context.Background(), "api.anthropic.com", 443)
	require.ErrorIs(t, err, ErrDial)
}

func TestConnect_IPv6AddressFormat(t *testing.T) {
	r := &fakeResolver{answers: map[string][]string{"api.example.com": {"2606:4700::6810:84e5"}}}
	d := &fakeDialer{}
	a := newAuthorizer(t, r, d, allowlist.Entry{Host: "api.example.com", Port: 443})
	conn, err := a.Connect(context.Background(), "api.example.com", 443)
	require.NoError(t, err)
	_ = conn.Close()
	assert.Equal(t, []string{"[2606:4700::6810:84e5]:443"}, d.dialed)
}
