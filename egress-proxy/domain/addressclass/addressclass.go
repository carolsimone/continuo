// Package addressclass sorts IP addresses into the classes egress-proxy's
// address check distinguishes.
package addressclass

import (
	"net/netip"
	"slices"
)

// Class is the kind of destination an address is.
type Class int

const (
	// Public is a global unicast address on the internet.
	Public Class = iota
	// Private is an address inside a private, link-local, shared, reserved or
	// multicast range.
	Private
	// Loopback is the host itself.
	Loopback
	// Unspecified is 0.0.0.0 or ::.
	Unspecified
	// Metadata is a cloud provider's instance metadata service.
	Metadata
)

var names = map[Class]string{Public: "public", Private: "private", Loopback: "loopback", Unspecified: "unspecified", Metadata: "metadata"}

func (c Class) String() string { return names[c] }

var metadataAddrs = []netip.Addr{
	netip.MustParseAddr("169.254.169.254"),
	netip.MustParseAddr("fd00:ec2::254"),
}

var privatePrefixes = mustPrefixes(
	"0.0.0.0/8",      // "this network"
	"10.0.0.0/8",     // private
	"100.64.0.0/10",  // carrier-grade NAT
	"169.254.0.0/16", // link-local
	"172.16.0.0/12",  // private
	"192.0.0.0/24",   // IETF protocol assignments
	"192.168.0.0/16", // private
	"198.18.0.0/15",  // benchmarking
	"224.0.0.0/4",    // multicast
	"240.0.0.0/4",    // reserved, and broadcast
	"fc00::/7",       // unique local
	"fe80::/10",      // link-local
	"ff00::/8",       // multicast
)

// nat64 is the well-known NAT64 prefix: its last 32 bits are an IPv4 address,
// classified as that address.
var nat64 = netip.MustParsePrefix("64:ff9b::/96")

// Of classifies a. The IPv6 zone is ignored, so a zoned address is classified
// as its unzoned form. An IPv4-mapped IPv6 address and a NAT64 address are
// classified by the IPv4 address they carry.
func Of(a netip.Addr) Class {
	a = a.WithZone("").Unmap()
	if a.Is6() && nat64.Contains(a) {
		b := a.As16()
		a = netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]})
	}
	switch {
	case slices.Contains(metadataAddrs, a):
		return Metadata
	case a.IsUnspecified():
		return Unspecified
	case a.IsLoopback():
		return Loopback
	}
	for _, p := range privatePrefixes {
		if p.Contains(a) {
			return Private
		}
	}
	return Public
}

func mustPrefixes(cidrs ...string) []netip.Prefix {
	out := make([]netip.Prefix, len(cidrs))
	for i, c := range cidrs {
		out[i] = netip.MustParsePrefix(c)
	}
	return out
}
