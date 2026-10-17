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
	// Private is an address inside a private, shared, reserved, transition or
	// multicast range.
	Private
	// Loopback is the host itself.
	Loopback
	// Unspecified is 0.0.0.0 or ::.
	Unspecified
	// Metadata is a cloud provider's instance metadata service.
	Metadata
	// LinkLocal is a link-local address; instance-metadata and credential
	// services answer there, and it is never an egress destination.
	LinkLocal
)

var names = map[Class]string{Public: "public", Private: "private", Loopback: "loopback", Unspecified: "unspecified", Metadata: "metadata", LinkLocal: "link-local"}

func (c Class) String() string { return names[c] }

var metadataAddrs = []netip.Addr{
	netip.MustParseAddr("169.254.169.254"),
	netip.MustParseAddr("fd20:ce::254"),    // Google Cloud IPv6
	netip.MustParseAddr("100.100.100.200"), // Alibaba Cloud
	netip.MustParseAddr("168.63.129.16"),   // Azure WireServer
}

// metadataPrefixes are the ranges a cloud provider reserves for its instance
// services: the whole AWS Nitro IPv6 range answers IMDS (::254), DNS (::253),
// NTP (::123) and more.
var metadataPrefixes = mustPrefixes(
	"fd00:ec2::/32",
)

// linkLocalPrefixes are the link-local ranges, where cloud credential services
// answer, for example EKS Pod Identity (169.254.170.23) and ECS (169.254.170.2).
var linkLocalPrefixes = mustPrefixes(
	"169.254.0.0/16",
	"fe80::/10",
)

var privatePrefixes = mustPrefixes(
	"0.0.0.0/8",      // "this network"
	"10.0.0.0/8",     // private
	"100.64.0.0/10",  // carrier-grade NAT
	"172.16.0.0/12",  // private
	"192.0.0.0/24",   // IETF protocol assignments
	"192.168.0.0/16", // private
	"198.18.0.0/15",  // benchmarking
	"224.0.0.0/4",    // multicast
	"240.0.0.0/4",    // reserved, and broadcast
	"fc00::/7",       // unique local
	"ff00::/8",       // multicast
	"fec0::/10",      // deprecated site-local
	"2002::/16",      // 6to4
	"2001::/32",      // Teredo
	"::/96",          // IPv4-compatible (:: and ::1 are classified before this)
	"64:ff9b:1::/48", // local-use NAT64
)

// nat64 is the well-known NAT64 prefix: its last 32 bits are an IPv4 address,
// classified as that address.
var nat64 = netip.MustParsePrefix("64:ff9b::/96")

// Of classifies a. An invalid address is Unspecified: it is never connectable.
// The IPv6 zone is ignored, so a zoned address is classified as its unzoned
// form. An IPv4-mapped IPv6 address and a NAT64 address are classified by the
// IPv4 address they carry, so a NAT64 address embedding a link-local or
// metadata IPv4 address (64:ff9b::a9fe:aa17) is LinkLocal or Metadata.
func Of(a netip.Addr) Class {
	if !a.IsValid() {
		return Unspecified
	}
	a = a.WithZone("").Unmap()
	if a.Is6() && nat64.Contains(a) {
		b := a.As16()
		a = netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]})
	}
	if slices.Contains(metadataAddrs, a) {
		return Metadata
	}
	for _, p := range metadataPrefixes {
		if p.Contains(a) {
			return Metadata
		}
	}
	switch {
	case a.IsUnspecified():
		return Unspecified
	case a.IsLoopback():
		return Loopback
	}
	for _, p := range linkLocalPrefixes {
		if p.Contains(a) {
			return LinkLocal
		}
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
