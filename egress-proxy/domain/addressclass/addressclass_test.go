package addressclass

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOf(t *testing.T) {
	cases := map[string]Class{
		"160.79.104.10":          Public,
		"140.82.112.5":           Public,
		"2606:4700::6810:84e5":   Public,
		"10.1.2.3":               Private,
		"172.16.0.1":             Private,
		"172.31.255.255":         Private,
		"192.168.1.1":            Private,
		"100.64.0.1":             Private,
		"169.254.10.10":          LinkLocal, // was Private before link-local became its own class
		"0.1.2.3":                Private,
		"192.0.0.8":              Private,
		"198.18.0.1":             Private,
		"224.0.0.1":              Private,
		"255.255.255.255":        Private,
		"fd12:3456::1":           Private,
		"fe80::1":                LinkLocal, // was Private before link-local became its own class
		"ff02::1":                Private,
		"127.0.0.1":              Loopback,
		"127.8.8.8":              Loopback,
		"::1":                    Loopback,
		"0.0.0.0":                Unspecified,
		"::":                     Unspecified,
		"169.254.169.254":        Metadata,
		"fd00:ec2::254":          Metadata,
		"::ffff:10.0.0.1":        Private,
		"::ffff:169.254.169.254": Metadata,
		"::ffff:160.79.104.10":   Public,
		"64:ff9b::a00:1":         Private,     // NAT64 of 10.0.0.1
		"64:ff9b::a9fe:a9fe":     Metadata,    // NAT64 of 169.254.169.254
		"64:ff9b::a04f:680a":     Public,      // NAT64 of 160.79.104.10
		"64:ff9b::7f00:1":        Loopback,    // NAT64 of 127.0.0.1
		"64:ff9b::":              Unspecified, // NAT64 of 0.0.0.0
		"fd00:ec2::254%eth0":     Metadata,    // zone must not defeat the metadata match
		"fe80::1%eth0":           LinkLocal,   // was Private before link-local became its own class
		"169.254.170.23":         LinkLocal,   // EKS Pod Identity
		"169.254.170.2":          LinkLocal,   // ECS task credentials
		"::ffff:169.254.170.23":  LinkLocal,
		"64:ff9b::a9fe:aa17":     LinkLocal, // NAT64 of 169.254.170.23
		"100.100.100.200":        Metadata,  // Alibaba
		"168.63.129.16":          Metadata,  // Azure WireServer
		"fd00:ec2::23":           Metadata,
		"fec0::1":                Private, // deprecated site-local
		"2002:a00:1::1":          Private, // 6to4
		"2001::1":                Private, // Teredo
		"::a00:1":                Private, // IPv4-compatible
		"64:ff9b:1::a00:1":       Private, // local-use NAT64
		"172.32.0.1":             Public,
		"100.128.0.1":            Public,
		"198.20.0.1":             Public,
		"192.0.1.1":              Public,
		"172.15.255.255":         Public,
		"2606:4700::1111":        Public,
		"2001:4860:4860::8888":   Public, // Google DNS: 2001::/32 does not shadow it
	}
	for addr, want := range cases {
		assert.Equal(t, want, Of(netip.MustParseAddr(addr)), addr)
	}
}

func TestOf_InvalidAddressIsUnspecified(t *testing.T) {
	assert.Equal(t, Unspecified, Of(netip.Addr{}))
}

func TestString(t *testing.T) {
	assert.Equal(t, "link-local", LinkLocal.String())
	assert.Equal(t, "public", Public.String())
	assert.Equal(t, "metadata", Metadata.String())
}
