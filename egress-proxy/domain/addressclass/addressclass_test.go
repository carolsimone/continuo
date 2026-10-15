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
		"169.254.10.10":          Private,
		"0.1.2.3":                Private,
		"192.0.0.8":              Private,
		"198.18.0.1":             Private,
		"224.0.0.1":              Private,
		"255.255.255.255":        Private,
		"fd12:3456::1":           Private,
		"fe80::1":                Private,
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
		"fe80::1%eth0":           Private,
	}
	for addr, want := range cases {
		assert.Equal(t, want, Of(netip.MustParseAddr(addr)), addr)
	}
}

func TestString(t *testing.T) {
	assert.Equal(t, "public", Public.String())
	assert.Equal(t, "metadata", Metadata.String())
}
