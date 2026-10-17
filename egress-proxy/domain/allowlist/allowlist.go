// Package allowlist holds the host names and ports egress-proxy may connect
// to.
package allowlist

import (
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"strings"
)

// Entry admits connections on Port to Host, or, for a "*.example.com" Host, to
// every name below example.com. AllowPrivate lets the name resolve to private
// addresses.
type Entry struct {
	Host         string
	Port         uint16
	AllowPrivate bool
}

// ErrInvalidEntry wraps every validation failure New reports.
var ErrInvalidEntry = errors.New("invalid allowlist entry")

type key struct {
	host string
	port uint16
}

// Allowlist is a validated, immutable set of entries.
type Allowlist struct {
	exact     map[key]Entry
	wildcards []Entry // longest name first
}

// New validates entries and builds an Allowlist. Hosts are compared without
// regard to case or a trailing dot. Two entries for the same host and port
// merge, allowing private addresses when either does.
func New(entries []Entry) (*Allowlist, error) {
	exact := map[key]Entry{}
	wild := map[key]Entry{}
	for i, e := range entries {
		host, err := validHost(e.Host)
		if err != nil {
			return nil, fmt.Errorf("%w: entry %d: %v", ErrInvalidEntry, i, err)
		}
		if e.Port == 0 {
			return nil, fmt.Errorf("%w: entry %d (%s): port must be 1-65535", ErrInvalidEntry, i, host)
		}
		e.Host = host
		target := exact
		if strings.HasPrefix(host, "*.") {
			target = wild
		}
		k := key{host: host, port: e.Port}
		if prev, ok := target[k]; ok {
			e.AllowPrivate = e.AllowPrivate || prev.AllowPrivate
		}
		target[k] = e
	}
	a := &Allowlist{exact: exact}
	for _, e := range wild {
		a.wildcards = append(a.wildcards, e)
	}
	sort.Slice(a.wildcards, func(i, j int) bool {
		if len(a.wildcards[i].Host) != len(a.wildcards[j].Host) {
			return len(a.wildcards[i].Host) > len(a.wildcards[j].Host)
		}
		return a.wildcards[i].Host < a.wildcards[j].Host
	})
	return a, nil
}

// Match returns the entry admitting host on port: an exact entry, else the
// longest matching wildcard. A host that is not a valid name matches nothing.
// Callers connect to the returned Entry.Host (or Normalize(host)), never to
// the raw input.
func (a *Allowlist) Match(host string, port uint16) (Entry, bool) {
	h, err := validHost(host)
	if err != nil || strings.HasPrefix(h, "*.") {
		return Entry{}, false
	}
	if e, ok := a.exact[key{host: h, port: port}]; ok {
		return e, true
	}
	for _, e := range a.wildcards {
		suffix := e.Host[1:] // ".example.com"
		if e.Port == port && strings.HasSuffix(h, suffix) {
			return e, true
		}
	}
	return Entry{}, false
}

// Len is the number of distinct host-and-port entries.
func (a *Allowlist) Len() int { return len(a.exact) + len(a.wildcards) }

// maxHostLen is the longest host name in bytes once normalized (RFC 1035).
const maxHostLen = 253

// Normalize lower-cases the ASCII letters of host and removes one trailing
// dot. Non-ASCII bytes are kept as they are, so validHost refuses them rather
// than folding them onto ASCII names.
func Normalize(host string) string {
	b := []byte(host)
	for i, c := range b {
		if 'A' <= c && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return strings.TrimSuffix(string(b), ".")
}

// validHost normalizes host and checks it is a DNS name: at most 253 ASCII
// bytes, labels of 1-63 letters, digits and inner hyphens, optionally led by a
// "*" label that has at least two labels after it. IP addresses and legacy
// numeric IPv4 forms (such as "127.1", "2130706433" or "0x7f.0.0.1") are
// refused: entries name hosts.
func validHost(raw string) (string, error) {
	h := Normalize(raw)
	if h == "" {
		return "", errors.New("host is empty")
	}
	if len(h) > maxHostLen {
		return "", fmt.Errorf("%q is longer than %d bytes", raw, maxHostLen)
	}
	for i := 0; i < len(h); i++ {
		if h[i] >= 0x80 {
			return "", fmt.Errorf("%q is not ASCII", raw)
		}
	}
	if _, err := netip.ParseAddr(strings.Trim(h, "[]")); err == nil {
		return "", fmt.Errorf("%q is an IP address; entries name hosts", raw)
	}
	labels := strings.Split(h, ".")
	if legacyNumeric(labels[len(labels)-1]) {
		return "", fmt.Errorf("%q reads as a numeric IPv4 address; entries name hosts", raw)
	}
	for i, l := range labels {
		if l == "*" && i == 0 && len(labels) > 2 {
			continue
		}
		if !validLabel(l) {
			return "", fmt.Errorf("%q is not a valid host name", raw)
		}
	}
	return h, nil
}

// legacyNumeric reports whether a normalized last label is all decimal digits,
// or "0x" followed only by hex digits. Resolvers read such a last label as part
// of a numeric IPv4 address.
func legacyNumeric(l string) bool {
	if l == "" {
		return false
	}
	if allIn(l, "0123456789") {
		return true
	}
	return strings.HasPrefix(l, "0x") && allIn(l[2:], "0123456789abcdef")
}

func allIn(s, set string) bool {
	for i := 0; i < len(s); i++ {
		if !strings.ContainsRune(set, rune(s[i])) {
			return false
		}
	}
	return true
}

func validLabel(l string) bool {
	if len(l) == 0 || len(l) > 63 || l[0] == '-' || l[len(l)-1] == '-' {
		return false
	}
	for _, c := range l {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			return false
		}
	}
	return true
}
