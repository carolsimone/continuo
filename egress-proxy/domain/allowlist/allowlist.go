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

// Normalize lower-cases host and removes one trailing dot.
func Normalize(host string) string {
	return strings.TrimSuffix(strings.ToLower(host), ".")
}

// validHost normalizes host and checks it is a DNS name: labels of 1-63
// letters, digits and inner hyphens, optionally led by a "*" label that has at
// least two labels after it. IP addresses are refused: entries name hosts.
func validHost(raw string) (string, error) {
	h := Normalize(raw)
	if h == "" {
		return "", errors.New("host is empty")
	}
	if _, err := netip.ParseAddr(strings.Trim(h, "[]")); err == nil {
		return "", fmt.Errorf("%q is an IP address; entries name hosts", raw)
	}
	labels := strings.Split(h, ".")
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
