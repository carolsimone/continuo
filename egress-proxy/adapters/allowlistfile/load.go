// Package allowlistfile reads egress-proxy's allowlist from a YAML file.
package allowlistfile

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/carolsimone/continuo/egress-proxy/domain/allowlist"
	"gopkg.in/yaml.v3"
)

// file is the allowlist file's shape:
//
//	entries:
//	  - host: api.anthropic.com
//	    port: 443
//	  - host: gpu-box.internal
//	    port: 8000
//	    allowPrivate: true
type file struct {
	Entries []struct {
		Host         string `yaml:"host"`
		Port         int    `yaml:"port"`
		AllowPrivate bool   `yaml:"allowPrivate"`
	} `yaml:"entries"`
}

// Load reads and validates the allowlist at path. An unknown field, a second
// YAML document, a port outside 1-65535, an invalid host or a file with no
// entries is an error: the proxy refuses to start rather than run with an
// allowlist it misread. An error for a bad entry names the entry's index and
// host, and nothing else from the file.
func Load(path string) (*allowlist.Allowlist, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // G304: path is the operator-set EGRESS_ALLOWLIST_PATH
	if err != nil {
		return nil, fmt.Errorf("read allowlist: %w", err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	var f file
	if err := dec.Decode(&f); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("allowlist %s is empty", path)
		}
		return nil, fmt.Errorf("parse allowlist %s: %w", path, err)
	}
	var next file
	if err := dec.Decode(&next); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("allowlist %s must hold a single YAML document", path)
	}
	if len(f.Entries) == 0 {
		return nil, errors.New("allowlist has no entries; the proxy would refuse every request")
	}
	entries := make([]allowlist.Entry, 0, len(f.Entries))
	for i, e := range f.Entries {
		if e.Port < 1 || e.Port > math.MaxUint16 {
			return nil, fmt.Errorf("allowlist entry %d (%s): port %d is not 1-65535", i, e.Host, e.Port)
		}
		entries = append(entries, allowlist.Entry{Host: e.Host, Port: uint16(e.Port), AllowPrivate: e.AllowPrivate}) //nolint:gosec // G115: checked to be 1-65535 above
	}
	return allowlist.New(entries)
}
