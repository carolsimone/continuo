package streams_test

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// The release id rule is written twice: the public release API accepts ids
// matching RELEASE_ID_PATTERN (ui), and announce-topology refuses any id that
// does not match releaseIDPattern (release-controller), because the id becomes
// part of an object key. The two literals must stay identical.
func TestReleaseIDPattern_UIAndAnnounceTopologyAgree(t *testing.T) {
	root := repoRootFromTest(t)
	read := func(parts ...string) string {
		path := filepath.Join(append([]string{root}, parts...)...)
		b, err := os.ReadFile(path) //nolint:gosec // G304: path is built from the repo root and fixed segments
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		return string(b)
	}
	ui := regexp.MustCompile(`RELEASE_ID_PATTERN = /(.+)/;`).FindStringSubmatch(read("ui", "src", "server", "routes", "v1.ts"))
	rc := regexp.MustCompile("releaseIDPattern = regexp\\.MustCompile\\(`(.+)`\\)").FindStringSubmatch(read("release-controller", "service", "handlers", "announce_topology.go"))
	if ui == nil || rc == nil {
		t.Fatalf("pattern literal not found (ui: %v, release-controller: %v)", ui != nil, rc != nil)
	}
	if ui[1] != rc[1] {
		t.Errorf("release id rules differ: ui %q, announce-topology %q", ui[1], rc[1])
	}
}
