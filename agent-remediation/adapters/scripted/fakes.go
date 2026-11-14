package scripted

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/service/conformance"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// artifactURIPrefix is the URI prefix of every artifact the world stores.
const artifactURIPrefix = "s3://conformance/"

// checkout is the repository at the trigger's commit, read file by file or
// fetched whole into a directory of its own. Any other repository or commit
// does not exist.
type checkout struct {
	repo   string
	commit string
	files  map[string]string
}

var (
	_ ports.SourceReader = checkout{}
	_ ports.RepoArchive  = checkout{}
)

func (c checkout) at(repo, ref string) bool { return repo == c.repo && ref == c.commit }

// ReadFile returns the file at p.
func (c checkout) ReadFile(_ context.Context, repo, ref, p string) (string, error) {
	content, ok := c.files[p]
	if !ok || !c.at(repo, ref) {
		return "", ports.ErrSourceNotFound
	}
	return content, nil
}

// ListDir returns the files directly inside dir, sorted.
func (c checkout) ListDir(_ context.Context, repo, ref, dir string) ([]string, error) {
	if !c.at(repo, ref) {
		return nil, ports.ErrSourceNotFound
	}
	dir = path.Clean(dir)
	var out []string
	for p := range c.files {
		if path.Dir(p) == dir {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil, ports.ErrSourceNotFound
	}
	sort.Strings(out)
	return out, nil
}

// Fetch writes every file into a new temporary directory, in path order, and
// returns it, with a cleanup that removes it.
func (c checkout) Fetch(_ context.Context, repo, ref string) (string, func(), error) {
	if !c.at(repo, ref) {
		return "", nil, ports.ErrSourceNotFound
	}
	root, err := os.MkdirTemp("", "conformance-checkout-*")
	if err != nil {
		return "", nil, fmt.Errorf("create checkout directory: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(root) }
	for _, p := range slices.Sorted(maps.Keys(c.files)) {
		content := c.files[p]
		full := filepath.Join(root, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
			cleanup()
			return "", nil, fmt.Errorf("create directory for %s: %w", p, err)
		}
		if err := os.WriteFile(full, []byte(content), 0o600); err != nil {
			cleanup()
			return "", nil, fmt.Errorf("write %s: %w", p, err)
		}
	}
	return root, cleanup, nil
}

// objectStore is object storage: the fixture's objects and every artifact the
// run writes, each read back by its URI.
type objectStore struct {
	mu      sync.Mutex
	objects map[string]string
}

var (
	_ ports.EvidenceReader = (*objectStore)(nil)
	_ ports.ArtifactWriter = (*objectStore)(nil)
)

func newObjectStore(objects map[string]string) *objectStore {
	s := &objectStore{objects: make(map[string]string, len(objects))}
	for uri, body := range objects {
		s.objects[uri] = body
	}
	return s
}

// Fetch returns the object at uri.
func (s *objectStore) Fetch(_ context.Context, uri string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	body, ok := s.objects[uri]
	if !ok {
		return "", ports.ErrNotFound
	}
	return body, nil
}

// Write stores body under key and returns its URI.
func (s *objectStore) Write(_ context.Context, key, body, _ string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	uri := artifactURIPrefix + key
	s.objects[uri] = body
	return uri, nil
}

// bundle is the release's code bundle, by node id.
type bundle struct {
	nodes map[string]ports.CandidateSource
}

var _ ports.CandidateSourceReader = bundle{}

// NodeSource returns the node's entry in the bundle.
func (b bundle) NodeSource(_ context.Context, _, uniqueID, _ string) (ports.CandidateSource, error) {
	src, ok := b.nodes[uniqueID]
	if !ok {
		return ports.CandidateSource{}, ports.ErrNotFound
	}
	return src, nil
}

// catalog answers the catalog's reads from the fixture.
type catalog struct{ c conformance.Catalog }

var (
	_ ports.NodeLocator          = catalog{}
	_ ports.UpstreamChangeReader = catalog{}
	_ ports.VersionReader        = catalog{}
	_ ports.PrecedentReader      = catalog{}
)

// errNotInGraph is what locating a node the catalog does not place returns.
var errNotInGraph = errors.New("node not found in the promoted graph")

// Locate returns where the node lives.
func (c catalog) Locate(_ context.Context, uniqueID string) (string, string, error) {
	loc, ok := c.c.Locations[uniqueID]
	if !ok {
		return "", "", fmt.Errorf("%s: %w", uniqueID, errNotInGraph)
	}
	return loc.FilePath, loc.Service, nil
}

// UpstreamChanges returns what changed upstream of the node.
func (c catalog) UpstreamChanges(_ context.Context, uniqueID string) ([]prompt.UpstreamChange, error) {
	return c.c.UpstreamChanges[uniqueID], nil
}

// CurrentVersion returns the version of the node that runs now.
func (c catalog) CurrentVersion(_ context.Context, uniqueID string) (ports.CurrentVersion, bool, error) {
	v, ok := c.c.CurrentVersions[uniqueID]
	return v, ok, nil
}

// Precedents returns the fixture's precedents, at most q.Limit of them when it
// is set.
func (c catalog) Precedents(_ context.Context, q ports.PrecedentQuery) ([]prompt.Precedent, error) {
	out := c.c.Precedents
	if q.Limit > 0 && len(out) > int(q.Limit) {
		out = out[:q.Limit]
	}
	return out, nil
}

// release answers the failing release's reads from the fixture.
type release struct{ r conformance.Release }

var _ ports.ReleaseReader = release{}

// ImageTag returns the release's image tag of every service.
func (r release) ImageTag(_ context.Context, releaseID, service string) (string, error) {
	if r.r.ImageTag == "" {
		return "", fmt.Errorf("the fixture names no image tag for %s in release %s", service, releaseID)
	}
	return r.r.ImageTag, nil
}

// FailingNodes returns the release's failing nodes.
func (r release) FailingNodes(context.Context, string) (map[string]string, error) {
	return r.r.FailingNodes, nil
}

// packager is the contract packaging tool. It accepts a contract directory
// that exists and holds a yaml file, inside a repository root that exists.
type packager struct{ p conformance.Packager }

var _ ports.ContractPackager = packager{}

// Merge returns the fixture's merged contract, or its refusal.
func (k packager) Merge(_ context.Context, contractDir, repoRoot, _, _ string) ([]byte, error) {
	if err := isDir(repoRoot); err != nil {
		return nil, fmt.Errorf("repository root: %w", err)
	}
	entries, err := os.ReadDir(contractDir)
	if err != nil {
		return nil, fmt.Errorf("contract directory: %w", err)
	}
	if !holdsYAML(entries) {
		return nil, fmt.Errorf("contract directory %s holds no contract yaml", contractDir)
	}
	if k.p.Refuse != "" {
		return nil, fmt.Errorf("%w: %s", ports.ErrContractRejected, k.p.Refuse)
	}
	if k.p.Merged == "" {
		return nil, errors.New("the fixture names no merged contract")
	}
	return []byte(k.p.Merged), nil
}

func isDir(p string) error {
	info, err := os.Stat(p)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", p)
	}
	return nil
}

func holdsYAML(entries []os.DirEntry) bool {
	for _, e := range entries {
		ext := strings.ToLower(filepath.Ext(e.Name()))
		if !e.IsDir() && (ext == ".yml" || ext == ".yaml") {
			return true
		}
	}
	return false
}
