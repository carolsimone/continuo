package streams_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/carolsimone/continuo/pkg/streams"
)

// pythonServiceNameRe matches topology-controller's SERVICE_NAME assignment.
var pythonServiceNameRe = regexp.MustCompile(`(?m)^SERVICE_NAME\s*=\s*"([^"]+)"\s*$`)

// TestDeadLetterProducersMatchTheServiceNames pins the producers of
// consumer.dead_letter:v1 in contract.yaml to the service names the consumers
// stamp on the dead letters they write: every Go service's config.ServiceName
// and topology-controller's SERVICE_NAME. A service that runs stream consumers
// declares that constant (TestMainsThatRunConsumersNameTheirService), so the
// two sets are equal.
func TestDeadLetterProducersMatchTheServiceNames(t *testing.T) {
	root := repoRootFromTest(t)
	names := goServiceNames(t, root)
	names["topology-controller/config/config.py"] = pythonServiceName(t, filepath.Join(root, "topology-controller", "config", "config.py"))

	producers := streamProducers(t, streams.ConsumerDeadLetterV1)
	stamped := make([]string, 0, len(names))
	for file, name := range names {
		stamped = append(stamped, name)
		if !slices.Contains(producers, name) {
			t.Errorf("%s names the service %q, which is not a producer of %s in contract.yaml", file, name, streams.ConsumerDeadLetterV1)
		}
	}
	for _, p := range producers {
		if !slices.Contains(stamped, p) {
			t.Errorf("contract.yaml lists %q as a producer of %s, but no service declares that name", p, streams.ConsumerDeadLetterV1)
		}
	}
}

// goServiceNames returns the ServiceName constant of every */config/config.go
// that declares one, keyed by the file's path relative to root.
func goServiceNames(t *testing.T, root string) map[string]string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, "*", "config", "config.go"))
	if err != nil {
		t.Fatalf("glob config.go: %v", err)
	}
	names := map[string]string{}
	fset := token.NewFileSet()
	for _, path := range files {
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		rel, _ := filepath.Rel(root, path)
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, spec := range gd.Specs {
				vs := spec.(*ast.ValueSpec)
				for i, id := range vs.Names {
					if id.Name != "ServiceName" {
						continue
					}
					lit, ok := vs.Values[i].(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						t.Fatalf("%s: ServiceName must be a string literal", rel)
					}
					name, err := strconv.Unquote(lit.Value)
					if err != nil {
						t.Fatalf("%s: unquote ServiceName: %v", rel, err)
					}
					names[rel] = name
				}
			}
		}
	}
	if len(names) == 0 {
		t.Fatal("no */config/config.go declares a ServiceName constant")
	}
	return names
}

// pythonServiceName returns the SERVICE_NAME string assigned in path.
func pythonServiceName(t *testing.T, path string) string {
	t.Helper()
	src, err := os.ReadFile(path) //nolint:gosec // G304: path is built from the repo root and fixed segments
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	m := pythonServiceNameRe.FindSubmatch(src)
	if m == nil {
		t.Fatalf("%s: no SERVICE_NAME = \"...\" assignment", path)
	}
	return string(m[1])
}

// streamProducers returns the producers contract.yaml lists for stream.
func streamProducers(t *testing.T, stream string) []string {
	t.Helper()
	data, err := os.ReadFile("contract.yaml")
	if err != nil {
		t.Fatalf("read contract.yaml: %v", err)
	}
	var c struct {
		Streams []struct {
			Name      string   `yaml:"name"`
			Producers []string `yaml:"producers"`
		} `yaml:"streams"`
	}
	if err := yaml.Unmarshal(data, &c); err != nil {
		t.Fatalf("parse contract.yaml: %v", err)
	}
	for _, s := range c.Streams {
		if s.Name == stream {
			return s.Producers
		}
	}
	t.Fatalf("contract.yaml has no %s stream", stream)
	return nil
}

// outboxImportPath is the package whose NewProcessor runs an outbox relay.
const outboxImportPath = "github.com/carolsimone/continuo/pkg/outbox"

// TestOutboxDeadLetterProducersMatchTheOutboxRelays pins the producers of
// outbox.dead_letter:v1 in contract.yaml to the services that run an outbox
// relay. A relay writes the dead letter of a row it gives up on to its own
// outbox and publishes it to outbox.dead_letter:v1, so a service is a producer
// exactly when its own code calls pkg/outbox's NewProcessor. The producers
// list decides which Redis logins may write the stream, so a relay missing
// from it is refused with NOPERM on its first dead letter.
func TestOutboxDeadLetterProducersMatchTheOutboxRelays(t *testing.T) {
	root := repoRootFromTest(t)
	relays := outboxRelayServices(t, root)
	if len(relays) == 0 {
		t.Fatal("no service calls pkg/outbox's NewProcessor: the scan found nothing")
	}
	producers := streamProducers(t, streams.OutboxDeadLetterV1)
	for service, file := range relays {
		if !slices.Contains(producers, service) {
			t.Errorf("%s runs an outbox relay, but %s is not a producer of %s in contract.yaml", file, service, streams.OutboxDeadLetterV1)
		}
	}
	for _, p := range producers {
		if _, ok := relays[p]; !ok {
			t.Errorf("contract.yaml lists %q as a producer of %s, but no non-test file under %s/ calls pkg/outbox's NewProcessor", p, streams.OutboxDeadLetterV1, p)
		}
	}
}

// outboxRelayServices returns each service directory under root with a
// non-test Go file that calls pkg/outbox's NewProcessor, mapped to the first
// such file. pkg/ (where the relay lives), cli/, tests/ and hidden directories
// are not services.
func outboxRelayServices(t *testing.T, root string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read %s: %v", root, err)
	}
	relays := map[string]string{}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() || strings.HasPrefix(name, ".") || slices.Contains([]string{"pkg", "cli", "tests", "ui", "node_modules"}, name) {
			continue
		}
		walkErr := filepath.WalkDir(filepath.Join(root, name), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if base := d.Name(); base == "node_modules" || base == "vendor" || (strings.HasPrefix(base, ".") && path != filepath.Join(root, name)) {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if err != nil {
				return fmt.Errorf("parse %s: %w", path, err)
			}
			if callsNewProcessor(f) {
				if _, seen := relays[name]; !seen {
					rel, _ := filepath.Rel(root, path)
					relays[name] = rel
				}
			}
			return nil
		})
		if walkErr != nil {
			t.Fatalf("scan %s: %v", name, walkErr)
		}
	}
	return relays
}

// callsNewProcessor reports whether f calls NewProcessor on its import of
// pkg/outbox, under whatever name f imports it.
func callsNewProcessor(f *ast.File) bool {
	local := ""
	for _, imp := range f.Imports {
		if path, _ := strconv.Unquote(imp.Path.Value); path == outboxImportPath {
			local = "outbox"
			if imp.Name != nil {
				local = imp.Name.Name
			}
		}
	}
	if local == "" {
		return false
	}
	found := false
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return !found
		}
		if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "NewProcessor" {
			if id, ok := sel.X.(*ast.Ident); ok && id.Name == local {
				found = true
			}
		}
		return !found
	})
	return found
}
