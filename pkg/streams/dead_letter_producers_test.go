package streams_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
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

	producers := deadLetterProducers(t)
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

// deadLetterProducers returns the producers contract.yaml lists for
// consumer.dead_letter:v1.
func deadLetterProducers(t *testing.T) []string {
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
		if s.Name == streams.ConsumerDeadLetterV1 {
			return s.Producers
		}
	}
	t.Fatalf("contract.yaml has no %s stream", streams.ConsumerDeadLetterV1)
	return nil
}
