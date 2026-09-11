package streams_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// streamsImportPath is the transport-contract package: Redis stream names and
// consumer-group names. Domain code must not reach it.
const streamsImportPath = "github.com/carolsimone/continuo/pkg/streams"

// TestDomainPackagesDoNotImportStreams fails on any Go file under a module's
// domain/ tree that imports pkg/streams. Stream and consumer-group names are a
// transport concern; the closed value sets domain code does name — the
// contract vocabularies — are generated into pkg/domain/model instead, so a
// domain package spells model.ParseFailureKind and never depends on the
// messaging contract. Test files are guarded too: a domain test that reaches
// for a stream constant is the same coupling.
//
// The Python counterpart is
// topology-controller/tests/test_domain_imports_no_streams_contract.py.
func TestDomainPackagesDoNotImportStreams(t *testing.T) {
	root := repoRootFromTest(t)
	fset := token.NewFileSet()
	checked := 0
	for _, mod := range domainModules {
		dir := filepath.Join(root, mod, "domain")
		if _, err := os.Stat(dir); err != nil {
			t.Fatalf("domain dir %s: %v", dir, err)
		}
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(d.Name(), ".go") {
				return nil
			}
			f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			checked++
			for _, imp := range f.Imports {
				if strings.Trim(imp.Path.Value, `"`) == streamsImportPath {
					rel, _ := filepath.Rel(root, path)
					t.Errorf("%s: domain code imports %q — stream and group names are transport; "+
						"name a contract vocabulary from pkg/domain/model instead", rel, streamsImportPath)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}
	if checked == 0 {
		t.Fatal("guard inspected no domain files — the discovery in domainModules is broken")
	}
}
