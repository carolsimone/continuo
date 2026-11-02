package streams_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// catalogFormatImportRoots are the packages that carry a stored file format
// or an implementation: a compressor, catalog's serialization packages (the
// export file's codec among them) and catalog's adapters. catalog's service
// and domain layers reach every one of them through a port, so none may
// import them.
var catalogFormatImportRoots = []string{
	"compress",
	"github.com/carolsimone/continuo/catalog/serialization",
	"github.com/carolsimone/continuo/catalog/adapters",
}

// catalogExportPackages are the service and domain packages that handle a
// tenant's export as records. They also stay clear of encoding/json, the
// codec the export file is written in; the handlers and queries of the
// service layer, which decode message payloads and diff code, keep it.
var catalogExportPackages = []string{
	"catalog/service/backup",
	"catalog/service/ports",
	"catalog/service/export",
	"catalog/service/export/graphschema",
}

// TestCatalogServiceAndDomainDoNotOwnTheExportFormat fails on any non-test Go
// file under catalog/service or catalog/domain that imports a compressor,
// catalog's serialization packages or catalog's adapters, and on any one of
// the export's own packages (catalogExportPackages) that imports
// encoding/json. The backup store port speaks records; its adapter in
// catalog/adapters/s3 owns the gzip, the export file's codec and the key
// layout, so the format changes in one place.
func TestCatalogServiceAndDomainDoNotOwnTheExportFormat(t *testing.T) {
	root := repoRootFromTest(t)
	fset := token.NewFileSet()
	checked := 0
	for _, layer := range []string{"catalog/service", "catalog/domain"} {
		dir := filepath.Join(root, layer)
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(d.Name(), ".go") || strings.HasSuffix(d.Name(), "_test.go") {
				return nil
			}
			f, perr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
			if perr != nil {
				t.Fatalf("parse %s: %v", path, perr)
			}
			checked++
			rel, _ := filepath.Rel(root, path)
			for _, imp := range catalogFormatImportsIn(f, filepath.ToSlash(filepath.Dir(rel))) {
				t.Errorf("%s:%d: imports %q — the export's format and the adapters stay behind ports.BackupStore; "+
					"encode, compress and lay out keys in catalog/adapters/s3", rel, fset.Position(imp.pos).Line, imp.path)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}
	if checked == 0 {
		t.Fatal("guard inspected no catalog service or domain file — its discovery is broken")
	}
}

// TestCatalogExportFormatGuardPackagesExist fails when a folder of
// catalogExportPackages is not a directory, so a package that moves cannot
// leave the encoding/json rule watching a path nothing lives at.
func TestCatalogExportFormatGuardPackagesExist(t *testing.T) {
	root := repoRootFromTest(t)
	for _, dir := range catalogExportPackages {
		info, err := os.Stat(filepath.Join(root, filepath.FromSlash(dir)))
		if err != nil || !info.IsDir() {
			t.Errorf("catalogExportPackages names %q, which is not a directory under the repository root (err: %v)", dir, err)
		}
	}
}

// catalogBackupCommandDir is the operator command that wires catalog's
// adapters to take, list and restore exports.
const catalogBackupCommandDir = "catalog/cmd/catalog-backup"

// TestCatalogBackupCommandDoesNotOwnTheExportFormat fails when a non-test file
// of catalog/cmd/catalog-backup imports a compressor or catalog's
// serialization packages. The command wires adapters, so it may import them,
// and it prints its results as JSON itself; it must not encode, compress or
// lay out an export, which is catalog/adapters/s3's.
func TestCatalogBackupCommandDoesNotOwnTheExportFormat(t *testing.T) {
	root := repoRootFromTest(t)
	dir := filepath.Join(root, filepath.FromSlash(catalogBackupCommandDir))
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	fset := token.NewFileSet()
	checked := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		f, perr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if perr != nil {
			t.Fatalf("parse %s: %v", path, perr)
		}
		checked++
		for _, imp := range catalogBackupCommandImportsIn(f) {
			t.Errorf("%s/%s:%d: imports %q — the command wires catalog/adapters/s3 and never owns the export's format; "+
				"encode, compress and lay out keys there", catalogBackupCommandDir, e.Name(), fset.Position(imp.pos).Line, imp.path)
		}
	}
	if checked == 0 {
		t.Fatalf("guard inspected no file of %s — its discovery is broken", catalogBackupCommandDir)
	}
}

// catalogBackupCommandImportsIn returns, in source order, the imports of f, a
// file of catalog/cmd/catalog-backup, that name a compressor or a package of
// catalog's serialization.
func catalogBackupCommandImportsIn(f *ast.File) []catalogFormatImport {
	var out []catalogFormatImport
	for _, imp := range f.Imports {
		p := strings.Trim(imp.Path.Value, `"`)
		if p == "compress" || strings.HasPrefix(p, "compress/") ||
			p == "github.com/carolsimone/continuo/catalog/serialization" ||
			strings.HasPrefix(p, "github.com/carolsimone/continuo/catalog/serialization/") {
			out = append(out, catalogFormatImport{path: p, pos: imp.Pos()})
		}
	}
	return out
}

// TestCatalogExportFormatGuardFlagsViolations pins the detector so it cannot
// rot into a guard that never fires.
func TestCatalogExportFormatGuardFlagsViolations(t *testing.T) {
	src := `package x
import (
	"context"
	"compress/gzip"
	"compress/flate"
	"encoding/json"
	"github.com/carolsimone/continuo/catalog/serialization/exportfile"
	"github.com/carolsimone/continuo/catalog/serialization"
	s3infra "github.com/carolsimone/continuo/catalog/adapters/s3"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/catalog/serializationx"
	"compressx"
)
`
	f, err := parser.ParseFile(token.NewFileSet(), "x.go", src, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	paths := func(dir string) []string {
		var out []string
		for _, imp := range catalogFormatImportsIn(f, dir) {
			out = append(out, imp.path)
		}
		return out
	}
	always := []string{
		"compress/gzip",
		"compress/flate",
		"github.com/carolsimone/continuo/catalog/serialization/exportfile",
		"github.com/carolsimone/continuo/catalog/serialization",
		"github.com/carolsimone/continuo/catalog/adapters/s3",
	}
	if got := paths("catalog/service/handlers"); !reflect.DeepEqual(got, always) {
		t.Fatalf("in service/handlers flagged %v, want %v", got, always)
	}
	withJSON := []string{
		"compress/gzip",
		"compress/flate",
		"encoding/json",
		"github.com/carolsimone/continuo/catalog/serialization/exportfile",
		"github.com/carolsimone/continuo/catalog/serialization",
		"github.com/carolsimone/continuo/catalog/adapters/s3",
	}
	if got := paths("catalog/service/backup"); !reflect.DeepEqual(got, withJSON) {
		t.Fatalf("in service/backup flagged %v, want %v", got, withJSON)
	}

	var command []string
	for _, imp := range catalogBackupCommandImportsIn(f) {
		command = append(command, imp.path)
	}
	wantCommand := []string{
		"compress/gzip",
		"compress/flate",
		"github.com/carolsimone/continuo/catalog/serialization/exportfile",
		"github.com/carolsimone/continuo/catalog/serialization",
	}
	if !reflect.DeepEqual(command, wantCommand) {
		t.Fatalf("in cmd/catalog-backup flagged %v, want %v (adapters and encoding/json are allowed there)", command, wantCommand)
	}
}

// catalogFormatImport is one import the guard refuses.
type catalogFormatImport struct {
	path string
	pos  token.Pos
}

// catalogFormatImportsIn returns, in source order, the imports of f, a file
// in the directory dir (relative to the repo root), that name a package of
// catalogFormatImportRoots or one under it, and encoding/json when dir is one
// of catalogExportPackages.
func catalogFormatImportsIn(f *ast.File, dir string) []catalogFormatImport {
	jsonBanned := false
	for _, p := range catalogExportPackages {
		jsonBanned = jsonBanned || dir == p
	}
	var out []catalogFormatImport
	for _, imp := range f.Imports {
		p := strings.Trim(imp.Path.Value, `"`)
		banned := jsonBanned && p == "encoding/json"
		for _, root := range catalogFormatImportRoots {
			banned = banned || p == root || strings.HasPrefix(p, root+"/")
		}
		if banned {
			out = append(out, catalogFormatImport{path: p, pos: imp.Pos()})
		}
	}
	return out
}
