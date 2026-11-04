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
	"strings"
	"testing"
)

// goRedisImport is the import path of the client whose key commands the guard
// inspects. A file that does not import it is never scanned, so a Get or Set of
// another type cannot be mistaken for a Redis command.
const goRedisImport = `"github.com/redis/go-redis/v9"`

// redisKeyCommandRe matches the go-redis commands whose key argument must be a
// prefixed key. XAdd is covered through its XAddArgs literal, XRead/XReadGroup
// through the Streams field of their args literal.
var redisKeyCommandRe = regexp.MustCompile(`^(XAck|XAutoClaim|XClaim\w*|XPending\w*|XInfo\w*|XGroup\w*|XRange\w*|XRevRange\w*|XTrim\w*|XLen|XDel|Del|Get\w*|Set\w*|Expire\w*|Eval\w*)$`)

// redisScriptRunners are the methods that take a script's keys as a []string
// literal in their third argument (redis.Script.Run).
var redisScriptRunners = map[string]bool{"Run": true, "RunRO": true}

// redisKeysGuardRoots returns the directories of every Go module in go.work
// except the end-to-end harness, which addresses streams as an external client
// and has its own helpers.
func redisKeysGuardRoots(t *testing.T, root string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "go.work")) //nolint:gosec // G304: the repository's own go.work
	if err != nil {
		t.Fatalf("read go.work: %v", err)
	}
	var dirs []string
	inUse := false
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "use ("):
			inUse = true
		case inUse && line == ")":
			inUse = false
		case inUse && line != "":
			dir := filepath.Clean(line)
			if !strings.HasPrefix(dir, "tests") {
				dirs = append(dirs, filepath.Join(root, dir))
			}
		}
	}
	if len(dirs) == 0 {
		t.Fatal("go.work lists no module to scan")
	}
	return dirs
}

// TestRedisKeysCarryThePrefix fails for every production Go site that hands a
// Redis command an unprefixed key: an XAddArgs whose Stream is not a Key call,
// or a key command whose key is a streams constant, a string literal or a
// concatenation that starts with one. Every key lives under streams.KeyPrefix,
// built only by pkg/redis.Key.
func TestRedisKeysCarryThePrefix(t *testing.T) {
	root := repoRootFromTest(t)
	exempt := filepath.Join(root, "pkg", "redis", "keys.go")
	var violations []string
	scanned := 0
	for _, dir := range redisKeysGuardRoots(t, root) {
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				switch d.Name() {
				case "vendor", "node_modules", "testdata", ".git":
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || path == exempt {
				return nil
			}
			found, inspected := scanRedisKeys(t, root, path, nil)
			violations = append(violations, found...)
			if inspected {
				scanned++
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}
	for _, v := range violations {
		t.Error(v)
	}
	// A guard that inspected no file would pass vacuously.
	if scanned == 0 {
		t.Fatal("the guard inspected no Go file that imports go-redis: its scan roots or file filter are wrong")
	}
}

// TestRedisKeyGuardFlagsAnUnprefixedKey proves the guard flags each unprefixed
// shape and accepts the prefixed ones, on source fixtures.
func TestRedisKeyGuardFlagsAnUnprefixedKey(t *testing.T) {
	const header = "package p\n\nimport (\n\tgoredis " + goRedisImport + "\n\tpkgredis \"x/pkg/redis\"\n\t\"x/pkg/streams\"\n)\n\nconst legacyPrefix = \"agent:\"\n\n"
	cases := []struct {
		name string
		body string
		want int
	}{
		{"XAddArgs with a streams constant", `var _ = &goredis.XAddArgs{Stream: streams.TaskDispatchV1}`, 1},
		{"XAddArgs with a literal", `var _ = goredis.XAddArgs{Stream: "task.dispatch:v1"}`, 1},
		{"XAddArgs with a variable", `func f(name string) any { return &goredis.XAddArgs{Stream: name} }`, 1},
		{"XAddArgs without a stream", `var _ = goredis.XAddArgs{}`, 1},
		{"XAddArgs with Key", `var _ = &goredis.XAddArgs{Stream: pkgredis.Key(streams.TaskDispatchV1)}`, 0},
		{"XAddArgs with a helper other than Key", `var _ = &goredis.XAddArgs{Stream: pkgredis.LegacyKey(streams.TaskDispatchV1)}`, 1},
		{"XAddArgs with a bare Key call", `var _ = &goredis.XAddArgs{Stream: Key("x")}`, 0},
		{"XAck with a streams constant", `func f(c *goredis.Client) { c.XAck(nil, streams.TaskDispatchV1, "g", "1") }`, 1},
		{"XAck with a Key variable", `func f(c *goredis.Client, key string) { c.XAck(nil, key, "g", "1") }`, 0},
		{"XLen with a literal", `func f(c *goredis.Client) { c.XLen(nil, "s") }`, 1},
		{"XInfoGroups with Key", `func f(c *goredis.Client) { c.XInfoGroups(nil, pkgredis.Key(streams.TaskDispatchV1)) }`, 0},
		{"Del with a concatenation", `func f(c *goredis.Client, u string) { c.Del(nil, "user:"+u) }`, 1},
		{"Del with a const prefix", `func f(c *goredis.Client, u string) { c.Del(nil, legacyPrefix+u) }`, 1},
		{"Get with a literal", `func f(c *goredis.Client) { c.Get(nil, "k") }`, 1},
		{"Get with a prefixed concatenation", `func f(c *goredis.Client, u string) { c.Get(nil, pkgredis.Key("k:"+u)) }`, 0},
		{"Set with a variable", `func f(c *goredis.Client, k string) { c.Set(nil, k, 1, 0) }`, 0},
		{"Expire with a literal", `func f(c *goredis.Client) { c.Expire(nil, "k", 0) }`, 1},
		{"Eval keys with a literal", `func f(c *goredis.Client) { c.Eval(nil, "return 1", []string{"k"}) }`, 1},
		{"Eval keys with Key", `func f(c *goredis.Client) { c.Eval(nil, "return 1", []string{pkgredis.Key("k")}) }`, 0},
		{"Script.Run keys with a literal", `func f(s *goredis.Script, c goredis.Scripter) { s.Run(nil, c, []string{"k"}) }`, 1},
		{"Script.Run keys with Key", `func f(s *goredis.Script, c goredis.Scripter) { s.Run(nil, c, []string{pkgredis.Key("k")}) }`, 0},
		{"XReadGroup with a streams constant", `func f(c *goredis.Client) { c.XReadGroup(nil, &goredis.XReadGroupArgs{Streams: []string{streams.TaskDispatchV1, ">"}}) }`, 1},
		{"XReadGroup with Key and the id marker", `func f(c *goredis.Client, key string) { c.XReadGroup(nil, &goredis.XReadGroupArgs{Streams: []string{pkgredis.Key(key), ">"}}) }`, 0},
		{"XReadGroup with variables", `func f(c *goredis.Client, key string) { c.XReadGroup(nil, &goredis.XReadGroupArgs{Streams: []string{key, ">"}}) }`, 0},
		{"XAutoClaim with a literal stream", `func f(c *goredis.Client) { c.XAutoClaim(nil, &goredis.XAutoClaimArgs{Stream: "s"}) }`, 1},
		{"XPendingExt with Key", `func f(c *goredis.Client) { c.XPendingExt(nil, &goredis.XPendingExtArgs{Stream: pkgredis.Key("s")}) }`, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := scanRedisKeysSource(t, header+tc.body)
			if len(got) != tc.want {
				t.Fatalf("want %d violations, got %d: %v", tc.want, len(got), got)
			}
		})
	}

	t.Run("a file without the go-redis import is not scanned", func(t *testing.T) {
		src := "package p\n\nfunc f(c interface{ Get(string) }) { c.Get(\"k\") }\n"
		if got := scanRedisKeysSource(t, src); len(got) != 0 {
			t.Fatalf("a file that does not import go-redis was flagged: %v", got)
		}
	})
}

// scanRedisKeysSource scans one fixture source.
func scanRedisKeysSource(t *testing.T, src string) []string {
	t.Helper()
	found, _ := scanRedisKeys(t, "", "fixture.go", []byte(src))
	return found
}

// scanRedisKeys returns one message per unprefixed Redis key in the Go file at
// path (or in src when it is not nil). Messages name the file relative to root.
// The boolean reports whether the file imports go-redis and was inspected.
func scanRedisKeys(t *testing.T, root, path string, src []byte) ([]string, bool) {
	t.Helper()
	var source any
	if src != nil {
		source = src
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, source, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	imports := false
	for _, imp := range f.Imports {
		if imp.Path.Value == goRedisImport {
			imports = true
		}
	}
	if !imports {
		return nil, false
	}
	consts := packageStringConsts(t, filepath.Dir(path), f, src != nil)
	rel := path
	if root != "" {
		if r, err := filepath.Rel(root, path); err == nil {
			rel = r
		}
	}
	var found []string
	flag := func(pos token.Pos, format string, args ...any) {
		found = append(found, fmt.Sprintf("%s:%d: %s", rel, fset.Position(pos).Line, fmt.Sprintf(format, args...)))
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.CompositeLit:
			checkKeyLiteral(n, consts, flag)
		case *ast.CallExpr:
			checkKeyCall(n, consts, flag)
		}
		return true
	})
	return found, true
}

// checkKeyLiteral checks the Stream of an XAddArgs literal and the stream fields
// of the other go-redis argument structs.
func checkKeyLiteral(lit *ast.CompositeLit, consts map[string]bool, flag func(token.Pos, string, ...any)) {
	typeName := literalTypeName(lit)
	switch {
	case typeName == "XAddArgs":
		stream := fieldValue(lit, "Stream")
		if stream == nil || !isKeyCall(stream) {
			flag(lit.Pos(), "XAddArgs.Stream must be a pkgredis.Key(...) call")
		}
	case typeName == "XReadGroupArgs" || typeName == "XReadArgs":
		streams, ok := fieldValue(lit, "Streams").(*ast.CompositeLit)
		if !ok {
			return
		}
		// Streams is the keys followed by one id marker per key.
		for _, key := range streams.Elts[:len(streams.Elts)/2] {
			if unprefixed(key, consts) {
				flag(key.Pos(), "%s.Streams key is not a pkgredis.Key(...) call", typeName)
			}
		}
	case strings.HasPrefix(typeName, "X") && strings.HasSuffix(typeName, "Args"):
		if stream := fieldValue(lit, "Stream"); stream != nil && unprefixed(stream, consts) {
			flag(stream.Pos(), "%s.Stream is not a pkgredis.Key(...) call", typeName)
		}
	}
}

// checkKeyCall checks the key arguments of a go-redis key command.
func checkKeyCall(call *ast.CallExpr, consts map[string]bool, flag func(token.Pos, string, ...any)) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return
	}
	name := sel.Sel.Name
	switch {
	case redisScriptRunners[name]:
		if len(call.Args) >= 3 {
			checkKeyList(name, call.Args[2], consts, flag)
		}
	case strings.HasPrefix(name, "Eval"):
		if redisKeyCommandRe.MatchString(name) && len(call.Args) >= 3 {
			checkKeyList(name, call.Args[2], consts, flag)
		}
	case redisKeyCommandRe.MatchString(name):
		// The first argument is the context, the second the key (every key of Del).
		if len(call.Args) < 2 {
			return
		}
		last := 2
		if name == "Del" {
			last = len(call.Args)
		}
		for _, arg := range call.Args[1:last] {
			if unprefixed(arg, consts) {
				flag(arg.Pos(), "%s key is not a pkgredis.Key(...) call", name)
			}
		}
	}
}

// checkKeyList checks the elements of a []string literal of keys.
func checkKeyList(command string, arg ast.Expr, consts map[string]bool, flag func(token.Pos, string, ...any)) {
	list, ok := arg.(*ast.CompositeLit)
	if !ok {
		return
	}
	for _, key := range list.Elts {
		if unprefixed(key, consts) {
			flag(key.Pos(), "%s key is not a pkgredis.Key(...) call", command)
		}
	}
}

// literalTypeName returns the type name of a composite literal, without package.
func literalTypeName(lit *ast.CompositeLit) string {
	switch typ := lit.Type.(type) {
	case *ast.Ident:
		return typ.Name
	case *ast.SelectorExpr:
		return typ.Sel.Name
	}
	return ""
}

// fieldValue returns the value of the named field of a keyed struct literal.
func fieldValue(lit *ast.CompositeLit, field string) ast.Expr {
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		if id, ok := kv.Key.(*ast.Ident); ok && id.Name == field {
			return kv.Value
		}
	}
	return nil
}

// isKeyCall reports whether expr is a call of Key, bare or through a package
// qualifier.
func isKeyCall(expr ast.Expr) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	var name string
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		name = fn.Name
	case *ast.SelectorExpr:
		name = fn.Sel.Name
	}
	return name == "Key"
}

// unprefixed reports whether expr is a key built from a contract constant or a
// string literal: a `streams.` selector, a string literal, a package-level
// string constant, or a `+` expression whose first operand is one of those. A
// call (Key, any helper) and a variable are never flagged.
func unprefixed(expr ast.Expr, consts map[string]bool) bool {
	switch e := expr.(type) {
	case *ast.ParenExpr:
		return unprefixed(e.X, consts)
	case *ast.BasicLit:
		return e.Kind == token.STRING
	case *ast.Ident:
		return consts[e.Name]
	case *ast.SelectorExpr:
		pkg, ok := e.X.(*ast.Ident)
		return ok && pkg.Name == "streams"
	case *ast.BinaryExpr:
		return e.Op == token.ADD && unprefixed(e.X, consts)
	}
	return false
}

// packageStringConsts returns the names of the string constants declared at
// package level in the files of dir, so a key built from one is seen as the
// literal it stands for. A fixture is scanned alone, from its own declarations.
func packageStringConsts(t *testing.T, dir string, own *ast.File, fixture bool) map[string]bool {
	t.Helper()
	consts := map[string]bool{}
	collect := func(f *ast.File) {
		for _, decl := range f.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				vs := spec.(*ast.ValueSpec)
				for i, name := range vs.Names {
					if i < len(vs.Values) && unprefixed(vs.Values[i], nil) {
						consts[name.Name] = true
					}
				}
			}
		}
	}
	collect(own)
	if fixture {
		return consts
	}
	siblings, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatalf("glob %s: %v", dir, err)
	}
	fset := token.NewFileSet()
	for _, path := range siblings {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		collect(f)
	}
	return consts
}
