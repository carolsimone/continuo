package streams_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// dedupCalls are the calls whose message-ID argument is a dedup identity.
var dedupCalls = map[string]bool{"Dedup": true, "DedupWithOutboxEntryID": true, "Handle": true}

// TestDedupIdentityComesFromDedupMessageID fails for every consumer binding
// that hands a deduplication call, a trigger's MessageID or a stored record's
// DedupKey the raw entry ID (msg.ID) of a stream message. A consumer reads the
// legacy and the prefixed key of one stream side by side, and their entries can
// share an ID, so a binding names its dedup identity with
// pkgredis.DedupMessageID(ctx, msg).
func TestDedupIdentityComesFromDedupMessageID(t *testing.T) {
	root := repoRootFromTest(t)
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
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") ||
				filepath.Base(filepath.Dir(path)) != "redis" || filepath.Base(filepath.Dir(filepath.Dir(path))) != "adapters" {
				return nil
			}
			found := scanDedupIdentity(t, root, path, nil)
			violations = append(violations, found...)
			scanned++
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}
	if scanned == 0 {
		t.Fatal("the guard inspected no adapters/redis file: its scan roots or file filter are wrong")
	}
	if len(violations) > 0 {
		t.Fatalf("%d binding(s) dedup under the raw entry ID msg.ID. Use pkgredis.DedupMessageID(ctx, msg):\n  %s",
			len(violations), strings.Join(violations, "\n  "))
	}
}

// TestDedupIdentityGuardFlagsTheRawEntryID proves the guard flags each shape of
// the raw entry ID and accepts DedupMessageID, on source fixtures.
func TestDedupIdentityGuardFlagsTheRawEntryID(t *testing.T) {
	const header = "package p\n\nfunc f(ctx int, msg M, h H, t T) {\n"
	cases := []struct {
		name string
		body string
		want int
	}{
		{"DedupWithOutboxEntryID with msg.ID", `messageprocessing.DedupWithOutboxEntryID(ctx, r, l, msg.ID, "s", nil, nil)`, 1},
		{"Dedup with msg.ID", `messageprocessing.Dedup(ctx, r, l, msg.ID, "s", nil)`, 1},
		{"Handle with msg.ID", `h.Handle(ctx, msg.ID, nil, nil)`, 1},
		{"MessageID from msg.ID", `t.MessageID = msg.ID`, 1},
		{"DedupWithOutboxEntryID with DedupMessageID", `messageprocessing.DedupWithOutboxEntryID(ctx, r, l, pkgredis.DedupMessageID(ctx, msg), "s", nil, nil)`, 0},
		{"a log line with msg.ID", `logger.Error("x", "message_id", msg.ID)`, 0},
		{"MessageID from DedupMessageID", `t.MessageID = pkgredis.DedupMessageID(ctx, msg)`, 0},
		{"DedupKey built from msg.ID", `_ = D{DedupKey: deadletter.UnreadableKey("s", msg.ID), OriginalMessageID: msg.ID}`, 1},
		{"DedupKey built from DedupMessageID", `_ = D{DedupKey: deadletter.UnreadableKey("s", pkgredis.DedupMessageID(ctx, msg)), OriginalMessageID: msg.ID}`, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := scanDedupIdentity(t, "", "fixture.go", []byte(header+tc.body+"\n}\n"))
			if len(got) != tc.want {
				t.Fatalf("want %d violations, got %d: %v", tc.want, len(got), got)
			}
		})
	}
}

// scanDedupIdentity returns one message per dedup call, MessageID assignment or
// DedupKey field in the Go file at path (or in src when it is not nil) that uses
// the raw entry ID msg.ID.
func scanDedupIdentity(t *testing.T, root, path string, src []byte) []string {
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
	rel := path
	if root != "" {
		if r, err := filepath.Rel(root, path); err == nil {
			rel = r
		}
	}
	var found []string
	flag := func(pos token.Pos, what string) {
		found = append(found, fmt.Sprintf("%s:%d: %s uses msg.ID", rel, fset.Position(pos).Line, what))
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.CallExpr:
			name := ""
			switch fn := n.Fun.(type) {
			case *ast.Ident:
				name = fn.Name
			case *ast.SelectorExpr:
				name = fn.Sel.Name
			}
			if !dedupCalls[name] {
				return true
			}
			for _, arg := range n.Args {
				if isRawEntryID(arg) {
					flag(arg.Pos(), name)
				}
			}
		case *ast.AssignStmt:
			for i, lhs := range n.Lhs {
				sel, ok := lhs.(*ast.SelectorExpr)
				if ok && sel.Sel.Name == "MessageID" && i < len(n.Rhs) && isRawEntryID(n.Rhs[i]) {
					flag(n.Rhs[i].Pos(), "MessageID")
				}
			}
		case *ast.KeyValueExpr:
			if key, ok := n.Key.(*ast.Ident); ok && key.Name == "DedupKey" {
				if raw := rawEntryIDIn(n.Value); raw != nil {
					flag(raw.Pos(), "DedupKey")
				}
			}
		}
		return true
	})
	return found
}

// rawEntryIDIn returns the first msg.ID selector within expr, or nil.
func rawEntryIDIn(expr ast.Expr) ast.Expr {
	var raw ast.Expr
	ast.Inspect(expr, func(n ast.Node) bool {
		if e, ok := n.(ast.Expr); ok && raw == nil && isRawEntryID(e) {
			raw = e
		}
		return raw == nil
	})
	return raw
}

// isRawEntryID reports whether expr is the selector msg.ID.
func isRawEntryID(expr ast.Expr) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "ID" {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == "msg"
}
