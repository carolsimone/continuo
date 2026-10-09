package handlers_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// Lock-ordering invariant guarded here:
//
// A release-controller transaction that BOTH takes the release-queue advisory
// lock (UnitOfWork.LockReleaseQueue, pg_advisory_xact_lock) AND acquires a
// run row FOR UPDATE (RunRepo().Load) must take the advisory lock FIRST. The
// advisory lock and the run-row lock are the two locks every writer can hold
// at once; taking them in a single global order (advisory → run-row) keeps the
// cross-process wait-for graph acyclic, so two transactions can never deadlock
// by each holding the lock the other waits on. A transaction that takes the
// run row first and the advisory lock later reverses that order and is the one
// cycle that can close.
//
// The advisory lock is re-entrant within one transaction (pg_advisory_xact_lock
// taken twice in the same tx simply succeeds), so a handler that acquires it
// early and a callee (promoteToProduction) that acquires it again both hold the
// same global order: the check below forbids only a run-row Load that happens
// before the FIRST advisory acquisition, not a later re-acquisition.

type lockEventKind int

const (
	eventLoad lockEventKind = iota // u.RunRepo().Load(...) — a run row FOR UPDATE
	eventLock                      // u.LockReleaseQueue(...) — the release-queue advisory lock
)

// lockEvent is one ordered occurrence inside a function body: either a run-row
// Load, a release-queue lock, or a call to another function in this package
// whose own events are inlined in document order to recover the real
// cross-function acquisition order.
type lockEvent struct {
	kind lockEventKind
	call string // non-empty => a call to a package-local function to inline
}

// collectFuncEvents parses every non-test .go file in the handlers package and
// returns, per function name, its ordered lock events (Load, Lock, or a
// package-local call to inline). It also reports whether any Load and any Lock
// were seen at all, so the guard can fail loudly if the method names it keys on
// were renamed out from under it.
func collectFuncEvents(t *testing.T) (events map[string][]lockEvent, sawLoad, sawLock bool) {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve the handlers package directory")
	}
	dir := filepath.Dir(thisFile)

	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse handlers package: %v", err)
	}

	events = map[string][]lockEvent{}
	// Pass 1: the set of function names declared in this package, so a call
	// expression can be recognised as a local call worth inlining.
	local := map[string]bool{}
	for _, pkg := range pkgs {
		for _, f := range pkg.Files {
			for _, decl := range f.Decls {
				if fn, ok := decl.(*ast.FuncDecl); ok {
					local[fn.Name.Name] = true
				}
			}
		}
	}

	// Pass 2: the ordered events of each function body.
	for _, pkg := range pkgs {
		for _, f := range pkg.Files {
			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				var seq []lockEvent
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					switch fun := call.Fun.(type) {
					case *ast.SelectorExpr:
						switch fun.Sel.Name {
						case "LockReleaseQueue":
							seq = append(seq, lockEvent{kind: eventLock})
							sawLock = true
						case "Load":
							// Match only <uow>.RunRepo().Load(...): the receiver of
							// .Load must itself be a .RunRepo() call. Get/Save/Active
							// on RunRepo, and Load on any other repo, are not run-row
							// FOR UPDATE and must not count.
							if inner, ok := fun.X.(*ast.CallExpr); ok {
								if innerSel, ok := inner.Fun.(*ast.SelectorExpr); ok && innerSel.Sel.Name == "RunRepo" {
									seq = append(seq, lockEvent{kind: eventLoad})
									sawLoad = true
								}
							}
						}
					case *ast.Ident:
						if local[fun.Name] {
							seq = append(seq, lockEvent{call: fun.Name})
						}
					}
					return true
				})
				events[fn.Name.Name] = seq
			}
		}
	}
	return events, sawLoad, sawLock
}

// flattenEvents resolves a function's events into a flat ordered slice of
// Load/Lock kinds, inlining package-local calls in document order. A cycle in
// the call graph is broken by skipping a function already being expanded.
func flattenEvents(name string, events map[string][]lockEvent, inProgress map[string]bool) []lockEventKind {
	if inProgress[name] {
		return nil
	}
	inProgress[name] = true
	defer delete(inProgress, name)

	var out []lockEventKind
	for _, e := range events[name] {
		if e.call != "" {
			out = append(out, flattenEvents(e.call, events, inProgress)...)
			continue
		}
		out = append(out, e.kind)
	}
	return out
}

// TestLockReleaseQueueIsTakenBeforeAnyRunLoad fails if any function in the
// handlers package acquires a run row FOR UPDATE (RunRepo().Load) before it
// takes the release-queue advisory lock within the same transaction, inlining
// package-local calls so a Load in a handler followed by a LockReleaseQueue in
// a callee (e.g. promoteToProduction) is caught across the function boundary.
//
// It fails on the inverted shape — Load, then a later LockReleaseQueue — and
// passes once the advisory lock is taken first. A re-acquisition of the
// advisory lock after the Load is fine: only a Load before the FIRST advisory
// acquisition is a violation.
func TestLockReleaseQueueIsTakenBeforeAnyRunLoad(t *testing.T) {
	events, sawLoad, sawLock := collectFuncEvents(t)

	if !sawLoad {
		t.Fatal("guard found no RunRepo().Load call anywhere in the handlers package; " +
			"the FOR-UPDATE accessor was likely renamed — update this guard to match")
	}
	if !sawLock {
		t.Fatal("guard found no LockReleaseQueue call anywhere in the handlers package; " +
			"the advisory-lock accessor was likely renamed — update this guard to match")
	}

	for name := range events {
		seq := flattenEvents(name, events, map[string]bool{})
		firstLock, firstLoad := -1, -1
		for i, k := range seq {
			if k == eventLock && firstLock < 0 {
				firstLock = i
			}
			if k == eventLoad && firstLoad < 0 {
				firstLoad = i
			}
		}
		// Only a transaction that takes BOTH locks is constrained. When it does,
		// the first advisory acquisition must precede the first run-row Load.
		if firstLock >= 0 && firstLoad >= 0 && firstLoad < firstLock {
			t.Errorf("%s acquires a run row FOR UPDATE (RunRepo().Load) before it takes the "+
				"release-queue advisory lock (LockReleaseQueue); take LockReleaseQueue first so "+
				"the global advisory→run-row lock order holds and the wait-for graph stays acyclic", name)
		}
	}
}
