package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// requiredWiring lists the constructors main must call for the executor to do
// its job. Each entry is a package-qualified constructor and the reason its
// absence would matter.
//
// This is a structural guard, not a behavioural one: what each collaborator does
// is covered by its own package's tests. What no other test can see is whether
// main ever builds it. A collaborator that is constructed nowhere is dead code
// that still compiles, passes every unit test, and does nothing in production —
// which is exactly how the worker path stayed inert while every part of it was
// tested and green.
var requiredWiring = map[string]string{
	"pool.NewReconciler": "no pool is ever registered, so no worker pod runs and " +
		"tasks routed to workers wait forever",
	"k8s.NewWorkerPools": "the reconciler has no runtime to create pools in",
	"reaper.NewReaper": "an expired lease is never taken back, so its execution " +
		"slot is held forever",
	"lease.NewService": "workers cannot claim tasks",
	"deployer.NewDispatcher": "no Kubernetes Job is ever created, so the jobs " +
		"path stops dispatching",
	"pkgoutbox.NewProcessor": "no outbox row is ever published",
}

// requiredGoroutines lists the run loops main must start. A loop that is built
// but never started is as inert as one that was never built.
var requiredGoroutines = []string{
	"outboxProcessor.Run",
	"deployDispatcher.Run",
	"leaseReaper.Run",
	"poolReconciler.Run",
	"jobTerminalConsumer.Start",
}

// callsIn collects every function call in main.go, rendered as it is written:
// "pkg.Func" for a package-qualified call, "x.Method" for a method call.
func callsIn(t *testing.T) map[string]bool {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "main.go", nil, 0)
	if err != nil {
		t.Fatalf("parse main.go: %v", err)
	}
	calls := map[string]bool{}
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if base, ok := sel.X.(*ast.Ident); ok {
			calls[base.Name+"."+sel.Sel.Name] = true
		}
		return true
	})
	return calls
}

// TestMainWiresItsCollaborators pins that main builds each collaborator the
// executor needs, so removing a construction breaks a test rather than quietly
// turning a subsystem off.
func TestMainWiresItsCollaborators(t *testing.T) {
	calls := callsIn(t)
	for constructor, consequence := range requiredWiring {
		if !calls[constructor] {
			t.Errorf("main.go never calls %s: %s", constructor, consequence)
		}
	}
}

// TestMainStartsItsRunLoops pins that every loop main builds is also started.
func TestMainStartsItsRunLoops(t *testing.T) {
	calls := callsIn(t)
	for _, loop := range requiredGoroutines {
		if !calls[loop] {
			t.Errorf("main.go never starts %s", loop)
		}
	}
}
