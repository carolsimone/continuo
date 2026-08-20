package packaging

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestNewCLIPackager_RefusesWhenTheCLIIsMissing pins the fail-closed rule: an
// image built without continuo-runtime must refuse to start. Resolving the
// binary lazily instead would turn a broken image into a per-message failure —
// every python remediation trigger failing, never acking, and redelivering
// forever, with the cause named once per redelivery far from the install that
// caused it.
func TestNewCLIPackager_RefusesWhenTheCLIIsMissing(t *testing.T) {
	t.Setenv("PATH", t.TempDir()) // an empty directory resolves nothing

	p, err := NewCLIPackager()
	if err == nil {
		t.Fatal("NewCLIPackager succeeded with no continuo-runtime on PATH; a mis-built image must refuse to boot")
	}
	if p != nil {
		t.Fatalf("NewCLIPackager returned a usable packager alongside its error: %+v", p)
	}
	if !errors.Is(err, exec.ErrNotFound) {
		t.Errorf("error = %v, want it to wrap exec.ErrNotFound so the cause is legible in the boot log", err)
	}
	if !strings.Contains(err.Error(), runtimeBinary) {
		t.Errorf("error = %v, want it to name %q", err, runtimeBinary)
	}
}

// TestNewCLIPackager_ResolvesTheCLIOnPATH is the control: with the binary
// present the constructor succeeds, and the packager runs the very path it
// resolved rather than re-resolving PATH per call.
func TestNewCLIPackager_ResolvesTheCLIOnPATH(t *testing.T) {
	dir := t.TempDir()
	stub := filepath.Join(dir, runtimeBinary)
	// The stub must carry the executable bit or exec.LookPath will not resolve
	// it, which is the whole point of the fixture; it lives in a per-test temp
	// directory the test framework removes.
	if err := os.WriteFile(stub, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil { //nolint:gosec // G306: an executable fixture is required, scoped to a temp dir.
		t.Fatalf("write stub %s: %v", stub, err)
	}
	t.Setenv("PATH", dir)

	p, err := NewCLIPackager()
	if err != nil {
		t.Fatalf("NewCLIPackager: %v", err)
	}
	if p.binPath != stub {
		t.Errorf("binPath = %q, want the resolved stub %q", p.binPath, stub)
	}
}
