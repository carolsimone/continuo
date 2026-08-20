package packaging

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// stubPackager installs script as the continuo-runtime this package resolves,
// and returns a CLIPackager bound to it. The stub carries the executable bit
// because exec.LookPath will not resolve it otherwise, and it lives in a
// per-test temp directory the test framework removes.
func stubPackager(t *testing.T, script string) *CLIPackager {
	t.Helper()
	dir := t.TempDir()
	stub := filepath.Join(dir, runtimeBinary)
	if err := os.WriteFile(stub, []byte(script), 0o700); err != nil { //nolint:gosec // G306: an executable fixture is required, scoped to a temp dir.
		t.Fatalf("write stub %s: %v", stub, err)
	}
	t.Setenv("PATH", dir)

	p, err := NewCLIPackager()
	if err != nil {
		t.Fatalf("NewCLIPackager: %v", err)
	}
	return p
}

// TestMerge_ARejectedContractIsDeterministic pins the classification a caller
// depends on to decide whether to retry.
//
// The CLI exiting nonzero after reading its input means it read the contract
// and refused it. Retrying cannot change that answer — and the model call that
// produced the contract is served from the idempotency cache on a redelivery,
// so the same refused input would be rebuilt every time until the stream's
// poison limit dropped the message and left the attempt in flight forever. The
// error therefore carries ErrContractRejected, which tells the caller to record
// a terminal failure instead, and it carries the CLI's own stderr, which is the
// evidence the next attempt is shown.
func TestMerge_ARejectedContractIsDeterministic(t *testing.T) {
	p := stubPackager(t, "#!/bin/sh\necho 'node analytics.x: output_columns is required' >&2\nexit 1\n")

	_, err := p.Merge(context.Background(), t.TempDir(), t.TempDir(), "svc-py", "postgres")
	if err == nil {
		t.Fatal("Merge succeeded although the CLI exited nonzero")
	}
	if !errors.Is(err, ports.ErrContractRejected) {
		t.Errorf("error = %v, want it to wrap ports.ErrContractRejected so the caller records a terminal failure", err)
	}
	if !strings.Contains(err.Error(), "output_columns is required") {
		t.Errorf("error = %v, want it to carry the CLI's stderr as evidence for the next attempt", err)
	}
}

// TestMerge_AnAbandonedRunIsTransient is the other half: the CLI that never
// got to answer. A cancelled context kills the subprocess, which surfaces as
// the same nonzero-exit shape as a rejection, so classifying on the exit alone
// would record a permanent failure for a shutdown or a deadline. That must stay
// retryable.
func TestMerge_AnAbandonedRunIsTransient(t *testing.T) {
	p := stubPackager(t, "#!/bin/sh\nsleep 30\n")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := p.Merge(ctx, t.TempDir(), t.TempDir(), "svc-py", "postgres")
	if err == nil {
		t.Fatal("Merge succeeded although its context was cancelled")
	}
	if errors.Is(err, ports.ErrContractRejected) {
		t.Errorf("error = %v, want a cancelled run NOT to be reported as the CLI rejecting the contract", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want it to carry the cancellation so the cause is legible", err)
	}
}

// TestMerge_ASuccessfulRunReturnsWhatTheCLIWrote is the control: a CLI that
// exits zero has its --out file returned verbatim, and nothing about the
// rejection classification touches that path.
func TestMerge_ASuccessfulRunReturnsWhatTheCLIWrote(t *testing.T) {
	p := stubPackager(t, `#!/bin/sh
while [ $# -gt 0 ]; do
  if [ "$1" = "--out" ]; then shift; printf 'contract_version: 1\n' > "$1"; fi
  shift
done
exit 0
`)

	out, err := p.Merge(context.Background(), t.TempDir(), t.TempDir(), "svc-py", "postgres")
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if string(out) != "contract_version: 1\n" {
		t.Errorf("Merge returned %q, want the bytes the CLI wrote to --out", out)
	}
}
