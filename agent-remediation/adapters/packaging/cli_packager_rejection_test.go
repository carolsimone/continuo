package packaging

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
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

// TestPack_ARejectedContractIsDeterministic pins the classification a caller
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
func TestPack_ARejectedContractIsDeterministic(t *testing.T) {
	p := stubPackager(t, "#!/bin/sh\necho 'contracts/x.odcs.yaml: schema[0].properties is required' >&2\nexit 1\n")

	_, err := p.Pack(context.Background(), t.TempDir(), t.TempDir(), "svc-py", "postgres", map[string]string{"pyarrow": "26.0.0"})
	if err == nil {
		t.Fatal("Pack succeeded although the CLI exited nonzero")
	}
	if !errors.Is(err, ports.ErrContractRejected) {
		t.Errorf("error = %v, want it to wrap ports.ErrContractRejected so the caller records a terminal failure", err)
	}
	if !strings.Contains(err.Error(), "schema[0].properties is required") {
		t.Errorf("error = %v, want it to carry the CLI's stderr as evidence for the next attempt", err)
	}
}

// TestPack_AnAbandonedRunIsTransient is the other half: the CLI that never
// got to answer. A cancelled context kills the subprocess, which surfaces as
// the same nonzero-exit shape as a rejection, so classifying on the exit alone
// would record a permanent failure for a shutdown or a deadline. That must stay
// retryable.
func TestPack_AnAbandonedRunIsTransient(t *testing.T) {
	p := stubPackager(t, "#!/bin/sh\nsleep 30\n")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := p.Pack(ctx, t.TempDir(), t.TempDir(), "svc-py", "postgres", map[string]string{"pyarrow": "26.0.0"})
	if err == nil {
		t.Fatal("Pack succeeded although its context was cancelled")
	}
	if errors.Is(err, ports.ErrContractRejected) {
		t.Errorf("error = %v, want a cancelled run NOT to be reported as the CLI rejecting the contract", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want it to carry the cancellation so the cause is legible", err)
	}
}

// TestPack_ASuccessfulRunReturnsWhatTheCLIWrote is the control: a CLI that
// exits zero has its --out file returned verbatim, and nothing about the
// rejection classification touches that path.
func TestPack_ASuccessfulRunReturnsWhatTheCLIWrote(t *testing.T) {
	p := stubPackager(t, `#!/bin/sh
while [ $# -gt 0 ]; do
  if [ "$1" = "--out" ]; then shift; printf '{"bundle_version": 1}' > "$1"; fi
  shift
done
exit 0
`)

	out, err := p.Pack(context.Background(), t.TempDir(), t.TempDir(), "svc-py", "postgres", map[string]string{"pyarrow": "26.0.0"})
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	if string(out) != `{"bundle_version": 1}` {
		t.Errorf("Pack returned %q, want the bytes the CLI wrote to --out", out)
	}
}

// TestPack_RunsPackWithTheImagePackagesFile pins the command line: pack of the
// contract directory, for the service, under the repository root, with the
// package list written as one JSON object to the file --image-packages names,
// the dialect, and --out an odcs-bundle.json the packager returns.
func TestPack_RunsPackWithTheImagePackagesFile(t *testing.T) {
	record := filepath.Join(t.TempDir(), "argv")
	p := stubPackager(t, `#!/bin/sh
out=""
pk=""
: > "`+record+`"
for a in "$@"; do printf '%s\n' "$a" >> "`+record+`"; done
while [ $# -gt 0 ]; do
  case "$1" in
    --out) shift; out="$1" ;;
    --image-packages) shift; pk="$1" ;;
  esac
  shift
done
# PATH holds only this stub, so the script uses shell builtins alone.
while IFS= read -r line || [ -n "$line" ]; do printf '%s' "$line" >> "`+record+`"; done < "$pk"
printf '\n%s\n' "${out##*/}" >> "`+record+`"
printf '{"bundle_version": 1}' > "$out"
`)

	contractDir, repoRoot := t.TempDir(), t.TempDir()
	out, err := p.Pack(context.Background(), contractDir, repoRoot, "svc-py", "trino",
		map[string]string{"pyarrow": "26.0.0", "duckdb": "1.4.0"})
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	if string(out) != `{"bundle_version": 1}` {
		t.Errorf("Pack returned %q, want the bytes the CLI wrote to --out", out)
	}

	raw, err := os.ReadFile(record) //nolint:gosec // G304: a file of this test's temp dir.
	if err != nil {
		t.Fatalf("read argv: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) != 14 {
		t.Fatalf("argv record = %q, want the arguments, the packages file and the out name", raw)
	}
	argv := lines[:10]
	want := []string{"pack", contractDir, "--service", "svc-py", "--repo-root", repoRoot, "--image-packages", argv[7], "--dialect", "trino"}
	for i := range want {
		if argv[i] != want[i] {
			t.Fatalf("argv = %q, want %q (then --out)", argv, want)
		}
	}
	if lines[10] != "--out" {
		t.Fatalf("argument 11 = %q, want --out", lines[10])
	}
	if got, wantJSON := lines[12], `{"duckdb":"1.4.0","pyarrow":"26.0.0"}`; got != wantJSON {
		t.Errorf("packages file = %q, want %q", got, wantJSON)
	}
	if got := lines[len(lines)-1]; got != "odcs-bundle.json" {
		t.Errorf("--out names %q, want an odcs-bundle.json", got)
	}
}
