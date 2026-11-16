// Package packaging implements ports.ContractPackager as a thin subprocess
// wrapper over the continuo-runtime CLI installed in the service image.
package packaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// CLIPackager runs `continuo-runtime pack`, the command the team's CI runs to
// pack a release's contracts, so a proposed fix's ODCS contracts are packed by
// the same tool that packs every release, never a reimplementation of it.
type CLIPackager struct {
	binPath string
}

var _ ports.ContractPackager = (*CLIPackager)(nil)

// runtimeBinary is the CLI this packager shells out to. It is installed in the
// agent-remediation image; nothing else provides it.
const runtimeBinary = "continuo-runtime"

// NewCLIPackager resolves runtimeBinary on PATH and fails if it is absent, so
// an image built without it refuses to start. Resolving it here rather than at
// call time is what turns a mis-built image into one boot failure naming the
// missing binary, instead of every python remediation trigger failing, never
// acking, and redelivering without end. Pack then runs the resolved path, so
// the binary that was checked is the binary that runs.
func NewCLIPackager() (*CLIPackager, error) {
	binPath, err := exec.LookPath(runtimeBinary)
	if err != nil {
		return nil, fmt.Errorf("resolve %s (installed in the agent-remediation image; a python-node fix cannot be packaged without it): %w", runtimeBinary, err)
	}
	return &CLIPackager{binPath: binPath}, nil
}

// Pack runs:
//
//	continuo-runtime pack <contractDir> --service <service> \
//	  --repo-root <repoRoot> --image-packages <tmp>/image-packages.json \
//	  --dialect <dialect> --out <tmp>/odcs-bundle.json
//
// in a fresh temporary directory it removes before returning: imagePackages is
// written there as one JSON object of package name to version, and the bytes
// the CLI wrote to --out are returned.
func (p *CLIPackager) Pack(ctx context.Context, contractDir, repoRoot, service, dialect string, imagePackages map[string]string) ([]byte, error) {
	tmpDir, err := os.MkdirTemp("", "contract-pack-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir for the odcs bundle: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	packages, err := json.Marshal(imagePackages)
	if err != nil {
		return nil, fmt.Errorf("encode the image package list: %w", err)
	}
	packagesPath := filepath.Join(tmpDir, "image-packages.json")
	if err := os.WriteFile(packagesPath, packages, 0o600); err != nil {
		return nil, fmt.Errorf("write the image package list: %w", err)
	}
	outPath := filepath.Join(tmpDir, ports.ODCSBundleName)
	// G204: exec.CommandContext runs the path resolved at construction directly
	// (no shell), so none of contractDir/service/repoRoot/dialect can be
	// interpreted as shell metacharacters. They land as literal argv entries
	// under a fixed, hardcoded flag set the caller cannot alter.
	cmd := exec.CommandContext(ctx, p.binPath, "pack", contractDir, //nolint:gosec // G204: direct execve, no shell, hardcoded flags.
		"--service", service,
		"--repo-root", repoRoot,
		"--image-packages", packagesPath,
		"--dialect", dialect,
		"--out", outPath,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, classifyRunError(ctx, contractDir, err, stderr.String())
	}

	bundle, err := os.ReadFile(outPath) //nolint:gosec // G304: outPath is built from a temp dir this function just created, not external input.
	if err != nil {
		return nil, fmt.Errorf("read the odcs bundle %s: %w", outPath, err)
	}
	return bundle, nil
}

// classifyRunError decides whether a failed pack run is the CLI answering
// "no" or the CLI never getting to answer, and returns an error the caller can
// tell apart with errors.Is.
//
// A normal nonzero exit status is the CLI having read the contract and refused
// it: deterministic on that input, so it is wrapped as ports.ErrContractRejected
// and carries the CLI's stderr, which is both the explanation and the evidence
// the next attempt is shown. Everything else is left plain and stays retryable:
// a cancelled or expired context (which kills the subprocess and surfaces as
// the same nonzero-exit shape, so it is checked first), a process killed by a
// signal (no exit status of its own — ExitCode reports -1), and every failure
// to start the binary at all.
func classifyRunError(ctx context.Context, contractDir string, err error, stderr string) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("continuo-runtime pack %s: %w: %s", contractDir, ctxErr, stderr)
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() >= 0 {
		return fmt.Errorf("continuo-runtime pack %s: exit status %d: %s: %w",
			contractDir, exitErr.ExitCode(), strings.TrimSpace(stderr), ports.ErrContractRejected)
	}
	return fmt.Errorf("continuo-runtime pack %s: %w: %s", contractDir, err, stderr)
}
