// Package packaging implements ports.ContractPackager as a thin subprocess
// wrapper over the continuo-runtime CLI installed in the service image.
package packaging

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// CLIPackager runs `continuo-runtime merge` — the exact command the team's
// release CI runs after a merge to main (continuo-dbt-demo's release.yml) —
// so a proposed fix's contract yaml is packaged and hash-folded by the same
// tool that packages every promoted release, never a reimplementation of it.
type CLIPackager struct{}

var _ ports.ContractPackager = (*CLIPackager)(nil)

// NewCLIPackager builds a CLIPackager. It has no dependencies: the
// continuo-runtime binary it shells out to is resolved from PATH.
func NewCLIPackager() *CLIPackager {
	return &CLIPackager{}
}

// Merge runs:
//
//	continuo-runtime merge <contractDir> --service <service> \
//	  --repo-root <repoRoot> --dialect <dialect> --out <tmp>/contract.yaml
//
// into a fresh temporary directory it removes before returning, and returns
// the bytes the CLI wrote to --out. Hashing is a side effect of the CLI's
// merge step, so no separate hash call is made.
func (p *CLIPackager) Merge(ctx context.Context, contractDir, repoRoot, service, dialect string) ([]byte, error) {
	tmpDir, err := os.MkdirTemp("", "contract-merge-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir for merged contract: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	outPath := filepath.Join(tmpDir, "contract.yaml")
	// G204: exec.CommandContext runs continuo-runtime directly (no shell), so
	// none of contractDir/service/repoRoot/dialect can be interpreted as
	// shell metacharacters. They land as literal argv entries under a fixed,
	// hardcoded flag set the caller cannot alter.
	cmd := exec.CommandContext(ctx, "continuo-runtime", "merge", contractDir, //nolint:gosec // G204: direct execve, no shell, hardcoded flags.
		"--service", service,
		"--repo-root", repoRoot,
		"--dialect", dialect,
		"--out", outPath,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("continuo-runtime merge %s: %w: %s", contractDir, err, stderr.String())
	}

	merged, err := os.ReadFile(outPath) //nolint:gosec // G304: outPath is built from a temp dir this function just created, not external input.
	if err != nil {
		return nil, fmt.Errorf("read merged contract %s: %w", outPath, err)
	}
	return merged, nil
}
