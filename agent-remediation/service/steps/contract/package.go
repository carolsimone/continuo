package contract

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// ContractPackages is the check `contract_packages`: the answer, applied to the
// checkout, is a contract the team's packaging tool accepts.
type ContractPackages struct {
	packager ports.ContractPackager
	dialect  string
}

var _ step.Check = (*ContractPackages)(nil)

// NewContractPackages builds the contract_packages step. dialect is the sqlglot
// dialect of the warehouse the install runs, which the tool renders the
// contract's reads under.
func NewContractPackages(packager ports.ContractPackager, dialect string) *ContractPackages {
	return &ContractPackages{packager: packager, dialect: dialect}
}

const nameContractPackages workflow.StepName = "contract_packages"

// Meta describes the step.
func (*ContractPackages) Meta() step.Meta {
	return step.Meta{
		Name: nameContractPackages,
		Kind: step.KindCheck,
		Description: "Packages the contract directory of the patched checkout with the tool the team's own release CI runs, and fails " +
			"a draft the tool refuses. Provides the merged contract a verification run is submitted with.",
		Needs:    needsDraftAndFile,
		Provides: []remediation.EvidenceKey{remediation.KeyPackagedContract},
		Stages:   preflightOnly,
	}
}

// Check packages the contract directory of the patched checkout, resolving
// scripts against the service's own directory, for the failing node's service.
// The order is the whole point: packaging the checkout before the answer is
// written to it would produce, and verify, the contract that already failed.
//
// A contract the tool read and refused is settled, because the tool is
// deterministic on its input: it is a contract_rejected finding keeping the
// tool's complaint. Any other failure is the tool never having answered, and is
// returned so the work is tried again. A draft with a path outside the checkout
// is never applied: it is reported as path_outside_contract_dir, as the path
// check does.
func (c *ContractPackages) Check(ctx context.Context, in step.CheckInput) (step.CheckOutput, error) {
	file, err := contractFileOf(in, nameContractPackages)
	if err != nil {
		return step.CheckOutput{}, err
	}
	root, out, done, err := patchedRoot(ctx, in, nameContractPackages, file)
	if done || err != nil {
		return out, err
	}
	merged, err := c.packager.Merge(ctx,
		filepath.Join(root, filepath.FromSlash(file.ContractDir)),
		filepath.Join(root, filepath.FromSlash(file.RepoRoot)),
		in.Failure.Target.Service, c.dialect)
	if errors.Is(err, ports.ErrContractRejected) {
		return result(nameContractPackages, []remediation.Finding{{
			Code: "contract_rejected",
			Message: fmt.Sprintf(
				"the tool that packages a contract for release refused the model's answer, so no release could be built from it: %v", err),
		}}), nil
	}
	if err != nil {
		return step.CheckOutput{}, fmt.Errorf("package contract for %s: %w", in.Failure.Target.NodeID, err)
	}
	return step.CheckOutput{
		Result:   remediation.CheckResult{Check: string(nameContractPackages)},
		Evidence: map[remediation.EvidenceKey]any{remediation.KeyPackagedContract: remediation.Bytes(merged)},
	}, nil
}
