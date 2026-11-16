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
	images   ports.ImagePackagesSource
	dialect  string
}

var _ step.Check = (*ContractPackages)(nil)

// NewContractPackages builds the contract_packages step. images reads the
// package list when the evidence holds none; dialect is the sqlglot dialect of
// the warehouse the install runs, which the tool checks the contract's reads
// under.
func NewContractPackages(packager ports.ContractPackager, images ports.ImagePackagesSource, dialect string) *ContractPackages {
	return &ContractPackages{packager: packager, images: images, dialect: dialect}
}

const nameContractPackages workflow.StepName = "contract_packages"

// Meta describes the step.
func (*ContractPackages) Meta() step.Meta {
	return step.Meta{
		Name: nameContractPackages,
		Kind: step.KindCheck,
		Description: "Packs the contract directory of the patched checkout into an odcs-bundle.json with the tool the team's own release CI runs, " +
			"recording the package list of the rejected release's image of the service that image_packages read, and fails a draft the tool refuses. " +
			"Provides the bundle a verification run is submitted with.",
		Needs:    []remediation.EvidenceKey{remediation.KeyDraft, remediation.KeyContractFile, remediation.KeyImagePackages},
		Provides: []remediation.EvidenceKey{remediation.KeyPackagedContract},
		Stages:   preflightOnly,
	}
}

// Check packs the contract directory of the patched checkout, resolving
// scripts against the service's own directory, for the failing node's service,
// with the package list of the rejected release's image of that service, which
// the image_packages context step read once for the cluster. A cluster whose
// context steps ran before image_packages was one of its type's steps has no
// list in its evidence, so the check reads it itself; a release that records
// none for the service is the check's error, saying why. The order is the
// whole point: packing the checkout before the answer is written to it would
// produce, and verify, the contract that already failed.
//
// A contract the tool read and refused is settled, because the tool is
// deterministic on its input: it is a contract_rejected finding keeping the
// tool's complaint. Any other failure is the tool never having answered, and
// is returned so the work is tried again. A draft with a path outside the
// checkout is never applied: it is reported as path_outside_contract_dir, as
// the path check does.
func (c *ContractPackages) Check(ctx context.Context, in step.CheckInput) (step.CheckOutput, error) {
	file, err := contractFileOf(in, nameContractPackages)
	if err != nil {
		return step.CheckOutput{}, err
	}
	root, out, done, err := patchedRoot(ctx, in, nameContractPackages, file)
	if done || err != nil {
		return out, err
	}
	f := in.Failure
	packages, ok := remediation.Get[remediation.ImagePackages](in.Evidence, remediation.KeyImagePackages)
	if !ok {
		read, err := c.images.ImagePackages(ctx, f.Trigger.ReleaseID, f.Target.Service)
		if err != nil {
			return step.CheckOutput{}, fmt.Errorf("%s: read the image packages to pack %s with: %w", nameContractPackages, f.Target.NodeID, err)
		}
		packages = remediation.ImagePackages(read)
	}
	bundle, err := c.packager.Pack(ctx,
		filepath.Join(root, filepath.FromSlash(file.ContractDir)),
		filepath.Join(root, filepath.FromSlash(file.RepoRoot)),
		f.Target.Service, c.dialect, packages)
	if errors.Is(err, ports.ErrContractRejected) {
		return result(nameContractPackages, []remediation.Finding{{
			Code: "contract_rejected",
			Message: fmt.Sprintf(
				"the tool that packages a contract for release refused the model's answer, so no release could be built from it: %v", err),
		}}), nil
	}
	if err != nil {
		return step.CheckOutput{}, fmt.Errorf("package contract for %s: %w", f.Target.NodeID, err)
	}
	return step.CheckOutput{
		Result:   remediation.CheckResult{Check: string(nameContractPackages)},
		Evidence: map[remediation.EvidenceKey]any{remediation.KeyPackagedContract: remediation.Bytes(bundle)},
	}, nil
}
