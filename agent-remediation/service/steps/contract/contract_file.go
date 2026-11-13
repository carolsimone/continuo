package contract

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
)

// ContractFile is the context source `contract_file`: the contract yaml that
// declares the failing python node.
type ContractFile struct {
	archive ports.RepoArchive
	locator ports.ContractLocator
	logger  *slog.Logger
}

var _ step.ContextSource = (*ContractFile)(nil)

// NewContractFile builds the contract_file step. It calls no port until Gather.
func NewContractFile(archive ports.RepoArchive, locator ports.ContractLocator, logger *slog.Logger) *ContractFile {
	return &ContractFile{archive: archive, locator: locator, logger: shared.LoggerOrDefault(logger)}
}

// Meta describes the step.
func (*ContractFile) Meta() step.Meta {
	return step.Meta{
		Name: "contract_file",
		Kind: step.KindContext,
		Description: "Finds the contract yaml that declares the failing python node in the repository at the trigger's commit and keeps " +
			"its repository-relative paths and its text. Declines a node id that names no schema and table, a trigger with no service, " +
			"a repository that is not available, and a node that no contract file declares or several do.",
		Needs:    []remediation.EvidenceKey{remediation.KeyFailure},
		Provides: []remediation.EvidenceKey{remediation.KeyContractFile},
	}
}

// Gather splits the node id into the schema and table the runtime matches a
// node by, fetches the repository at the failing commit, locates the contract
// file declaring the node and removes the checkout again: the evidence holds
// the paths and the yaml text, never a directory. RepoRoot is the
// repository-relative application root of the node, not a path on disk.
//
// A node id that names no schema and table, a trigger that names no service
// (the fix would have no release to be verified under), a repository or commit
// that does not exist, and a node that no contract file declares or that
// several declare are settled facts, and each declines the cluster with the
// reason as its text. Any other fetch or locate failure is transient and is
// returned so the work is tried again.
func (s *ContractFile) Gather(ctx context.Context, in step.ContextInput) (step.ContextOutput, error) {
	f := in.Failure
	nodeID := f.Target.NodeID
	schema, table, ok := splitNodeID(nodeID)
	if !ok {
		return s.decline(f, fmt.Sprintf(
			"node id %q does not name a schema and a table, so the contract file declaring it cannot be found", nodeID)), nil
	}
	if f.Target.Service == "" {
		return s.decline(f, "the trigger names no service, so the fix has no release to be verified under"), nil
	}

	root, cleanup, reason, err := fetchCheckout(ctx, s.archive, f)
	if err != nil {
		return step.ContextOutput{}, err
	}
	if reason != "" {
		return s.decline(f, reason), nil
	}
	defer cleanup()

	// A python node's declaring yaml path is recorded nowhere in the control
	// plane, so the whole tree is searched.
	located, err := s.locator.Locate(root, schema, table)
	if errors.Is(err, ports.ErrNodeNotDeclared) || errors.Is(err, ports.ErrAmbiguousDeclaration) {
		return s.decline(f, err.Error()), nil
	}
	if err != nil {
		return step.ContextOutput{}, fmt.Errorf("locate contract for %s: %w", nodeID, err)
	}
	return step.ContextOutput{Evidence: map[remediation.EvidenceKey]any{
		remediation.KeyContractFile: remediation.ContractFile{
			Schema:      schema,
			Table:       table,
			RepoRoot:    located.RepoRoot,
			ContractDir: located.ContractDir,
			YAMLPath:    located.YAMLPath,
			YAMLText:    located.YAMLText,
		},
	}}, nil
}

func (s *ContractFile) decline(f remediation.Failure, reason string) step.ContextOutput {
	return declined(s.logger, f, reason)
}

// fetchCheckout fetches the repository at the failure's commit. The caller
// calls cleanup when it is done with root, which is set only when reason and
// err are empty. A repository or commit that does not exist is permanent, and
// is returned as the decline reason; any other failure is transient and is
// returned as err.
func fetchCheckout(ctx context.Context, archive ports.RepoArchive, f remediation.Failure) (root string, cleanup func(), reason string, err error) {
	repo, ref := f.Trigger.Repo, f.Trigger.CommitSHA
	root, cleanup, err = archive.Fetch(ctx, repo, ref)
	if errors.Is(err, ports.ErrSourceNotFound) {
		return "", nil, fmt.Sprintf("repository %s at commit %s is not available", repo, ref), nil
	}
	if err != nil {
		return "", nil, "", fmt.Errorf("fetch repo %s@%s: %w", repo, ref, err)
	}
	return root, cleanup, "", nil
}

// declined is the output of a step that declines the cluster with a reason.
func declined(logger *slog.Logger, f remediation.Failure, reason string) step.ContextOutput {
	logger.Info("python contract fix declined", "node", f.Target.NodeID, "node_type", f.Target.NodeType, "reason", reason)
	return step.ContextOutput{Declined: true, Decline: reason}
}

// splitNodeID reads the schema and table out of a node id. A python node's
// identity is "<schema>.<table>", and the trailing two dot-separated segments
// are what the runtime matches a node by, so they are what the contract search
// is given. ok is false for an id with no dot, from which no node can be
// addressed.
func splitNodeID(nodeID string) (schema, table string, ok bool) {
	parts := strings.Split(nodeID, ".")
	if len(parts) < 2 {
		return "", "", false
	}
	schema, table = parts[len(parts)-2], parts[len(parts)-1]
	if schema == "" || table == "" {
		return "", "", false
	}
	return schema, table, true
}
