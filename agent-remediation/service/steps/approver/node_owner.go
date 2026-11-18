// Package approver holds the steps that work out who must approve what a
// remediation did with each of its nodes. node_owner names each node's owner.
package approver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
)

const nameNodeOwner workflow.StepName = "node_owner"

// NodeOwner is the approver step `node_owner`. Each node's approver is its
// owner: the one the release's code bundle records, else the team.name of the
// ODCS contract file a cluster's evidence carries for the node, else none.
type NodeOwner struct {
	bundle    ports.CandidateSourceReader
	inspector ports.ContractInspector
	logger    *slog.Logger
}

var _ step.Approver = (*NodeOwner)(nil)

// NewNodeOwner builds the node_owner step. bundle reads a node's owner from
// the release's code bundle, and inspector reads the team.name of an ODCS
// contract file.
func NewNodeOwner(bundle ports.CandidateSourceReader, inspector ports.ContractInspector, logger *slog.Logger) *NodeOwner {
	return &NodeOwner{bundle: bundle, inspector: inspector, logger: shared.LoggerOrDefault(logger)}
}

// Meta describes the step. It needs no evidence: it reads a cluster's
// contract file when the cluster has one.
func (*NodeOwner) Meta() step.Meta {
	return step.Meta{
		Name: nameNodeOwner,
		Kind: step.KindApprover,
		Description: "Names each node's owner as its approver: the owner the release's code bundle records " +
			"(a dbt node's config meta.owner, a python node's contract team.name), else the team.name of the " +
			"node's ODCS contract file its cluster gathered, else none.",
		Stages: []workflow.Stage{workflow.StageApprover},
	}
}

// Approvers names one approver per node of in.Nodes, sorted by node id. A
// bundle read that fails for another reason than a permanent miss
// (ports.ErrNotFound) is returned.
func (s *NodeOwner) Approvers(ctx context.Context, in step.ApproverInput) ([]proposal.Approver, error) {
	nodes := slices.Clone(in.Nodes)
	slices.Sort(nodes)
	nodes = slices.Compact(nodes)
	out := make([]proposal.Approver, 0, len(nodes))
	for _, id := range nodes {
		a, err := s.approver(ctx, in, id)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, nil
}

// approver is the approver of one node.
func (s *NodeOwner) approver(ctx context.Context, in step.ApproverInput, nodeID string) (proposal.Approver, error) {
	src, err := s.bundle.NodeSource(ctx, in.Trigger.CodeBundleURI, nodeID, in.Trigger.ReleaseID)
	switch {
	case err == nil && src.Owner != "":
		return proposal.Approver{NodeID: nodeID, Owner: src.Owner, Source: proposal.ApproverSourceBundle}, nil
	case err != nil && !errors.Is(err, ports.ErrNotFound):
		return proposal.Approver{}, fmt.Errorf("read the owner of %s from the code bundle: %w", nodeID, err)
	}
	if owner := s.contractOwner(in.Clusters, nodeID); owner != "" {
		return proposal.Approver{NodeID: nodeID, Owner: owner, Source: proposal.ApproverSourceContractFile}, nil
	}
	return proposal.Approver{NodeID: nodeID, Source: proposal.ApproverSourceNone}, nil
}

// contractOwner is the team.name of the first contract file, among the
// clusters' evidence, that declares nodeID: its schema and table are the last
// two dot-separated parts of the node id. A contract that does not parse, or
// names no team, gives no owner.
func (s *NodeOwner) contractOwner(clusters []step.ClusterDraft, nodeID string) string {
	for _, c := range clusters {
		cf, ok := remediation.Get[remediation.ContractFile](c.Evidence, remediation.KeyContractFile)
		if !ok || !declares(cf, nodeID) {
			continue
		}
		decls, err := s.inspector.Declarations(cf.YAMLText)
		if err != nil {
			s.logger.Warn("the contract file of a node does not parse; its owner is not read from it",
				"node", nodeID, "path", cf.YAMLPath, "error", err)
			continue
		}
		for _, d := range decls {
			if d.Identity.Owner != "" {
				return d.Identity.Owner
			}
		}
	}
	return ""
}

// declares reports whether cf is the contract file of nodeID.
func declares(cf remediation.ContractFile, nodeID string) bool {
	if cf.Schema == "" || cf.Table == "" {
		return false
	}
	return nodeID == cf.Schema+"."+cf.Table || strings.HasSuffix(nodeID, "."+cf.Schema+"."+cf.Table)
}
