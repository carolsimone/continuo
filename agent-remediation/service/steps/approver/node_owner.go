// Package approver holds the steps that work out who must approve what a
// remediation did with each of its nodes. node_owner names each node's owner.
package approver

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
	"github.com/carolsimone/continuo/pkg/domain/model"
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

// NewNodeOwner builds the node_owner step. bundle reads the nodes' owners from
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

// Approvers names one approver per node of in.Nodes, sorted by node id, from
// one read of the code bundle. It runs in the transaction that ends the
// remediation, and a bundle read never fails that end: a bundle that cannot be
// read for good (ports.ErrNotFound) records no owner, and any other read error
// is logged as a warning and read the same way, so each node falls back to the
// owner its contract file declares, else none.
func (s *NodeOwner) Approvers(ctx context.Context, in step.ApproverInput) ([]proposal.Approver, error) {
	nodes := slices.Clone(in.Nodes)
	slices.Sort(nodes)
	nodes = slices.Compact(nodes)
	if len(nodes) == 0 {
		return []proposal.Approver{}, nil
	}
	owners, err := s.bundle.NodeOwners(ctx, in.Trigger.CodeBundleURI, in.Trigger.ReleaseID, nodes)
	if err != nil && !errors.Is(err, ports.ErrNotFound) {
		s.logger.Warn("the code bundle could not be read; each node's owner falls back to its contract file, else none",
			"release", in.Trigger.ReleaseID, "nodes", strings.Join(nodes, ", "), "error", err)
		owners = nil
	}
	out := make([]proposal.Approver, 0, len(nodes))
	for _, id := range nodes {
		if owner := owners[id]; owner != "" {
			out = append(out, proposal.Approver{NodeID: id, Owner: owner, Source: model.ApproverSourceBundle})
			continue
		}
		out = append(out, s.fallback(in.Clusters, id))
	}
	return out, nil
}

// fallback is the approver of a node the code bundle names no owner for: the
// owner its contract file declares, else none.
func (s *NodeOwner) fallback(clusters []step.ClusterDraft, nodeID string) proposal.Approver {
	if owner := s.contractOwner(clusters, nodeID); owner != "" {
		return proposal.Approver{NodeID: nodeID, Owner: owner, Source: model.ApproverSourceContractFile}
	}
	return proposal.Approver{NodeID: nodeID, Source: model.ApproverSourceNone}
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
