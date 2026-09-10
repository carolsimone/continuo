// Package serialization holds the wire-facing DTOs for orchestrator's
// json-tagged domain types, keeping the domain packages (domain, domain/event,
// domain/model) free of struct tags. It sits outside adapters/ so the
// application layer (service/handlers, which both produces query.model:v1
// payloads and re-marshals consumed events as dedup fingerprints) may map
// through it without importing an adapter, and outside domain/ so the tags live
// away from the domain types. The redis parsers (consume side) and the publisher
// (query.model produce side) map through these DTOs, fixing the byte shapes here.
package serialization

import (
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/domain/event"
	"github.com/carolsimone/continuo/orchestrator/domain/model"
)

// NodeReadyForExecutionDTO is the JSON shape of the query.model:v1 payload.
type NodeReadyForExecutionDTO struct {
	ScheduleID      string `json:"schedule_id"`
	ScheduleName    string `json:"schedule_name"`
	ServiceName     string `json:"service_name"`
	SchemaName      string `json:"schema_name"`
	TableName       string `json:"table_name"`
	TaskID          string `json:"task_id"`
	JobName         string `json:"job_name"`
	NodeType        string `json:"node_type"`
	ManifestVersion string `json:"manifest_version"`
	ImageTag        string `json:"image_tag"`
	Operation       string `json:"operation,omitempty"`
}

// NodeReadyForExecutionFromDomain maps a domain event to its DTO.
func NodeReadyForExecutionFromDomain(e domain.NodeReadyForExecution) NodeReadyForExecutionDTO {
	return NodeReadyForExecutionDTO{
		ScheduleID:      e.ScheduleID,
		ScheduleName:    e.ScheduleName,
		ServiceName:     e.ServiceName,
		SchemaName:      e.SchemaName,
		TableName:       e.TableName,
		TaskID:          e.TaskID,
		JobName:         e.JobName,
		NodeType:        e.NodeType,
		ManifestVersion: e.ManifestVersion,
		ImageTag:        e.ImageTag,
		Operation:       e.Operation,
	}
}

// ToDomain maps a decoded DTO back to the domain event.
func (d NodeReadyForExecutionDTO) ToDomain() domain.NodeReadyForExecution {
	return domain.NodeReadyForExecution{
		ScheduleID:      d.ScheduleID,
		ScheduleName:    d.ScheduleName,
		ServiceName:     d.ServiceName,
		SchemaName:      d.SchemaName,
		TableName:       d.TableName,
		TaskID:          d.TaskID,
		JobName:         d.JobName,
		NodeType:        d.NodeType,
		ManifestVersion: d.ManifestVersion,
		ImageTag:        d.ImageTag,
		Operation:       d.Operation,
	}
}

// ReleasePromotedNodeDTO is the JSON shape of one node in a release.promoted:v1
// topology array.
type ReleasePromotedNodeDTO struct {
	UniqueID          string   `json:"unique_id"`
	SchemaName        string   `json:"schema_name"`
	TableName         string   `json:"table_name"`
	ServiceName       string   `json:"service_name"`
	NodeType          string   `json:"node_type"`
	ContentHash       string   `json:"content_hash"`
	TestCount         int      `json:"test_count"`
	ImageTag          string   `json:"image_tag"`
	Schedule          string   `json:"schedule"`
	UpstreamUniqueIDs []string `json:"upstream_unique_ids"`
	Changed           bool     `json:"changed"`
	OriginalFilePath  string   `json:"original_file_path"`
}

// ReleasePromotedDTO is the JSON shape of the release.promoted:v1 payload.
type ReleasePromotedDTO struct {
	ReleaseID     string                   `json:"release_id"`
	Topology      []ReleasePromotedNodeDTO `json:"topology"`
	ImageTags     map[string]string        `json:"image_tags"`
	Repo          string                   `json:"repo"`
	CommitSHA     string                   `json:"commit_sha"`
	PromotedAt    time.Time                `json:"promoted_at"`
	CodeBundleURI string                   `json:"code_bundle_uri"`
	Bootstrap     bool                     `json:"bootstrap"`
}

// ReleasePromotedToDomain maps a decoded DTO back to the domain event.
func (d ReleasePromotedDTO) ToDomain() event.ReleasePromoted {
	var topo []event.ReleasePromotedNode
	if d.Topology != nil {
		topo = make([]event.ReleasePromotedNode, len(d.Topology))
		for i, n := range d.Topology {
			topo[i] = event.ReleasePromotedNode{
				UniqueID:          n.UniqueID,
				SchemaName:        n.SchemaName,
				TableName:         n.TableName,
				ServiceName:       n.ServiceName,
				NodeType:          n.NodeType,
				ContentHash:       n.ContentHash,
				TestCount:         n.TestCount,
				ImageTag:          n.ImageTag,
				Schedule:          n.Schedule,
				UpstreamUniqueIDs: n.UpstreamUniqueIDs,
				Changed:           n.Changed,
				OriginalFilePath:  n.OriginalFilePath,
			}
		}
	}
	return event.ReleasePromoted{
		ReleaseID:     d.ReleaseID,
		Topology:      topo,
		ImageTags:     d.ImageTags,
		Repo:          d.Repo,
		CommitSHA:     d.CommitSHA,
		PromotedAt:    d.PromotedAt,
		CodeBundleURI: d.CodeBundleURI,
		Bootstrap:     d.Bootstrap,
	}
}

// RemediationRequestedNodeDTO is the JSON shape of one classified failure.
type RemediationRequestedNodeDTO struct {
	NodeID         string `json:"node_id"`
	Category       string `json:"category"`
	ErrorSignature string `json:"error_signature"`
	Reason         string `json:"reason"`
	ErrorExcerpt   string `json:"error_excerpt"`
	DBTLogURI      string `json:"dbt_log_uri"`
}

// RemediationRequestedDTO is the JSON shape of the remediation.requested:v2 payload.
type RemediationRequestedDTO struct {
	EventID       string                        `json:"event_id"`
	Source        string                        `json:"source"`
	ReleaseID     string                        `json:"release_id"`
	CodeBundleURI string                        `json:"code_bundle_uri"`
	ClassifiedAt  string                        `json:"classified_at"`
	Nodes         []RemediationRequestedNodeDTO `json:"nodes"`
}

// RemediationRequestedFromDomain maps a domain event to its DTO.
func RemediationRequestedFromDomain(e event.RemediationRequested) RemediationRequestedDTO {
	var nodes []RemediationRequestedNodeDTO
	if e.Nodes != nil {
		nodes = make([]RemediationRequestedNodeDTO, len(e.Nodes))
		for i, n := range e.Nodes {
			nodes[i] = RemediationRequestedNodeDTO(n)
		}
	}
	return RemediationRequestedDTO{
		EventID:       e.EventID,
		Source:        e.Source,
		ReleaseID:     e.ReleaseID,
		CodeBundleURI: e.CodeBundleURI,
		ClassifiedAt:  e.ClassifiedAt,
		Nodes:         nodes,
	}
}

// ToDomain maps a decoded DTO back to the domain event.
func (d RemediationRequestedDTO) ToDomain() event.RemediationRequested {
	var nodes []event.RemediationRequestedNode
	if d.Nodes != nil {
		nodes = make([]event.RemediationRequestedNode, len(d.Nodes))
		for i, n := range d.Nodes {
			nodes[i] = event.RemediationRequestedNode(n)
		}
	}
	return event.RemediationRequested{
		EventID:       d.EventID,
		Source:        d.Source,
		ReleaseID:     d.ReleaseID,
		CodeBundleURI: d.CodeBundleURI,
		ClassifiedAt:  d.ClassifiedAt,
		Nodes:         nodes,
	}
}

// PROpenedDTO is the JSON shape of the remediation.pr_opened:v1 payload.
type PROpenedDTO struct {
	ProposalID      string   `json:"proposal_id"`
	ReleaseID       string   `json:"release_id"`
	NodeID          string   `json:"node_id"`
	ResolvedNodeIDs []string `json:"resolved_node_ids"`
	PrURL           string   `json:"pr_url"`
	PrNumber        int      `json:"pr_number"`
	OpenedBy        string   `json:"opened_by"`
	OpenedAt        string   `json:"opened_at"`
	Service         string   `json:"service"`
}

// PROpenedFromDomain maps a domain event to its DTO.
func PROpenedFromDomain(e event.PROpened) PROpenedDTO {
	return PROpenedDTO{
		ProposalID:      e.ProposalID,
		ReleaseID:       e.ReleaseID,
		NodeID:          e.NodeID,
		ResolvedNodeIDs: e.ResolvedNodeIDs,
		PrURL:           e.PrURL,
		PrNumber:        e.PrNumber,
		OpenedBy:        e.OpenedBy,
		OpenedAt:        e.OpenedAt,
		Service:         e.Service,
	}
}

// ToDomain maps a decoded DTO back to the domain event.
func (d PROpenedDTO) ToDomain() event.PROpened {
	return event.PROpened{
		ProposalID:      d.ProposalID,
		ReleaseID:       d.ReleaseID,
		NodeID:          d.NodeID,
		ResolvedNodeIDs: d.ResolvedNodeIDs,
		PrURL:           d.PrURL,
		PrNumber:        d.PrNumber,
		OpenedBy:        d.OpenedBy,
		OpenedAt:        d.OpenedAt,
		Service:         d.Service,
	}
}

// PRClosedEditDTO is the JSON shape of one file edit on a closed PR.
type PRClosedEditDTO struct {
	Path         string `json:"path"`
	TargetNodeID string `json:"target_node_id"`
	Amended      bool   `json:"amended"`
	Diff         string `json:"diff,omitempty"`
}

// PRClosedDTO is the JSON shape of the remediation.pr_closed:v1 payload.
type PRClosedDTO struct {
	ProposalID      string            `json:"proposal_id"`
	ReleaseID       string            `json:"release_id"`
	NodeID          string            `json:"node_id"`
	ResolvedNodeIDs []string          `json:"resolved_node_ids"`
	Service         string            `json:"service,omitempty"`
	PrURL           string            `json:"pr_url"`
	PrNumber        int               `json:"pr_number"`
	Outcome         string            `json:"outcome"`
	ClosedAt        string            `json:"closed_at"`
	Edits           []PRClosedEditDTO `json:"edits,omitempty"`
}

// PRClosedFromDomain maps a domain event to its DTO.
func PRClosedFromDomain(e event.PRClosed) PRClosedDTO {
	var edits []PRClosedEditDTO
	if e.Edits != nil {
		edits = make([]PRClosedEditDTO, len(e.Edits))
		for i, ed := range e.Edits {
			edits[i] = PRClosedEditDTO(ed)
		}
	}
	return PRClosedDTO{
		ProposalID:      e.ProposalID,
		ReleaseID:       e.ReleaseID,
		NodeID:          e.NodeID,
		ResolvedNodeIDs: e.ResolvedNodeIDs,
		Service:         e.Service,
		PrURL:           e.PrURL,
		PrNumber:        e.PrNumber,
		Outcome:         e.Outcome,
		ClosedAt:        e.ClosedAt,
		Edits:           edits,
	}
}

// ToDomain maps a decoded DTO back to the domain event.
func (d PRClosedDTO) ToDomain() event.PRClosed {
	var edits []event.PRClosedEdit
	if d.Edits != nil {
		edits = make([]event.PRClosedEdit, len(d.Edits))
		for i, ed := range d.Edits {
			edits[i] = event.PRClosedEdit(ed)
		}
	}
	return event.PRClosed{
		ProposalID:      d.ProposalID,
		ReleaseID:       d.ReleaseID,
		NodeID:          d.NodeID,
		ResolvedNodeIDs: d.ResolvedNodeIDs,
		Service:         d.Service,
		PrURL:           d.PrURL,
		PrNumber:        d.PrNumber,
		Outcome:         d.Outcome,
		ClosedAt:        d.ClosedAt,
		Edits:           edits,
	}
}

// PromotedSeedsNodeDTO is the JSON shape of one node in a trigger.promoted_seeds:v1
// nodes array.
type PromotedSeedsNodeDTO struct {
	ServiceName string `json:"service_name"`
	SchemaName  string `json:"schema_name"`
	TableName   string `json:"table_name"`
	NodeType    string `json:"node_type"`
	ImageTag    string `json:"image_tag"`
}

// PromotedSeedsNodesToDomain maps decoded node DTOs to domain nodes.
func PromotedSeedsNodesToDomain(in []PromotedSeedsNodeDTO) []model.PromotedSeedsNode {
	if in == nil {
		return nil
	}
	out := make([]model.PromotedSeedsNode, len(in))
	for i, n := range in {
		out[i] = model.PromotedSeedsNode(n)
	}
	return out
}
