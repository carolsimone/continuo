// File: catalog/service/queries/contract_query_service.go
//
// ContractQueryService is the read-side application service over a tenant's
// contract history. The port returns each promotion's stored contract; this
// service renders the published form of the current one and the field diff of
// two, so both are testable without a database.
package queries

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/catalog/domain/contractversion"
	"github.com/carolsimone/continuo/catalog/service/ports"
)

// NodeContract is a node's current contract version and, when it was asked
// for, the version's published form as YAML.
type NodeContract struct {
	Version contractversion.VersionView
	// DocumentYAML is the published form of the contract without a `servers`
	// list; empty when the document was not asked for.
	DocumentYAML string
}

// ContractQueryService composes the contract reader into the views the gRPC
// and CLI surfaces serve.
type ContractQueryService struct {
	reader ContractReader
	// renderer writes a published contract as YAML.
	renderer ports.PublishedRenderer
	// renderValue writes the value of a changed path as JSON text.
	renderValue func(any) string
}

// NewContractQueryService constructs a ContractQueryService. renderValue
// writes one decoded JSON value as text and is called for the values of a
// diff's rows.
func NewContractQueryService(reader ContractReader, renderer ports.PublishedRenderer, renderValue func(any) string) *ContractQueryService {
	return &ContractQueryService{reader: reader, renderer: renderer, renderValue: renderValue}
}

// GetNodeContract returns the contract a live node has now. With
// includeDocument it also renders the contract's published form, as the live
// folder writes it but with no `servers` list, since that list names an
// install's warehouse and this answer is the contract itself. A node with no
// current contract returns contractversion.ErrNotFound.
func (s *ContractQueryService) GetNodeContract(ctx context.Context, tenantID, uniqueID string, includeDocument bool) (NodeContract, error) {
	version, err := s.reader.Current(ctx, tenantID, uniqueID)
	if err != nil {
		return NodeContract{}, fmt.Errorf("ContractQueryService.GetNodeContract: %w", err)
	}
	out := NodeContract{Version: version}
	if !includeDocument {
		return out, nil
	}
	published := contractversion.Published(version.Document, version.Version, nil, contractversion.Provenance{
		ContractHash:      version.ContractHash,
		ContentHash:       version.ContentHash,
		ReleaseID:         version.ReleaseID,
		ContinuoContracts: version.ContinuoContracts,
		SourcePath:        version.SourcePath,
		PromotionSeq:      version.PromotionSeq,
		PromotedAt:        version.PromotedAt,
	})
	body, err := s.renderer.Render(published)
	if err != nil {
		return NodeContract{}, fmt.Errorf("ContractQueryService.GetNodeContract: render node %s version %d: %w", uniqueID, version.Version, err)
	}
	out.DocumentYAML = string(body)
	return out, nil
}

// ListNodeContractVersions lists a node's contract history, newest first, up
// to limit (every version when limit is 0). The rows carry no document. A node
// with no history has an empty list.
func (s *ContractQueryService) ListNodeContractVersions(ctx context.Context, tenantID, uniqueID string, limit int) ([]contractversion.VersionView, error) {
	versions, err := s.reader.History(ctx, tenantID, uniqueID, limit)
	if err != nil {
		return nil, fmt.Errorf("ContractQueryService.ListNodeContractVersions: %w", err)
	}
	return versions, nil
}

// GetNodeContractDiff lists the paths at which two of a node's stored
// contracts differ, each side's value written by the service's value renderer.
// A version the node has no promotion for returns contractversion.ErrNotFound.
func (s *ContractQueryService) GetNodeContractDiff(ctx context.Context, tenantID, uniqueID string, fromVersion, toVersion int64) ([]contractversion.FieldChange, error) {
	from, err := s.reader.ByVersion(ctx, tenantID, uniqueID, fromVersion)
	if err != nil {
		return nil, fmt.Errorf("ContractQueryService.GetNodeContractDiff: version %d: %w", fromVersion, err)
	}
	to, err := s.reader.ByVersion(ctx, tenantID, uniqueID, toVersion)
	if err != nil {
		return nil, fmt.Errorf("ContractQueryService.GetNodeContractDiff: version %d: %w", toVersion, err)
	}
	return contractversion.Diff(from.Document, to.Document, s.renderValue), nil
}
