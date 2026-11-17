package grpc

import (
	"context"
	"errors"
	"log/slog"
	"time"

	catalogv1 "github.com/carolsimone/continuo/catalog/api/catalog/v1"
	"github.com/carolsimone/continuo/catalog/domain"
	"github.com/carolsimone/continuo/catalog/domain/casebase"
	"github.com/carolsimone/continuo/catalog/domain/codeversion"
	"github.com/carolsimone/continuo/catalog/domain/contractversion"
	"github.com/carolsimone/continuo/catalog/service/queries"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/pkg/num"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NodeReader returns a tenant's node metadata and node source locations.
// Reads go straight to Neo4j and return raw domain types. Satisfied by
// adapters/neo4j.NodeQueryRepository.
type NodeReader interface {
	GetNode(ctx context.Context, tenantID, service, schema, table string, scope domain.NodeScope) (*domain.NodeMeta, error)
	GetNodeLocation(ctx context.Context, tenantID, uniqueID string) (*domain.NodeLocation, error)
}

// CodeVersionHistoryReader returns a tenant's code-version history views
// served by the code-version RPCs: node version chains, rendered diffs,
// upstream changes, and shared-code unit chains. Cap policy (upstream ancestor
// count, diff byte size) and limit defaulting/clamping live here, not in the
// handler. Satisfied by service/queries.CodeVersionQueryService.
type CodeVersionHistoryReader interface {
	GetNodeVersions(ctx context.Context, tenantID, uniqueID string, limit int32, includeCode bool) ([]codeversion.VersionView, error)
	// GetCurrentNodeVersion returns only the version the node runs now — a
	// 0-or-1 slice, revert-safe where GetNodeVersions' newest-first is not.
	GetCurrentNodeVersion(ctx context.Context, tenantID, uniqueID string, includeCode bool) ([]codeversion.VersionView, error)
	GetNodeVersionDiff(ctx context.Context, tenantID, uniqueID string, fromSeq, toSeq int64) (*codeversion.VersionDiff, error)
	GetUpstreamChanges(ctx context.Context, tenantID, uniqueID string, depth int32, since time.Time) ([]codeversion.UpstreamChange, error)
	GetCodeUnitVersions(ctx context.Context, tenantID, unitID, uniqueID string, limit int32) ([]codeversion.UnitVersionView, error)
}

// PrecedentHistoryReader serves the failure-precedent RPC for a tenant.
// Satisfied by service/queries.PrecedentQueryService.
type PrecedentHistoryReader interface {
	GetPrecedents(ctx context.Context, tenantID, signature, category, reason string, limit int32, includeCode bool) ([]casebase.Precedent, error)
}

// ContractHistoryReader serves the contract RPCs for a tenant: a node's
// current contract, its history and the field diff of two versions. Satisfied
// by service/queries.ContractQueryService.
type ContractHistoryReader interface {
	// GetNodeContract returns the contract a live node has now, with its
	// published YAML when includeDocument is set. A node with no contract
	// returns contractversion.ErrNotFound.
	GetNodeContract(ctx context.Context, tenantID, uniqueID string, includeDocument bool) (queries.NodeContract, error)
	// ListNodeContractVersions lists the node's versions, newest first, up to
	// limit (every version when limit is 0).
	ListNodeContractVersions(ctx context.Context, tenantID, uniqueID string, limit int) ([]contractversion.VersionView, error)
	// GetNodeContractDiff lists the fields that differ between two versions. A
	// version the node has none of returns contractversion.ErrNotFound.
	GetNodeContractDiff(ctx context.Context, tenantID, uniqueID string, fromVersion, toVersion int64) ([]contractversion.FieldChange, error)
}

// QueryHandler implements the CatalogQuery gRPC service. Every RPC reads for
// the tenant the identity interceptor placed on the request.
type QueryHandler struct {
	catalogv1.UnimplementedCatalogQueryServer
	nodes        NodeReader
	codeVersions CodeVersionHistoryReader
	precedents   PrecedentHistoryReader
	contracts    ContractHistoryReader
	logger       *slog.Logger
}

func NewQueryHandler(nodes NodeReader, codeVersions CodeVersionHistoryReader, precedents PrecedentHistoryReader, contracts ContractHistoryReader, logger *slog.Logger) *QueryHandler {
	return &QueryHandler{nodes: nodes, codeVersions: codeVersions, precedents: precedents, contracts: contracts, logger: logger}
}

// requestTenant returns the tenant the request acts for, which the identity
// interceptor at the gRPC edge placed on ctx. A context that carries no tenant
// is refused with Internal before anything is read: the server serving it was
// built without the interceptor, and no handler acts for a tenant it guessed.
func requestTenant(ctx context.Context) (string, error) {
	tenantID := identity.TenantFromContext(ctx)
	if tenantID == "" {
		return "", status.Error(codes.Internal, "the request carries no tenant")
	}
	return tenantID, nil
}

// GetNode returns the metadata of one of the tenant's nodes; a retired node
// is found only when the request sets include_inactive.
func (h *QueryHandler) GetNode(ctx context.Context, req *catalogv1.GetNodeRequest) (*catalogv1.GetNodeResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	if req.ServiceName == "" || req.SchemaName == "" || req.TableName == "" {
		return nil, status.Error(codes.InvalidArgument, "service_name, schema_name and table_name are required")
	}
	scope := domain.ActiveNodesOnly
	if req.GetIncludeInactive() {
		scope = domain.IncludeInactiveNodes
	}
	meta, err := h.nodes.GetNode(ctx, tenantID, req.ServiceName, req.SchemaName, req.TableName, scope)
	if err != nil {
		if errors.Is(err, domain.ErrNodeNotFound) {
			return nil, status.Errorf(codes.NotFound, "node %s.%s.%s not found", req.ServiceName, req.SchemaName, req.TableName)
		}
		h.logger.Error("GetNode failed", "service", req.ServiceName, "schema", req.SchemaName, "table", req.TableName, "error", err)
		return nil, status.Errorf(codes.Internal, "GetNode: %v", err)
	}
	return &catalogv1.GetNodeResponse{
		NodeType:       meta.NodeType,
		TestCount:      num.ClampInt32(meta.TestCount),
		TestCountKnown: meta.TestCountKnown,
		Inactive:       meta.Inactive,
	}, nil
}

// GetNodeLocation returns where a node's source lives. An unknown or retired
// unique_id maps to NOT_FOUND per the entity-lookup convention.
func (h *QueryHandler) GetNodeLocation(ctx context.Context, req *catalogv1.GetNodeLocationRequest) (*catalogv1.GetNodeLocationResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	loc, err := h.nodes.GetNodeLocation(ctx, tenantID, req.UniqueId)
	if err != nil {
		if errors.Is(err, domain.ErrNodeNotFound) {
			return nil, status.Errorf(codes.NotFound, "node %q not found", req.UniqueId)
		}
		h.logger.Error("GetNodeLocation failed", "unique_id", req.UniqueId, "error", err)
		return nil, status.Errorf(codes.Internal, "get node location: %v", err)
	}
	return &catalogv1.GetNodeLocationResponse{FilePath: loc.FilePath, ServiceName: loc.ServiceName}, nil
}

// maxUpstreamDepth caps GetUpstreamChanges' depth request; the proto
// documents this as the maximum and rejects anything above it.
const maxUpstreamDepth = 10

// GetNodeVersions lists a node's versions, or with current_only the one it
// runs now. No version of a retired node is current, so current_only returns
// an empty list for one.
func (h *QueryHandler) GetNodeVersions(ctx context.Context, req *catalogv1.GetNodeVersionsRequest) (*catalogv1.GetNodeVersionsResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	if req.UniqueId == "" {
		return nil, status.Error(codes.InvalidArgument, "unique_id is required")
	}
	if req.CurrentOnly {
		versions, err := h.codeVersions.GetCurrentNodeVersion(ctx, tenantID, req.UniqueId, req.IncludeCode)
		if err != nil {
			if errors.Is(err, domain.ErrNodeNotFound) {
				return nil, status.Errorf(codes.NotFound, "node %q not found", req.UniqueId)
			}
			h.logger.Error("GetNodeVersions current_only failed", "unique_id", req.UniqueId, "error", err)
			return nil, status.Errorf(codes.Internal, "get current node version: %v", err)
		}
		out := make([]*catalogv1.VersionView, 0, len(versions))
		for _, v := range versions {
			out = append(out, codeVersionViewToProto(v))
		}
		return &catalogv1.GetNodeVersionsResponse{Versions: out}, nil
	}
	versions, err := h.codeVersions.GetNodeVersions(ctx, tenantID, req.UniqueId, req.Limit, req.IncludeCode)
	if err != nil {
		if errors.Is(err, domain.ErrNodeNotFound) {
			return nil, status.Errorf(codes.NotFound, "node %q not found", req.UniqueId)
		}
		h.logger.Error("GetNodeVersions failed", "unique_id", req.UniqueId, "error", err)
		return nil, status.Errorf(codes.Internal, "GetNodeVersions: %v", err)
	}
	resp := &catalogv1.GetNodeVersionsResponse{
		Versions: make([]*catalogv1.VersionView, 0, len(versions)),
	}
	for _, v := range versions {
		resp.Versions = append(resp.Versions, codeVersionViewToProto(v))
	}
	return resp, nil
}

func (h *QueryHandler) GetNodeVersionDiff(ctx context.Context, req *catalogv1.GetNodeVersionDiffRequest) (*catalogv1.GetNodeVersionDiffResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	if req.UniqueId == "" {
		return nil, status.Error(codes.InvalidArgument, "unique_id is required")
	}
	if req.FromSeq == req.ToSeq {
		return nil, status.Error(codes.InvalidArgument, "from_seq and to_seq must differ")
	}
	diff, err := h.codeVersions.GetNodeVersionDiff(ctx, tenantID, req.UniqueId, req.FromSeq, req.ToSeq)
	if err != nil {
		if errors.Is(err, domain.ErrNodeNotFound) {
			return nil, status.Errorf(codes.NotFound, "node %q version %d or %d not found", req.UniqueId, req.FromSeq, req.ToSeq)
		}
		h.logger.Error("GetNodeVersionDiff failed", "unique_id", req.UniqueId, "error", err)
		return nil, status.Errorf(codes.Internal, "GetNodeVersionDiff: %v", err)
	}
	return &catalogv1.GetNodeVersionDiffResponse{Diff: codeVersionDiffToProto(*diff)}, nil
}

func (h *QueryHandler) GetUpstreamChanges(ctx context.Context, req *catalogv1.GetUpstreamChangesRequest) (*catalogv1.GetUpstreamChangesResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	if req.UniqueId == "" {
		return nil, status.Error(codes.InvalidArgument, "unique_id is required")
	}
	if req.Depth > maxUpstreamDepth {
		return nil, status.Errorf(codes.InvalidArgument, "depth must be <= %d", maxUpstreamDepth)
	}
	var since time.Time
	if req.Since != "" {
		parsed, err := time.Parse(time.RFC3339, req.Since)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "since must be RFC3339: %v", err)
		}
		since = parsed
	}
	changes, err := h.codeVersions.GetUpstreamChanges(ctx, tenantID, req.UniqueId, req.Depth, since)
	if err != nil {
		if errors.Is(err, domain.ErrNodeNotFound) {
			return nil, status.Errorf(codes.NotFound, "node %q not found", req.UniqueId)
		}
		h.logger.Error("GetUpstreamChanges failed", "unique_id", req.UniqueId, "error", err)
		return nil, status.Errorf(codes.Internal, "GetUpstreamChanges: %v", err)
	}
	resp := &catalogv1.GetUpstreamChangesResponse{
		Changes: make([]*catalogv1.UpstreamChange, 0, len(changes)),
	}
	for _, c := range changes {
		resp.Changes = append(resp.Changes, codeUpstreamChangeToProto(c))
	}
	return resp, nil
}

func (h *QueryHandler) GetCodeUnitVersions(ctx context.Context, req *catalogv1.GetCodeUnitVersionsRequest) (*catalogv1.GetCodeUnitVersionsResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	if (req.UnitId == "") == (req.UniqueId == "") {
		return nil, status.Error(codes.InvalidArgument, "exactly one of unit_id or unique_id is required")
	}
	versions, err := h.codeVersions.GetCodeUnitVersions(ctx, tenantID, req.UnitId, req.UniqueId, req.Limit)
	if err != nil {
		if errors.Is(err, domain.ErrNodeNotFound) || errors.Is(err, domain.ErrUnitNotFound) {
			id := req.UnitId
			if id == "" {
				id = req.UniqueId
			}
			return nil, status.Errorf(codes.NotFound, "%q not found", id)
		}
		h.logger.Error("GetCodeUnitVersions failed", "unit_id", req.UnitId, "unique_id", req.UniqueId, "error", err)
		return nil, status.Errorf(codes.Internal, "GetCodeUnitVersions: %v", err)
	}
	resp := &catalogv1.GetCodeUnitVersionsResponse{
		Versions: make([]*catalogv1.UnitVersionView, 0, len(versions)),
	}
	for _, v := range versions {
		resp.Versions = append(resp.Versions, codeUnitVersionViewToProto(v))
	}
	return resp, nil
}

func (h *QueryHandler) GetPrecedents(ctx context.Context, req *catalogv1.GetPrecedentsRequest) (*catalogv1.GetPrecedentsResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	if req.Signature == "" && (req.Category == "" || req.Reason == "") {
		return nil, status.Error(codes.InvalidArgument, "either signature, or both category and reason, are required")
	}
	precedents, err := h.precedents.GetPrecedents(ctx, tenantID, req.Signature, req.Category, req.Reason, req.Limit, req.IncludeCode)
	if err != nil {
		h.logger.Error("GetPrecedents failed", "signature", req.Signature, "category", req.Category, "reason", req.Reason, "error", err)
		return nil, status.Errorf(codes.Internal, "GetPrecedents: %v", err)
	}
	out := make([]*catalogv1.Precedent, 0, len(precedents))
	for _, p := range precedents {
		out = append(out, precedentToProto(p))
	}
	return &catalogv1.GetPrecedentsResponse{Precedents: out}, nil
}

// GetNodeContract returns the contract a live node has now. A node with no
// contract, a retired node and an unknown unique_id are NOT_FOUND.
func (h *QueryHandler) GetNodeContract(ctx context.Context, req *catalogv1.GetNodeContractRequest) (*catalogv1.GetNodeContractResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	if req.UniqueId == "" {
		return nil, status.Error(codes.InvalidArgument, "unique_id is required")
	}
	contract, err := h.contracts.GetNodeContract(ctx, tenantID, req.UniqueId, req.IncludeDocument)
	if err != nil {
		if errors.Is(err, contractversion.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "node %q has no contract", req.UniqueId)
		}
		h.logger.Error("GetNodeContract failed", "unique_id", req.UniqueId, "error", err)
		return nil, status.Errorf(codes.Internal, "GetNodeContract: %v", err)
	}
	view := contractVersionViewToProto(contract.Version)
	view.DocumentYaml = contract.DocumentYAML
	return &catalogv1.GetNodeContractResponse{Contract: view}, nil
}

// ListNodeContractVersions lists a node's contract history, newest first.
// limit 0 lists every version.
func (h *QueryHandler) ListNodeContractVersions(ctx context.Context, req *catalogv1.ListNodeContractVersionsRequest) (*catalogv1.ListNodeContractVersionsResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	if req.UniqueId == "" {
		return nil, status.Error(codes.InvalidArgument, "unique_id is required")
	}
	if req.Limit < 0 {
		return nil, status.Error(codes.InvalidArgument, "limit must be 0 (every version) or more")
	}
	versions, err := h.contracts.ListNodeContractVersions(ctx, tenantID, req.UniqueId, int(req.Limit))
	if err != nil {
		h.logger.Error("ListNodeContractVersions failed", "unique_id", req.UniqueId, "error", err)
		return nil, status.Errorf(codes.Internal, "ListNodeContractVersions: %v", err)
	}
	resp := &catalogv1.ListNodeContractVersionsResponse{
		Versions: make([]*catalogv1.ContractVersionView, 0, len(versions)),
	}
	for _, v := range versions {
		resp.Versions = append(resp.Versions, contractVersionViewToProto(v))
	}
	return resp, nil
}

// GetNodeContractDiff lists the fields that differ between two versions of a
// node's contract. A version below 1 is INVALID_ARGUMENT, and a version the
// node has no promotion for is NOT_FOUND.
func (h *QueryHandler) GetNodeContractDiff(ctx context.Context, req *catalogv1.GetNodeContractDiffRequest) (*catalogv1.GetNodeContractDiffResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	if req.UniqueId == "" {
		return nil, status.Error(codes.InvalidArgument, "unique_id is required")
	}
	if req.FromVersion < 1 || req.ToVersion < 1 {
		return nil, status.Error(codes.InvalidArgument, "from_version and to_version must be 1 or more")
	}
	changes, err := h.contracts.GetNodeContractDiff(ctx, tenantID, req.UniqueId, req.FromVersion, req.ToVersion)
	if err != nil {
		if errors.Is(err, contractversion.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "node %q has no contract version %d or %d", req.UniqueId, req.FromVersion, req.ToVersion)
		}
		h.logger.Error("GetNodeContractDiff failed", "unique_id", req.UniqueId, "error", err)
		return nil, status.Errorf(codes.Internal, "GetNodeContractDiff: %v", err)
	}
	resp := &catalogv1.GetNodeContractDiffResponse{
		Changes: make([]*catalogv1.ContractFieldChange, 0, len(changes)),
	}
	for _, c := range changes {
		resp.Changes = append(resp.Changes, &catalogv1.ContractFieldChange{
			Path: c.Path, BeforeJson: c.BeforeJSON, AfterJson: c.AfterJSON,
		})
	}
	return resp, nil
}

func precedentToProto(p casebase.Precedent) *catalogv1.Precedent {
	pb := &catalogv1.Precedent{
		ReleaseId:               p.Rejection.ReleaseID,
		NodeId:                  p.Rejection.NodeID,
		Stage:                   p.Rejection.Stage,
		Category:                p.Rejection.Category,
		Reason:                  p.Rejection.Reason,
		ErrorExcerpt:            p.Rejection.ErrorExcerpt,
		RejectedAt:              p.Rejection.At.UTC().Format(time.RFC3339),
		FailingCode:             p.Rejection.RawCode,
		Resolved:                p.Resolved,
		ResolutionDiff:          p.ResolutionDiff,
		ResolutionDiffTruncated: p.ResolutionDiffTruncated,
	}
	if p.ResolvingVersion != nil {
		pb.ResolvingVersion = codeVersionViewToProto(*p.ResolvingVersion)
	}
	for _, pr := range p.Proposals {
		pb.Proposals = append(pb.Proposals, &catalogv1.PrecedentProposal{
			ProposalId: pr.ProposalID, PrUrl: pr.PrURL,
			PrNumber: num.ClampInt32(pr.PrNumber), PrState: pr.PrState,
		})
	}
	for _, e := range p.Edited {
		pb.Edited = append(pb.Edited, &catalogv1.PrecedentEdit{
			NodeId: e.NodeID, Path: e.Path, Amended: e.Amended, Diff: e.Diff,
			DiffIsShipped: e.DiffIsShipped,
		})
	}
	return pb
}

// formatOptionalRFC3339 renders t as RFC3339, or "" for the zero time — the
// wire contract's convention for "not recorded" across every code-version
// timestamp field.
func formatOptionalRFC3339(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

func codeVersionViewToProto(v codeversion.VersionView) *catalogv1.VersionView {
	return &catalogv1.VersionView{
		UniqueId:          v.UniqueID,
		VersionSeq:        v.VersionSeq,
		ContentHash:       v.ContentHash,
		SourceHash:        v.SourceHash,
		SharedCodeHash:    v.SharedCodeHash,
		ConfigHash:        v.ConfigHash,
		Runtime:           v.Runtime,
		RawCode:           v.RawCode,
		CompiledCode:      v.CompiledCode,
		CompiledTruncated: v.CompiledTruncated,
		ConfigJson:        v.ConfigJSON,
		Repo:              v.Repo,
		CommitSha:         v.CommitSHA,
		ReleaseId:         v.ReleaseID,
		PromotedAt:        formatOptionalRFC3339(v.PromotedAt),
		Healed:            v.Healed,
		Backfilled:        v.Backfilled,
		IsCurrent:         v.IsCurrent,
	}
}

// contractVersionViewToProto maps one promotion of a node's contract history
// to its wire form, without a document. promoted_at is RFC 3339 in UTC.
func contractVersionViewToProto(v contractversion.VersionView) *catalogv1.ContractVersionView {
	return &catalogv1.ContractVersionView{
		UniqueId:          v.UniqueID,
		Version:           v.Version,
		ContractHash:      v.ContractHash,
		ReleaseId:         v.ReleaseID,
		PromotedAt:        formatOptionalRFC3339(v.PromotedAt.UTC()),
		ContentHash:       v.ContentHash,
		IsCurrent:         v.IsCurrent,
		SameAsVersion:     v.SameAsVersion,
		ContinuoContracts: v.ContinuoContracts,
		OntologyVersion:   v.OntologyVersion,
	}
}

func codeVersionDiffToProto(d codeversion.VersionDiff) *catalogv1.VersionDiff {
	return &catalogv1.VersionDiff{
		UniqueId:          d.UniqueID,
		From:              codeVersionViewToProto(d.From),
		To:                codeVersionViewToProto(d.To),
		RawCodeDiff:       d.RawCodeDiff,
		ConfigDiff:        d.ConfigDiff,
		SourceChanged:     d.SourceChanged,
		SharedCodeChanged: d.SharedCodeChanged,
		ConfigChanged:     d.ConfigChanged,
		Truncated:         d.Truncated,
	}
}

func codeUpstreamChangeToProto(c codeversion.UpstreamChange) *catalogv1.UpstreamChange {
	return &catalogv1.UpstreamChange{
		UniqueId: c.UniqueID,
		Depth:    c.Depth,
		Diff:     codeVersionDiffToProto(c.Diff),
	}
}

func codeUnitVersionViewToProto(v codeversion.UnitVersionView) *catalogv1.UnitVersionView {
	return &catalogv1.UnitVersionView{
		UnitId:     v.UnitID,
		Checksum:   v.Checksum,
		Source:     v.Source,
		Repo:       v.Repo,
		CommitSha:  v.CommitSHA,
		ReleaseId:  v.ReleaseID,
		PromotedAt: formatOptionalRFC3339(v.PromotedAt),
		IsCurrent:  v.IsCurrent,
	}
}
