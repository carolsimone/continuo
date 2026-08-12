// File: orchestrator/service/queries/code_version_query_service.go
//
// CodeVersionQueryService is the read-side application service over the
// code-version graph's recorded history. The port returns raw views from
// storage; this service applies caps and renders diffs, so cap policy and
// diff rendering are testable without a database.
package queries

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/carolsimone/continuo/orchestrator/domain/codeversion"
	"github.com/pmezard/go-difflib/difflib"
)

// upstreamAncestorCap and diffByteCap are contract, not tuning. remediation's
// prompt builder inherits them from the GitHub path this replaces, so changing
// either silently changes prompt size for every heal.
const (
	upstreamAncestorCap = 5
	diffByteCap         = 8 * 1024
)

// defaultUpstreamDepth is used when GetUpstreamChanges receives depth <= 0.
const defaultUpstreamDepth = 3

// defaultQueryLimit and maxQueryLimit bound every list-limit parameter on
// this service: proto3 leaves an unset int32 at its zero value, so <= 0 falls
// back to the default rather than reaching the reader as a 0-row request;
// anything above the max is clamped.
const (
	defaultQueryLimit = 20
	maxQueryLimit     = 200
)

// CodeVersionReader is the read surface over the code-version graph.
type CodeVersionReader interface {
	// NodeVersions walks the chain from :CURRENT, newest first, up to limit.
	// An unknown node returns ErrNodeNotFound; a known node with no recorded
	// history returns an empty slice.
	NodeVersions(ctx context.Context, uniqueID string, limit int32) ([]codeversion.VersionView, error)
	// VersionsBySeq returns the two named versions of one node. An unknown
	// node, or a seq that node has no recorded version for, returns
	// ErrNodeNotFound.
	VersionsBySeq(ctx context.Context, uniqueID string, fromSeq, toSeq int64) (from, to codeversion.VersionView, err error)
	// Ancestors returns the node's transitive upstreams up to depth, each with
	// its two most recent versions, most-recently-changed first.
	Ancestors(ctx context.Context, uniqueID string, depth int32, since time.Time) ([]codeversion.AncestorVersions, error)
	// UnitVersions walks a shared-code unit's chain, newest first.
	UnitVersions(ctx context.Context, unitID string, limit int32) ([]codeversion.UnitVersionView, error)
	// UnitsForNode returns the units the node's current version uses.
	UnitsForNode(ctx context.Context, uniqueID string) ([]string, error)
	// RunExecutions returns runs that executed the node, newest first.
	RunExecutions(ctx context.Context, uniqueID string, limit int32) ([]codeversion.RunExecution, error)
}

// CodeVersionQueryService composes the code-version reader into the capped,
// diff-rendered views the gRPC and CLI surfaces serve.
type CodeVersionQueryService struct {
	reader CodeVersionReader
}

// NewCodeVersionQueryService constructs a CodeVersionQueryService.
func NewCodeVersionQueryService(reader CodeVersionReader) *CodeVersionQueryService {
	return &CodeVersionQueryService{reader: reader}
}

// GetNodeVersions returns a node's version history, newest first, up to
// limit. limit <= 0 defaults to defaultQueryLimit; anything above
// maxQueryLimit is clamped to it.
func (s *CodeVersionQueryService) GetNodeVersions(ctx context.Context, uniqueID string, limit int32) ([]codeversion.VersionView, error) {
	versions, err := s.reader.NodeVersions(ctx, uniqueID, clampQueryLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("CodeVersionQueryService.GetNodeVersions: %w", err)
	}
	return versions, nil
}

// GetNodeVersionDiff renders the diff between two named versions of one node.
func (s *CodeVersionQueryService) GetNodeVersionDiff(ctx context.Context, uniqueID string, fromSeq, toSeq int64) (*codeversion.VersionDiff, error) {
	from, to, err := s.reader.VersionsBySeq(ctx, uniqueID, fromSeq, toSeq)
	if err != nil {
		return nil, fmt.Errorf("CodeVersionQueryService.GetNodeVersionDiff: %w", err)
	}
	diff := renderDiff(uniqueID, from, to)
	return &diff, nil
}

// GetUpstreamChanges returns the node's ancestors' most recent code changes,
// most-recently-changed first, capped at upstreamAncestorCap. depth <= 0
// defaults to defaultUpstreamDepth; a zero-value since applies no time filter.
func (s *CodeVersionQueryService) GetUpstreamChanges(ctx context.Context, uniqueID string, depth int32, since time.Time) ([]codeversion.UpstreamChange, error) {
	if depth <= 0 {
		depth = defaultUpstreamDepth
	}
	ancestors, err := s.reader.Ancestors(ctx, uniqueID, depth, since)
	if err != nil {
		return nil, fmt.Errorf("CodeVersionQueryService.GetUpstreamChanges: %w", err)
	}
	if len(ancestors) == 0 {
		// Ancestors returns an empty slice both for an unknown node and for a
		// known node with no upstreams (or none surviving the since filter) —
		// it cannot tell those apart. NodeVersions can: it distinguishes an
		// unknown unique_id from a known one, so use it purely as an
		// existence check without paying for a second full ancestry query.
		if _, err := s.reader.NodeVersions(ctx, uniqueID, 1); err != nil {
			return nil, fmt.Errorf("CodeVersionQueryService.GetUpstreamChanges: %w", err)
		}
		return []codeversion.UpstreamChange{}, nil
	}

	changes := make([]codeversion.UpstreamChange, 0, len(ancestors))
	for _, a := range ancestors {
		var diff codeversion.VersionDiff
		switch len(a.Versions) {
		case 0:
			// No recorded version for this ancestor — nothing to report.
			continue
		case 1:
			// No prior version to compare against: the whole code is the change.
			diff = renderDiff(a.UniqueID, codeversion.VersionView{}, a.Versions[0])
		default:
			diff = renderDiff(a.UniqueID, a.Versions[1], a.Versions[0])
		}
		changes = append(changes, codeversion.UpstreamChange{
			UniqueID: a.UniqueID,
			Depth:    a.Depth,
			Diff:     diff,
		})
	}

	// The reader already orders ancestors most-recently-changed first; cap
	// after that ordering so the kept 5 are the most recently changed.
	if len(changes) > upstreamAncestorCap {
		changes = changes[:upstreamAncestorCap]
	}
	return changes, nil
}

// GetCodeUnitVersions returns a shared-code unit's version chain, newest
// first, up to limit. Pass unitID to query one unit directly; leave it empty
// and pass uniqueID to resolve the node's current units first, returning each
// of their chains concatenated in the order UnitsForNode returns them. limit
// <= 0 defaults to defaultQueryLimit; anything above maxQueryLimit is clamped
// to it.
func (s *CodeVersionQueryService) GetCodeUnitVersions(ctx context.Context, unitID, uniqueID string, limit int32) ([]codeversion.UnitVersionView, error) {
	limit = clampQueryLimit(limit)
	if unitID != "" {
		versions, err := s.reader.UnitVersions(ctx, unitID, limit)
		if err != nil {
			return nil, fmt.Errorf("CodeVersionQueryService.GetCodeUnitVersions: %w", err)
		}
		return versions, nil
	}

	unitIDs, err := s.reader.UnitsForNode(ctx, uniqueID)
	if err != nil {
		return nil, fmt.Errorf("CodeVersionQueryService.GetCodeUnitVersions: %w", err)
	}
	all := make([]codeversion.UnitVersionView, 0, len(unitIDs))
	for _, id := range unitIDs {
		versions, err := s.reader.UnitVersions(ctx, id, limit)
		if err != nil {
			return nil, fmt.Errorf("CodeVersionQueryService.GetCodeUnitVersions unit %s: %w", id, err)
		}
		all = append(all, versions...)
	}
	return all, nil
}

// GetNodeRunHistory returns runs that executed the node, newest first.
// limit <= 0 defaults to defaultQueryLimit; anything above maxQueryLimit is
// clamped to it.
func (s *CodeVersionQueryService) GetNodeRunHistory(ctx context.Context, uniqueID string, limit int32) ([]codeversion.RunExecution, error) {
	runs, err := s.reader.RunExecutions(ctx, uniqueID, clampQueryLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("CodeVersionQueryService.GetNodeRunHistory: %w", err)
	}
	return runs, nil
}

// clampQueryLimit applies the default/max bound shared by every list-limit
// parameter on this service.
func clampQueryLimit(limit int32) int32 {
	if limit <= 0 {
		return defaultQueryLimit
	}
	if limit > maxQueryLimit {
		return maxQueryLimit
	}
	return limit
}

// renderDiff compares two versions of one node and produces the diff view the
// gRPC and CLI surfaces serve: the three-part hash comparison exposes exactly
// what changed, and the unified diffs show it.
func renderDiff(uniqueID string, from, to codeversion.VersionView) codeversion.VersionDiff {
	rawDiff, rawTruncated := renderUnifiedDiff(from.RawCode, to.RawCode, from.VersionSeq, to.VersionSeq)
	configDiff, configTruncated := renderConfigDiff(from.ConfigJSON, to.ConfigJSON, from.VersionSeq, to.VersionSeq)
	return codeversion.VersionDiff{
		UniqueID:          uniqueID,
		From:              from,
		To:                to,
		RawCodeDiff:       rawDiff,
		ConfigDiff:        configDiff,
		SourceChanged:     from.SourceHash != to.SourceHash,
		SharedCodeChanged: from.SharedCodeHash != to.SharedCodeHash,
		ConfigChanged:     from.ConfigHash != to.ConfigHash,
		Truncated:         rawTruncated || configTruncated,
	}
}

// renderConfigDiff diffs the canonical (sorted-key, indented) form of each
// side's config JSON, so key order alone can never manufacture a diff. An
// empty side is treated as "{}"; if canonicalization fails on either side,
// it falls back to diffing the raw strings rather than failing the request.
func renderConfigDiff(fromJSON, toJSON string, fromSeq, toSeq int64) (diff string, truncated bool) {
	return renderUnifiedDiff(canonicalizeConfigJSON(fromJSON), canonicalizeConfigJSON(toJSON), fromSeq, toSeq)
}

func canonicalizeConfigJSON(raw string) string {
	if raw == "" {
		raw = "{}"
	}
	var v interface{}
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return raw
	}
	canon, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return raw
	}
	return string(canon)
}

// renderUnifiedDiff renders a unified diff between two texts, labeled with
// their version seqs, cut to diffByteCap bytes on a rune boundary if needed.
func renderUnifiedDiff(fromText, toText string, fromSeq, toSeq int64) (diff string, truncated bool) {
	if fromText == toText {
		return "", false
	}
	ud := difflib.UnifiedDiff{
		A:        difflib.SplitLines(fromText),
		B:        difflib.SplitLines(toText),
		FromFile: fmt.Sprintf("version %d", fromSeq),
		ToFile:   fmt.Sprintf("version %d", toSeq),
		Context:  3,
	}
	text, err := difflib.GetUnifiedDiffString(ud)
	if err != nil {
		// difflib does not fail on well-formed line input; degrade to an
		// empty diff rather than propagate an internal rendering error.
		return "", false
	}
	return truncateToByteCap(text, diffByteCap)
}

// truncateToByteCap cuts s to at most capBytes bytes without splitting a
// UTF-8 rune, walking back from the cap to the nearest rune start.
func truncateToByteCap(s string, capBytes int) (string, bool) {
	if len(s) <= capBytes {
		return s, false
	}
	cut := capBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut], true
}
