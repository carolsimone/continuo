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

// upstreamAncestorCap and diffByteCap are contract, not tuning. They mirror
// the caps agent-remediation's prompt builder enforces on its GitHub-read
// upstream diffs, so a consumer switching between the two paths sees the same
// prompt size; changing either silently changes prompt size for every heal.
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
	// includeCode controls whether raw_code/compiled_code are fetched at all:
	// false must keep those fields off the wire between Neo4j and this
	// process, not merely blank them after the fact, since a version's
	// compiled_code alone can run to 256 KiB. An unknown node returns
	// ErrNodeNotFound; a known node with no recorded history returns an empty
	// slice.
	NodeVersions(ctx context.Context, uniqueID string, limit int32, includeCode bool) ([]codeversion.VersionView, error)
	// CurrentNodeVersion returns the single version :CURRENT points at, as a
	// 0-or-1 slice — revert-safe where NodeVersions' newest-first is not. An
	// unknown node returns ErrNodeNotFound; a known node with no current
	// version returns an empty slice.
	CurrentNodeVersion(ctx context.Context, uniqueID string, includeCode bool) ([]codeversion.VersionView, error)
	// VersionsBySeq returns the two named versions of one node. An unknown
	// node, or a seq that node has no recorded version for, returns
	// ErrNodeNotFound.
	VersionsBySeq(ctx context.Context, uniqueID string, fromSeq, toSeq int64) (from, to codeversion.VersionView, err error)
	// Ancestors returns up to cap of the node's transitive upstreams within
	// depth hops, most-recently-changed first, each with its two most
	// relevant versions: the version :CURRENT points to (if any) and the
	// newest-by-promoted_at version other than that one — so a revert that
	// re-points :CURRENT at an older immutable version still yields the right
	// pair. Ranking, the since filter, and the cap are all applied against
	// this effective recency — max(newest version's promoted_at, the :CURRENT
	// edge's own promoted_at) — before any version body is fetched, so a wide
	// DAG never pays to load code for ancestors it is about to discard.
	Ancestors(ctx context.Context, uniqueID string, depth int32, since time.Time, cap int32) ([]codeversion.AncestorVersions, error)
	// UnitVersions walks a shared-code unit's chain, newest first. An unknown
	// unit_id returns ErrUnitNotFound; a known unit with no recorded history
	// returns an empty slice.
	UnitVersions(ctx context.Context, unitID string, limit int32) ([]codeversion.UnitVersionView, error)
	// UnitVersionsBatch is UnitVersions for many units in one round trip, each
	// capped independently at limit. Every requested id that resolves to at
	// least one version appears in the result map; an id with none is simply
	// absent (this batched form makes no known/unknown distinction — it exists
	// to serve the node-selector path, whose unit ids are already known to be
	// real).
	UnitVersionsBatch(ctx context.Context, unitIDs []string, limit int32) (map[string][]codeversion.UnitVersionView, error)
	// UnitsForNode returns the units the node's current version uses.
	UnitsForNode(ctx context.Context, uniqueID string) ([]string, error)
	// RunExecutions returns runs that executed the node, newest first,
	// optionally filtered to one operation ("" applies no filter). An unknown
	// node returns ErrNodeNotFound; a known node with no matching runs returns
	// an empty slice.
	RunExecutions(ctx context.Context, uniqueID string, limit int32, operation string) ([]codeversion.RunExecution, error)
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

// GetNodeVersions returns a node's version history, newest first (ordered by
// when each version's code was first promoted — a revert re-points at an
// existing version without changing its position in that ordering; is_current
// on each row marks what actually runs now), up to limit. limit <= 0 defaults
// to defaultQueryLimit; anything above maxQueryLimit is clamped to it.
// includeCode false keeps raw_code/compiled_code off the wire entirely.
func (s *CodeVersionQueryService) GetNodeVersions(ctx context.Context, uniqueID string, limit int32, includeCode bool) ([]codeversion.VersionView, error) {
	versions, err := s.reader.NodeVersions(ctx, uniqueID, clampQueryLimit(limit), includeCode)
	if err != nil {
		return nil, fmt.Errorf("CodeVersionQueryService.GetNodeVersions: %w", err)
	}
	return versions, nil
}

// GetCurrentNodeVersion returns only the version the node runs now, honoring
// includeCode. The 0-or-1 slice shape matches GetNodeVersions so the gRPC
// response is identical in form.
func (s *CodeVersionQueryService) GetCurrentNodeVersion(ctx context.Context, uniqueID string, includeCode bool) ([]codeversion.VersionView, error) {
	versions, err := s.reader.CurrentNodeVersion(ctx, uniqueID, includeCode)
	if err != nil {
		return nil, fmt.Errorf("CodeVersionQueryService.GetCurrentNodeVersion: %w", err)
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
	ancestors, err := s.reader.Ancestors(ctx, uniqueID, depth, since, upstreamAncestorCap)
	if err != nil {
		return nil, fmt.Errorf("CodeVersionQueryService.GetUpstreamChanges: %w", err)
	}
	if len(ancestors) == 0 {
		// Ancestors returns an empty slice both for an unknown node and for a
		// known node with no upstreams (or none surviving the since filter) —
		// it cannot tell those apart. NodeVersions can: it distinguishes an
		// unknown unique_id from a known one, so use it purely as an
		// existence check without paying for a second full ancestry query.
		// No code body is needed for an existence probe.
		if _, err := s.reader.NodeVersions(ctx, uniqueID, 1, false); err != nil {
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
			// Versions[0] is the version :CURRENT points to and Versions[1] is
			// the newest-by-promoted_at version other than that one — the
			// reader guarantees that pairing, not merely "the two newest by
			// promoted_at", so a revert that re-points :CURRENT at an older
			// immutable version still reports that reversion's actual
			// From→To rather than the two versions' original creation order.
			diff = renderDiff(a.UniqueID, a.Versions[1], a.Versions[0])
		}
		changes = append(changes, codeversion.UpstreamChange{
			UniqueID: a.UniqueID,
			Depth:    a.Depth,
			Diff:     diff,
		})
	}

	// The reader already ranked, since-filtered, and capped ancestors at the
	// id level before fetching any version body, so changes already holds at
	// most upstreamAncestorCap entries in most-recently-changed order; this
	// only guards against a reader returning more than it promised.
	if len(changes) > upstreamAncestorCap {
		changes = changes[:upstreamAncestorCap]
	}
	return changes, nil
}

// GetCodeUnitVersions returns a shared-code unit's version chain, newest
// first, up to limit. Pass unitID to query one unit directly: an unknown
// unit_id returns ErrUnitNotFound. Leave it empty and pass uniqueID to
// resolve the node's current units first, returning each of their chains
// concatenated in the order UnitsForNode returns them, fetched in one batched
// round trip; an unknown uniqueID returns ErrNodeNotFound — UnitsForNode
// alone cannot distinguish that from a known node with no current version, so
// an empty resolution falls back to NodeVersions purely as an existence
// check, the same pattern GetUpstreamChanges uses. limit <= 0 defaults to
// defaultQueryLimit; anything above maxQueryLimit is clamped to it.
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
	if len(unitIDs) == 0 {
		// No code body is needed for an existence probe.
		if _, err := s.reader.NodeVersions(ctx, uniqueID, 1, false); err != nil {
			return nil, fmt.Errorf("CodeVersionQueryService.GetCodeUnitVersions: %w", err)
		}
		return []codeversion.UnitVersionView{}, nil
	}

	byUnit, err := s.reader.UnitVersionsBatch(ctx, unitIDs, limit)
	if err != nil {
		return nil, fmt.Errorf("CodeVersionQueryService.GetCodeUnitVersions: %w", err)
	}
	all := make([]codeversion.UnitVersionView, 0, len(unitIDs))
	for _, id := range unitIDs {
		all = append(all, byUnit[id]...)
	}
	return all, nil
}

// GetNodeRunHistory returns runs that executed the node, newest first,
// optionally filtered to one operation ("" returns every operation). limit
// <= 0 defaults to defaultQueryLimit; anything above maxQueryLimit is
// clamped to it.
func (s *CodeVersionQueryService) GetNodeRunHistory(ctx context.Context, uniqueID string, limit int32, operation string) ([]codeversion.RunExecution, error) {
	runs, err := s.reader.RunExecutions(ctx, uniqueID, clampQueryLimit(limit), operation)
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
