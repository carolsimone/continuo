package handlers

import (
	"encoding/json"
	"log/slog"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/carolsimone/continuo/catalog/domain/codeversion"
	domainModel "github.com/carolsimone/continuo/catalog/domain/model"
	"github.com/carolsimone/continuo/catalog/domain/topology"
	"github.com/carolsimone/continuo/pkg/codebundle"
	"github.com/carolsimone/continuo/pkg/sanitize"
)

// MaxCompiledCodeBytes bounds the compiled code stored on a version node.
// Compiled output is the unbounded half of a node's text — macros expand and
// CTEs inline — while raw source is human-written and stays small. Above the
// limit the text is stored truncated and flagged, so a pathological node costs
// one version's fidelity instead of bloating the graph store.
const MaxCompiledCodeBytes = 256 * 1024

// VersionsBatchSize is how many bundle nodes one write transaction of the
// versions group carries. A baseline release ingests the whole estate at once,
// and one unbounded transaction holding every node's source text would
// dominate the server's heap.
const VersionsBatchSize = 100

// planVersionWrite turns a release's code bundle, the nodes its topology swap
// writes and its promotion into a version-write request. It performs no
// input/output and makes no write decision: every bundle node is carried
// through, because whether a node needs a version is a question only the graph
// can answer.
func planVersionWrite(
	in domainModel.PromoteReleaseInput,
	b codebundle.Bundle,
	nodesOfRelease []topology.ReleasePromotedTopologyNode,
	logger *slog.Logger,
) codeversion.WriteInput {
	// changed_node_ids qualifies provenance; it never gates a write.
	changed := make(map[string]bool, len(in.ChangedNodeIDs))
	for _, id := range in.ChangedNodeIDs {
		changed[id] = true
	}

	uniqueIDs := make([]string, 0, len(b.Nodes))
	for id := range b.Nodes {
		uniqueIDs = append(uniqueIDs, id)
	}
	sort.Strings(uniqueIDs) // deterministic batching and logs

	nodes := make([]codeversion.NodeVersion, 0, len(uniqueIDs))
	usedUnits := make(map[string]struct{})
	for _, uniqueID := range uniqueIDs {
		n := b.Nodes[uniqueID]

		compiled, truncated := truncateCompiled(sanitize.Text(n.CompiledCode))
		if truncated {
			logger.Warn("code bundle: compiled_code truncated at ingestion",
				"release_id", in.ReleaseID,
				"unique_id", uniqueID,
				"original_bytes", len(n.CompiledCode),
				"limit_bytes", MaxCompiledCodeBytes)
		}

		refs := closureRefs(n.CodeUnitIDs, b.SharedCode)
		for _, r := range refs {
			usedUnits[r.UnitID] = struct{}{}
		}

		nodes = append(nodes, codeversion.NodeVersion{
			UniqueID:          uniqueID,
			ContentHash:       n.ContentHash,
			SourceHash:        n.SourceHash,
			SharedCodeHash:    n.SharedCodeHash,
			ConfigHash:        n.ConfigHash,
			Runtime:           n.Runtime,
			RawCode:           sanitize.Text(n.RawCode),
			CompiledCode:      compiled,
			CompiledTruncated: truncated,
			ConfigJSON:        canonicalJSON(n.Config),
			// A bundle node outside changed_node_ids is carried by a release
			// that did not author it, so its commit and release stamps are
			// approximate; a bootstrap release authored none of its code.
			Healed:   in.Bootstrap || !changed[uniqueID],
			UnitRefs: refs,
		})
	}

	unitIDs := make([]string, 0, len(usedUnits))
	for id := range usedUnits {
		unitIDs = append(unitIDs, id)
	}
	sort.Strings(unitIDs)

	units := make([]codeversion.CodeUnitVersion, 0, len(unitIDs))
	for _, id := range unitIDs {
		u := b.SharedCode[id]
		units = append(units, codeversion.CodeUnitVersion{
			UnitID:   id,
			Checksum: u.Checksum,
			Source:   sanitize.Text(u.Source),
		})
	}

	topo := make(map[string]topology.ReleasePromotedTopologyNode, len(nodesOfRelease))
	for _, n := range nodesOfRelease {
		topo[n.UniqueID] = n
	}

	return codeversion.WriteInput{
		ReleaseID:    in.ReleaseID,
		PromotionSeq: in.PromotionSeq,
		Repo:         in.Repo,
		CommitSHA:    in.CommitSHA,
		PromotedAt:   in.PromotedAt,
		Nodes:        nodes,
		Units:        units,
		Topology:     topo,
	}
}

// versionBatches splits a version-write request into batches of at most size
// nodes, in the request's order. Each batch carries the request's promotion,
// and the shared-code units and topology entries of its own nodes only. An
// empty request yields no batch.
func versionBatches(in codeversion.WriteInput, size int) []codeversion.WriteInput {
	var batches []codeversion.WriteInput
	for start := 0; start < len(in.Nodes); start += size {
		end := min(start+size, len(in.Nodes))
		batch := in
		batch.Nodes = in.Nodes[start:end]
		batch.Topology = make(map[string]topology.ReleasePromotedTopologyNode, end-start)
		used := map[string]struct{}{}
		for _, n := range batch.Nodes {
			if entry, ok := in.Topology[n.UniqueID]; ok {
				batch.Topology[n.UniqueID] = entry
			}
			for _, ref := range n.UnitRefs {
				used[ref.UnitID] = struct{}{}
			}
		}
		batch.Units = nil
		for _, u := range in.Units {
			if _, ok := used[u.UnitID]; ok {
				batch.Units = append(batch.Units, u)
			}
		}
		batches = append(batches, batch)
	}
	return batches
}

// closureRefs expands a node's direct shared-code dependencies into the full
// transitive set in scope, so the recorded edges name every unit whose source
// folds into the node's shared-code hash. An id absent from the map is skipped:
// the bundle exports only the units its nodes reach, so a missing one is a
// producer defect, not a reason to drop the whole version. The seen set also
// makes a dependency cycle terminate.
func closureRefs(direct []string, shared map[string]codebundle.SharedCodeUnit) []codeversion.UnitRef {
	seen := make(map[string]struct{}, len(direct))
	queue := append([]string(nil), direct...)
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if _, done := seen[id]; done {
			continue
		}
		unit, ok := shared[id]
		if !ok {
			continue
		}
		seen[id] = struct{}{}
		queue = append(queue, unit.DependsOn...)
	}

	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	refs := make([]codeversion.UnitRef, 0, len(ids))
	for _, id := range ids {
		refs = append(refs, codeversion.UnitRef{UnitID: id, Checksum: shared[id].Checksum})
	}
	return refs
}

// truncateCompiled cuts code down to the ingestion size guard on a rune
// boundary, so the stored text stays valid UTF-8 and Neo4j accepts it.
func truncateCompiled(code string) (string, bool) {
	if len(code) <= MaxCompiledCodeBytes {
		return code, false
	}
	cut := code[:MaxCompiledCodeBytes]
	for len(cut) > 0 && !utf8.ValidString(cut) {
		cut = cut[:len(cut)-1]
	}
	return cut, true
}

// canonicalJSON encodes a node's resolved config with sorted keys (Go's JSON
// encoder sorts map keys), so two equal configs always produce the same stored
// string. A config that cannot be encoded stores as an empty object rather than
// failing the whole release's ingestion.
func canonicalJSON(cfg map[string]any) string {
	if len(cfg) == 0 {
		return "{}"
	}
	out, err := json.Marshal(cfg)
	if err != nil {
		return "{}"
	}
	return strings.TrimSpace(string(out))
}
