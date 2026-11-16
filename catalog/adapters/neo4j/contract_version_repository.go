package neo4jinfra

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/catalog/domain/contractversion"
	"github.com/carolsimone/continuo/catalog/domain/repository"
)

// readContractStatesCypher returns, per node of the batch the graph holds, its
// contract watermark, the hash and counter of its current contract, and every
// promotion edge as a [contract hash, version] pair. collect skips the null an
// edgeless node's OPTIONAL MATCH yields, so such a node lists none.
const readContractStatesCypher = `
UNWIND $unique_ids AS uid
MATCH (n:Node {tenant_id: $tenant_id, unique_id: uid})
OPTIONAL MATCH (n)-[cur:CURRENT_CONTRACT]->(cv:ContractVersion {tenant_id: $tenant_id})
WITH uid, n, cv.contract_hash AS current_hash, cur.version AS current_version
OPTIONAL MATCH (n)-[e:CONTRACT_PROMOTED]->(ev:ContractVersion {tenant_id: $tenant_id})
RETURN uid                                             AS unique_id,
       coalesce(n.contract_promotion_seq, 0)           AS watermark,
       current_hash,
       coalesce(current_version, 0)                    AS current_version,
       collect(CASE WHEN e IS NULL THEN null ELSE [ev.contract_hash, e.version] END) AS promotions`

// createMissingContractNodesCypher creates every node of the promotion's
// contracts the tenant's graph does not hold yet, inactive, with the contract's
// service and the release's id; it has no retired_at, because it was never
// live. A node the graph holds is left as it is: the topology swap owns a
// node's properties and makes it live when it applies a release that carries
// it.
const createMissingContractNodesCypher = `
UNWIND $nodes AS c
MERGE (n:Node {tenant_id: $tenant_id, unique_id: c.unique_id})
ON CREATE SET n.service_name = c.service,
              n.release_id   = $release_id,
              n.active       = false`

// advanceContractWatermarkCypher sets each node's contract watermark to the
// larger of its value and the promotion's seq. Writing to the node takes its
// write lock, so every later guard of the transaction reads a value no
// concurrent writer can change underneath it.
const advanceContractWatermarkCypher = `
UNWIND $unique_ids AS uid
MATCH (n:Node {tenant_id: $tenant_id, unique_id: uid})
SET n.contract_promotion_seq = CASE
      WHEN n.contract_promotion_seq IS NULL OR n.contract_promotion_seq < $promotion_seq
      THEN $promotion_seq
      ELSE n.contract_promotion_seq END`

// writeContractVersionsCypher records each stored contract once: every
// property is set on creation only, so a revert, or the same contract promoted
// again, MERGEs onto the version that exists. first_release_id is the release
// whose promotion first stored the version.
const writeContractVersionsCypher = `
UNWIND $versions AS c
MERGE (v:ContractVersion {tenant_id: $tenant_id, unique_id: c.unique_id, contract_hash: c.contract_hash})
ON CREATE SET v.family           = c.family,
              v.service          = c.service,
              v.document         = c.document,
              v.first_release_id = c.first_release_id`

// recordContractPromotionsCypher draws one :CONTRACT_PROMOTED edge per change
// while the node's watermark is this promotion's seq and its pointer does not
// already name the edge's contract, so a plan made from a stale read never
// records the same change twice. It runs before moveContractPointerCypher, so
// the pointer it reads is the one before this write. A null property (no
// revert, no ontology) is not stored.
const recordContractPromotionsCypher = `
UNWIND $edges AS e
MATCH (n:Node {tenant_id: $tenant_id, unique_id: e.unique_id})
WHERE n.contract_promotion_seq = $promotion_seq
MATCH (v:ContractVersion {tenant_id: $tenant_id, unique_id: e.unique_id, contract_hash: e.contract_hash})
WHERE NOT (n)-[:CURRENT_CONTRACT]->(v)
CREATE (n)-[:CONTRACT_PROMOTED {
  version:            e.version,
  promotion_seq:      $promotion_seq,
  release_id:         $release_id,
  promoted_at:        $promoted_at,
  content_hash:       e.content_hash,
  continuo_contracts: $continuo_contracts,
  ontology_version:   $ontology_version,
  same_as_version:    e.same_as_version,
  source_path:        e.source_path
}]->(v)`

// moveContractPointerCypher points each changed node at its new contract while
// the node's watermark is this promotion's seq and its pointer names another
// contract. The comparison and the replacement are one statement, behind the
// watermark's write lock, so an older promotion can never delete a newer
// pointer and put a stale one in its place, and a stale plan never moves a
// pointer onto the contract it already names.
const moveContractPointerCypher = `
UNWIND $edges AS e
MATCH (n:Node {tenant_id: $tenant_id, unique_id: e.unique_id})
WHERE n.contract_promotion_seq = $promotion_seq
MATCH (v:ContractVersion {tenant_id: $tenant_id, unique_id: e.unique_id, contract_hash: e.contract_hash})
WHERE NOT (n)-[:CURRENT_CONTRACT]->(v)
OPTIONAL MATCH (n)-[old:CURRENT_CONTRACT]->(:ContractVersion {tenant_id: $tenant_id})
DELETE old
CREATE (n)-[:CURRENT_CONTRACT {
  version:       e.version,
  promotion_seq: $promotion_seq,
  release_id:    $release_id,
  promoted_at:   $promoted_at
}]->(v)`

// contractVersionRepository writes one tenant's contract history on the open
// transaction of a unit-of-work Write.
//
// A :ContractVersion is immutable: every property is set under ON CREATE, so
// the graph holds one per distinct (unique_id, contract_hash) ever promoted.
//
// Change detection and ordering come from ReadStates and contractversion.Plan,
// run inside the same write transaction as Write. The unit of work takes the
// tenant's export-gate lock before either, so a tenant's writes are
// serialized and the states a plan is made from cannot change before it is
// written. The Cypher guards are a second line: every node of a promotion
// advances its watermark contract_promotion_seq to the larger seq, and every
// edge and pointer write is guarded, inside the same statement, on that
// watermark being this promotion's seq and on the pointer not already naming
// the edge's contract. So an older promotion records its contracts but never
// a change, and a plan made from a stale read records no change twice.
type contractVersionRepository struct {
	run TenantRunner
}

var _ repository.ContractRepository = (*contractVersionRepository)(nil)

// ReadStates returns the contract state of each node of uniqueIDs the graph
// holds. It refuses a tenantID other than the transaction's.
func (r *contractVersionRepository) ReadStates(ctx context.Context, tenantID string, uniqueIDs []string) (map[string]contractversion.NodeState, error) {
	if err := r.run.requireTenant(tenantID); err != nil {
		return nil, err
	}
	states := make(map[string]contractversion.NodeState, len(uniqueIDs))
	if len(uniqueIDs) == 0 {
		return states, nil
	}
	res, err := r.run.Run(ctx, readContractStatesCypher, map[string]any{"unique_ids": uniqueIDs})
	if err != nil {
		return nil, fmt.Errorf("read node contract state: %w", err)
	}
	for res.Next(ctx) {
		rec := res.Record()
		uid, _ := recordString(rec, "unique_id")
		hash, _ := recordString(rec, "current_hash")
		st := contractversion.NodeState{
			UniqueID:           uid,
			Watermark:          toInt64(recordValue(rec, "watermark")),
			CurrentHash:        hash,
			CurrentVersion:     toInt64(recordValue(rec, "current_version")),
			FirstVersionByHash: map[string]int64{},
		}
		promotions, _ := recordValue(rec, "promotions").([]any)
		for _, p := range promotions {
			pair, _ := p.([]any)
			if len(pair) != 2 {
				continue
			}
			h, _ := pair[0].(string)
			version := toInt64(pair[1])
			st.MaxVersion = max(st.MaxVersion, version)
			if first, seen := st.FirstVersionByHash[h]; !seen || version < first {
				st.FirstVersionByHash[h] = version
			}
		}
		states[uid] = st
	}
	if err := res.Err(); err != nil {
		return nil, fmt.Errorf("iterate node contract state: %w", err)
	}
	return states, nil
}

// Write applies plan for promotion p on the open transaction:
//
//  1. Create, inactive, every node of plan.Versions the graph does not hold.
//  2. Advance every such node's contract watermark.
//  3. Record every stored contract the graph does not hold.
//  4. For every edge of plan.Edges whose node's watermark is p.PromotionSeq:
//     record the :CONTRACT_PROMOTED edge, then move :CURRENT_CONTRACT.
//
// It refuses a tenantID other than the transaction's.
func (r *contractVersionRepository) Write(
	ctx context.Context,
	tenantID string,
	p contractversion.Promotion,
	plan contractversion.WritePlan,
) (contractversion.WriteResult, error) {
	var out contractversion.WriteResult
	if err := r.run.requireTenant(tenantID); err != nil {
		return out, err
	}
	if len(plan.Versions) == 0 {
		return out, nil
	}

	versions := make([]map[string]any, 0, len(plan.Versions))
	nodes := make([]map[string]any, 0, len(plan.Versions))
	uniqueIDs := make([]string, 0, len(plan.Versions))
	seen := make(map[string]struct{}, len(plan.Versions))
	for _, v := range plan.Versions {
		document, err := canonicalJSON(v.Document)
		if err != nil {
			return out, fmt.Errorf("encode the stored contract of %s: %w", v.UniqueID, err)
		}
		versions = append(versions, map[string]any{
			"unique_id":        v.UniqueID,
			"contract_hash":    v.ContractHash,
			"family":           v.Family,
			"service":          v.Service,
			"document":         document,
			"first_release_id": v.FirstReleaseID,
		})
		if _, dup := seen[v.UniqueID]; !dup {
			seen[v.UniqueID] = struct{}{}
			uniqueIDs = append(uniqueIDs, v.UniqueID)
			nodes = append(nodes, map[string]any{"unique_id": v.UniqueID, "service": v.Service})
		}
	}

	common := map[string]any{
		"release_id":    p.ReleaseID,
		"promotion_seq": p.PromotionSeq,
		// The Neo4j Go driver names a time.Time's zone by its
		// Location().String(), and Neo4j rejects time.Local's "Local", so the
		// time is sent in UTC.
		"promoted_at":        p.PromotedAt.UTC(),
		"continuo_contracts": p.ContinuoContracts,
		"ontology_version":   nullIfEmpty(p.OntologyVersion),
	}

	if _, err := runCounting(ctx, r.run, createMissingContractNodesCypher,
		with(common, "nodes", nodes), "create the contract nodes the graph does not hold"); err != nil {
		return out, err
	}
	if _, err := runCounting(ctx, r.run, advanceContractWatermarkCypher,
		with(common, "unique_ids", uniqueIDs), "advance the :Node contract watermark"); err != nil {
		return out, err
	}
	written, err := runCounting(ctx, r.run, writeContractVersionsCypher,
		with(common, "versions", versions), "write :ContractVersion nodes")
	if err != nil {
		return out, err
	}
	out.VersionsCreated = written.NodesCreated()

	if len(plan.Edges) == 0 {
		return out, nil
	}
	edges := make([]map[string]any, 0, len(plan.Edges))
	for _, e := range plan.Edges {
		var sameAs any
		if e.SameAsVersion > 0 {
			sameAs = e.SameAsVersion
		}
		edges = append(edges, map[string]any{
			"unique_id":       e.UniqueID,
			"contract_hash":   e.ContractHash,
			"content_hash":    e.ContentHash,
			"source_path":     e.SourcePath,
			"version":         e.Version,
			"same_as_version": sameAs,
		})
	}
	// Count what the database did: a node whose watermark is past this
	// promotion is skipped by the guard.
	recorded, err := runCounting(ctx, r.run, recordContractPromotionsCypher,
		with(common, "edges", edges), "record :CONTRACT_PROMOTED edges")
	if err != nil {
		return out, err
	}
	out.EdgesCreated = recorded.RelationshipsCreated()
	moved, err := runCounting(ctx, r.run, moveContractPointerCypher,
		with(common, "edges", edges), "move :CURRENT_CONTRACT pointers")
	if err != nil {
		return out, err
	}
	out.PointersMoved = moved.RelationshipsCreated()
	return out, nil
}

// nullIfEmpty returns nil for an empty string, so the property is not stored.
func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
