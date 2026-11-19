// File: catalog/adapters/neo4j/contract_query_repository.go
//
// ContractQueryRepository implements queries.ContractReader by reading the
// :ContractVersion graph the contracts group's repository in
// contract_version_repository.go produces. It issues read-only Cypher
// exclusively.
package neo4jinfra

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/catalog/domain/contractversion"
	"github.com/carolsimone/continuo/catalog/service/queries"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// ContractQueryRepository implements queries.ContractReader against Neo4j.
// Every read runs in one read transaction bound to the tenant it is asked
// for, and reaches only that tenant's nodes.
//
// A node's history is its :CONTRACT_PROMOTED edges, ordered by the counter
// each carries, never by time: the counter is assigned at promotion and never
// goes backwards. The current version is the edge whose counter the node's
// :CURRENT_CONTRACT pointer carries, so after a revert the first edge to the
// same stored contract is not current. Current, History and ByVersion read
// the pointer through a live node whose contract watermark is at or above the
// live folder's: a retired node, or a node the newest promotion the folder
// recorded did not carry (the release dropped it), keeps its pointer as the
// record of its last contract, but none of its versions is current and it has
// no current contract, as the folder holds no file of it. Live instead reads
// the nodes a promotion's bundle named, by their contract watermark, whatever
// their active flag.
type ContractQueryRepository struct {
	client Neo4jClient
}

// Compile-time assertion that the adapter satisfies the read port.
var _ queries.ContractReader = (*ContractQueryRepository)(nil)

// NewContractQueryRepository constructs a ContractQueryRepository backed by
// the given Neo4j client.
func NewContractQueryRepository(client Neo4jClient) *ContractQueryRepository {
	return &ContractQueryRepository{client: client}
}

// liveFolderMarkCypher binds folder_seq to the promotion the tenant's live
// contracts folder was last recorded for, 0 before its first rewrite.
const liveFolderMarkCypher = `
OPTIONAL MATCH (folder:Meta {tenant_id: $tenant_id, key: '` + liveFolderNodeKey + `'})
WITH coalesce(folder.promotion_seq, 0) AS folder_seq
`

// currentContractWhere keeps a node n whose pointer is current: n is live and
// its contract watermark is at or above folder_seq, so the newest promotion
// the folder recorded carried its contract.
const currentContractWhere = `
WHERE coalesce(n.active, true) AND coalesce(n.contract_promotion_seq, 0) >= folder_seq`

// liveContractVersionCypher binds current_version to the counter of the
// tenant's :Node $uid's pointer when that pointer is current
// (currentContractWhere), or to null when the node is retired, was left out
// of the newest promotion the folder recorded, has no contract or does not
// exist. It is the head of every query that marks is_current.
const liveContractVersionCypher = liveFolderMarkCypher + `
OPTIONAL MATCH (n:Node {tenant_id: $tenant_id, unique_id: $uid})-[cur:CURRENT_CONTRACT]->(:ContractVersion {tenant_id: $tenant_id})
` + currentContractWhere + `
WITH cur.version AS current_version
`

// contractVersionColumns projects one promotion edge e of the node to the
// stored contract v. It assumes the query binds current_version as
// liveContractVersionCypher does. withDocument false keeps the stored
// document off the wire between Neo4j and this process.
func contractVersionColumns(withDocument bool) string {
	document := `v.document AS document,`
	if !withDocument {
		document = `null AS document,`
	}
	return `e.version AS version, v.contract_hash AS contract_hash, e.release_id AS release_id,
       e.promotion_seq AS promotion_seq, e.promoted_at AS promoted_at, e.content_hash AS content_hash,
       e.continuo_contracts AS continuo_contracts, e.ontology_version AS ontology_version,
       coalesce(e.same_as_version, 0) AS same_as_version, e.source_path AS source_path,
       ` + document + `
       (current_version IS NOT NULL AND e.version = current_version) AS is_current`
}

// History lists the node's promotion edges, newest first, up to limit (every
// edge when limit is 0 or below), without their documents.
func (r *ContractQueryRepository) History(ctx context.Context, tenantID, uniqueID string, limit int) ([]contractversion.VersionView, error) {
	query := liveContractVersionCypher + `
MATCH (:Node {tenant_id: $tenant_id, unique_id: $uid})-[e:CONTRACT_PROMOTED]->(v:ContractVersion {tenant_id: $tenant_id})
RETURN ` + contractVersionColumns(false) + `
ORDER BY version DESC`
	params := map[string]any{"uid": uniqueID}
	if limit > 0 {
		query += `
LIMIT $limit`
		params["limit"] = int64(limit)
	}
	views, err := inReadTx(ctx, r.client, tenantID, func(run TenantRunner) ([]contractversion.VersionView, error) {
		return collectContractVersions(ctx, run, query, params, uniqueID)
	})
	if err != nil {
		return nil, fmt.Errorf("ContractQueryRepository.History: %w", err)
	}
	return views, nil
}

// Current returns the promotion edge whose counter the node's current pointer
// (currentContractWhere) carries, with its document.
func (r *ContractQueryRepository) Current(ctx context.Context, tenantID, uniqueID string) (contractversion.VersionView, error) {
	query := liveFolderMarkCypher + `
MATCH (n:Node {tenant_id: $tenant_id, unique_id: $uid})-[cur:CURRENT_CONTRACT]->(v:ContractVersion {tenant_id: $tenant_id})
` + currentContractWhere + `
MATCH (n)-[e:CONTRACT_PROMOTED]->(v)
WHERE e.version = cur.version
WITH e, v, cur.version AS current_version
RETURN ` + contractVersionColumns(true)
	return r.one(ctx, tenantID, uniqueID, query, map[string]any{"uid": uniqueID}, "Current")
}

// ByVersion returns the node's promotion edge numbered version, with its
// document.
func (r *ContractQueryRepository) ByVersion(ctx context.Context, tenantID, uniqueID string, version int64) (contractversion.VersionView, error) {
	query := liveContractVersionCypher + `
MATCH (:Node {tenant_id: $tenant_id, unique_id: $uid})-[e:CONTRACT_PROMOTED]->(v:ContractVersion {tenant_id: $tenant_id})
WHERE e.version = $version
RETURN ` + contractVersionColumns(true)
	return r.one(ctx, tenantID, uniqueID, query, map[string]any{"uid": uniqueID, "version": version}, "ByVersion")
}

// one runs a query that matches at most one promotion edge and returns it, or
// contractversion.ErrNotFound when it matches none.
func (r *ContractQueryRepository) one(ctx context.Context, tenantID, uniqueID, query string, params map[string]any, op string) (contractversion.VersionView, error) {
	views, err := inReadTx(ctx, r.client, tenantID, func(run TenantRunner) ([]contractversion.VersionView, error) {
		return collectContractVersions(ctx, run, query, params, uniqueID)
	})
	if err != nil {
		return contractversion.VersionView{}, fmt.Errorf("ContractQueryRepository.%s: %w", op, err)
	}
	if len(views) == 0 {
		return contractversion.VersionView{}, fmt.Errorf("ContractQueryRepository.%s: node %s: %w", op, uniqueID, contractversion.ErrNotFound)
	}
	return views[0], nil
}

// liveContractsCypher returns the current contract of every node whose
// contract watermark is $promotion_seq, with the promotion edge that made it
// current and the node's declared schema (null for a node only the contracts
// group has written), sorted by unique id.
const liveContractsCypher = `
MATCH (n:Node {tenant_id: $tenant_id})-[cur:CURRENT_CONTRACT]->(v:ContractVersion {tenant_id: $tenant_id})
WHERE n.contract_promotion_seq = $promotion_seq
MATCH (n)-[e:CONTRACT_PROMOTED]->(v)
WHERE e.version = cur.version
RETURN n.unique_id AS unique_id, n.schema_name AS schema_name, e.version AS version, v.contract_hash AS contract_hash,
       v.document AS document, e.content_hash AS content_hash, e.release_id AS release_id, e.continuo_contracts AS continuo_contracts,
       e.source_path AS source_path, e.promotion_seq AS promotion_seq, e.promoted_at AS promoted_at
ORDER BY unique_id`

// latestContractPromotionCypher returns the largest contract watermark of
// the tenant's nodes, 0 when none has one.
const latestContractPromotionCypher = `
MATCH (n:Node {tenant_id: $tenant_id})
WHERE n.contract_promotion_seq IS NOT NULL
RETURN coalesce(max(n.contract_promotion_seq), 0) AS seq`

// Live returns the current contract of every node whose contract watermark
// is promotionSeq, active or not, sorted by unique id.
func (r *ContractQueryRepository) Live(ctx context.Context, tenantID string, promotionSeq int64) ([]contractversion.LiveContract, error) {
	live, err := inReadTx(ctx, r.client, tenantID, func(run TenantRunner) ([]contractversion.LiveContract, error) {
		res, err := run.Run(ctx, liveContractsCypher, map[string]any{"promotion_seq": promotionSeq})
		if err != nil {
			return nil, err
		}
		out := make([]contractversion.LiveContract, 0)
		for res.Next(ctx) {
			rec := res.Record()
			uid, _ := recordString(rec, "unique_id")
			text, _ := recordString(rec, "document")
			document, err := decodeDocument(text)
			if err != nil {
				return nil, fmt.Errorf("node %s: %w", uid, err)
			}
			hash, _ := recordString(rec, "contract_hash")
			content, _ := recordString(rec, "content_hash")
			release, _ := recordString(rec, "release_id")
			library, _ := recordString(rec, "continuo_contracts")
			source, _ := recordString(rec, "source_path")
			schema, _ := recordString(rec, "schema_name")
			out = append(out, contractversion.LiveContract{
				UniqueID:     uid,
				Version:      toInt64(recordValue(rec, "version")),
				ContractHash: hash,
				Document:     document,
				Provenance: contractversion.Provenance{
					ContractHash:      hash,
					ContentHash:       content,
					ReleaseID:         release,
					ContinuoContracts: library,
					SourcePath:        source,
					PromotionSeq:      toInt64(recordValue(rec, "promotion_seq")),
					PromotedAt:        recordTime(rec, "promoted_at"),
				},
				PhysicalSchema: schema,
			})
		}
		return out, res.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("ContractQueryRepository.Live: %w", err)
	}
	return live, nil
}

// LatestPromotionSeq returns the largest contract watermark of the tenant's
// nodes, 0 when no node has one.
func (r *ContractQueryRepository) LatestPromotionSeq(ctx context.Context, tenantID string) (int64, error) {
	seq, err := inReadTx(ctx, r.client, tenantID, func(run TenantRunner) (int64, error) {
		res, err := run.Run(ctx, latestContractPromotionCypher, nil)
		if err != nil {
			return 0, err
		}
		rec, err := res.Single(ctx)
		if err != nil {
			return 0, err
		}
		return toInt64(recordValue(rec, "seq")), nil
	})
	if err != nil {
		return 0, fmt.Errorf("ContractQueryRepository.LatestPromotionSeq: %w", err)
	}
	return seq, nil
}

// collectContractVersions runs a query projected by contractVersionColumns
// and reads every row.
func collectContractVersions(ctx context.Context, run TenantRunner, query string, params map[string]any, uniqueID string) ([]contractversion.VersionView, error) {
	res, err := run.Run(ctx, query, params)
	if err != nil {
		return nil, err
	}
	views := make([]contractversion.VersionView, 0)
	for res.Next(ctx) {
		view, err := contractVersionFromRecord(res.Record(), uniqueID)
		if err != nil {
			return nil, err
		}
		views = append(views, view)
	}
	return views, res.Err()
}

// contractVersionFromRecord reads one row projected by contractVersionColumns.
// A null document is left nil.
func contractVersionFromRecord(rec *neo4j.Record, uniqueID string) (contractversion.VersionView, error) {
	view := contractversion.VersionView{
		UniqueID:      uniqueID,
		Version:       toInt64(recordValue(rec, "version")),
		PromotionSeq:  toInt64(recordValue(rec, "promotion_seq")),
		PromotedAt:    recordTime(rec, "promoted_at"),
		SameAsVersion: toInt64(recordValue(rec, "same_as_version")),
	}
	view.ContractHash, _ = recordString(rec, "contract_hash")
	view.ReleaseID, _ = recordString(rec, "release_id")
	view.ContentHash, _ = recordString(rec, "content_hash")
	view.ContinuoContracts, _ = recordString(rec, "continuo_contracts")
	view.OntologyVersion, _ = recordString(rec, "ontology_version")
	view.SourcePath, _ = recordString(rec, "source_path")
	view.IsCurrent = recordBool(rec, "is_current")
	if text, ok := recordString(rec, "document"); ok {
		document, err := decodeDocument(text)
		if err != nil {
			return contractversion.VersionView{}, fmt.Errorf("node %s version %d: %w", uniqueID, view.Version, err)
		}
		view.Document = document
	}
	return view, nil
}
