package events

import (
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ReleasePromotedSchemaVersion is the schema_version of release.promoted:v2 entries.
const ReleasePromotedSchemaVersion = 1

// ReleasePromoted is the payload of a release.promoted:v2 entry: a topology
// becoming the live one, numbered by PromotionSeq. release-controller allocates
// the seq in the transaction that moves current_prod, or when it announces a
// topology without moving it; a consumer applies an entry only when its seq is
// higher than the last one it applied. The topology is the artifact at
// TopologyURI, verified against TopologySHA256. ChangedNodeIDs lists the
// non-test nodes whose content changed against the previous prod topology; it
// is informational and empty when Bootstrap is set.
type ReleasePromoted struct {
	ReleaseID       string    `json:"release_id"`
	PromotedAt      time.Time `json:"promoted_at"`
	PromotionSeq    int64     `json:"promotion_seq"`
	TopologyURI     string    `json:"topology_uri"`
	TopologySHA256  string    `json:"topology_sha256"`
	ChangedNodeIDs  []string  `json:"changed_node_ids"`
	CandidateSchema string    `json:"candidate_schema"`
	CodeBundleURI   string    `json:"code_bundle_uri"`
	Repo            string    `json:"repo"`
	CommitSHA       string    `json:"commit_sha"`
	Bootstrap       bool      `json:"bootstrap"`
}

// ReleasePromotedEventID derives the event id of the release.promoted:v2 entry
// with seq promotionSeq. Every promotion and announcement takes a fresh seq, so
// the id names one announcement; re-publishing the same outbox row repeats it.
func ReleasePromotedEventID(tenantID string, promotionSeq int64) string {
	name := strings.Join([]string{"release.promoted", tenantID, strconv.FormatInt(promotionSeq, 10)}, "|")
	return uuid.NewSHA1(EventIDNamespace, []byte(name)).String()
}

// ReleasePromotedFields renders p as the Redis fields of one release.promoted:v2
// entry. promoted_at is written in UTC and changed_node_ids is always a list.
func ReleasePromotedFields(tenantID, producer string, occurredAt time.Time, p ReleasePromoted) (map[string]any, error) {
	if p.ChangedNodeIDs == nil {
		p.ChangedNodeIDs = []string{}
	}
	p.PromotedAt = p.PromotedAt.UTC()
	env := Envelope{
		EventID:       ReleasePromotedEventID(tenantID, p.PromotionSeq),
		TenantID:      tenantID,
		OccurredAt:    occurredAt,
		Producer:      producer,
		SchemaVersion: ReleasePromotedSchemaVersion,
	}
	return env.Fields(p)
}
