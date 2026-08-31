package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/remediation/domain/failure"
	"github.com/carolsimone/continuo/remediation/service/handlers"
)

// rejectedPayload mirrors the release.rejected:v1 wire shape produced by
// release-controller. Only the fields the classifier needs are decoded.
type rejectedPayload struct {
	ReleaseID string `json:"release_id"`
	Stage     string `json:"stage"`  // "compile" | "seed_build" | "validation"; absent in older payloads and in the stage-less duplicate_table rejection
	Reason    string `json:"reason"` // "compile_failed" | "seed_build_failed" | "validation_failed" | "parse_rehearsal_failed" | "artifact_upload_failed" | "duplicate_table"
	Repo      string `json:"repo"`
	CommitSHA string `json:"commit_sha"`
	// RemediationRound is set by release-controller on a
	// remediation.retry_requested:v1 replay of this payload — a human's "try
	// again" on the release. Absent (and thus zero) on the original
	// release.rejected:v1 rejection, which is round 1.
	RemediationRound int `json:"remediation_round"`
	// CodeBundleURI locates the rejected release's code-bundle document,
	// stamped by release-controller. Absent (and thus empty) for a payload
	// from before the field existed or for a rejection with no bundle.
	CodeBundleURI string `json:"code_bundle_uri"`
	// Shadow is true when the rejected release was a shadow release posted
	// by agent-remediation to verify a proposed fix, rather than a real
	// release. Always present on current release-controller payloads; absent
	// (and thus false) on payloads from before the field existed.
	Shadow  bool `json:"shadow"`
	PerNode []struct {
		NodeID               string `json:"node_id"`
		Status               string `json:"status"`
		DBTLogURI            string `json:"dbt_log_uri"`
		RunResultsURI        string `json:"run_results_uri"`
		CandidateArtifactURI string `json:"candidate_artifact_uri"`
		// RelationID is the contested physical relation for a
		// duplicate-relation rejection, distinct from NodeID (the target
		// claimant's own unique_id) — the two differ whenever the target
		// carries an alias. Empty for every other reason.
		RelationID string `json:"relation_id"`
		// FilePath and Service carry the source location from the candidate
		// topology, set by release-controller on validation, seed_build, and
		// duplicate_table rejections. When present, the remediation agent can
		// locate the source file without querying GetNodeLocation, which only
		// holds promoted topology.
		FilePath string `json:"file_path"`
		Service  string `json:"service"`
		// NodeType is the failing node's kind (dbt-model, dbt-seed,
		// dbt-snapshot, python-model, or python-csv), set by
		// release-controller on validation and duplicate-relation
		// rejections. It travels onto the remediation trigger, where it
		// tells a python target apart from a dbt one without a topology
		// lookup of its own: a python validation failure selects the fixer
		// that repairs the contract yaml declaring the node, and a python
		// duplicate-relation failure is skipped.
		NodeType string `json:"node_type"`
		// OtherService and OtherFilePath locate the competing node that also
		// produces the contested relation (RelationID), set on duplicate-relation
		// rejections so the agent can name the relation's other producer in the
		// rename prompt. The path is carried because two nodes in the SAME
		// service can collide, in which case the service name alone identifies
		// nothing.
		OtherService  string `json:"other_service"`
		OtherFilePath string `json:"other_file_path"`
		// ChangedAncestors are the node's changed transitive ancestors, stamped
		// by release-controller from the candidate topology, each with the file
		// path and service THAT topology declares for it — the location an
		// upstream fix must edit, which for a node this release renamed or moved
		// is not where the promoted graph places it.
		ChangedAncestors []changedAncestorPayload `json:"changed_ancestors"`
	} `json:"per_node"`
}

// changedAncestorPayload is one entry of a per_node entry's changed_ancestors
// array: the changed upstream's id plus the file path and service the rejected
// release's candidate topology declares for it.
type changedAncestorPayload struct {
	NodeID   string `json:"node_id"`
	FilePath string `json:"file_path"`
	Service  string `json:"service"`
}

// changedAncestors projects the decoded changed ancestors onto the domain
// evidence, preserving the payload's order (release-controller sorts them by
// id).
func changedAncestors(in []changedAncestorPayload) []failure.ChangedAncestor {
	if len(in) == 0 {
		return nil
	}
	out := make([]failure.ChangedAncestor, 0, len(in))
	for _, a := range in {
		out = append(out, failure.ChangedAncestor{NodeID: a.NodeID, FilePath: a.FilePath, Service: a.Service})
	}
	return out
}

// sourceFromPayload resolves the remediation Source from a release.rejected
// payload. It prefers the explicit stage field and falls back to the reason
// field, which covers older payloads with no stage and the stage-less
// duplicate_table rejection. The bool is false when the rejection is not
// remediable — parse_failed, unbuildable_cross_service_upstream, or an unknown
// future stage; the caller then produces no evidence rather than misrouting it.
func sourceFromPayload(stage, reason string) (failure.Source, bool) {
	switch stage {
	case "compile":
		return failure.SourceCompile, true
	case "seed_build":
		return failure.SourceSeed, true
	case "validation":
		return failure.SourceValidation, true
	case "":
		switch reason {
		case "compile_failed":
			return failure.SourceCompile, true
		case "seed_build_failed":
			return failure.SourceSeed, true
		case "validation_failed":
			return failure.SourceValidation, true
		case "duplicate_table":
			return failure.SourceDuplicateTable, true
		}
	}
	return "", false
}

// evidenceFromRejected translates a release.rejected:v1 payload into one
// FailureEvidence per failed node. Nodes with status != "failed" are skipped.
// The Source field is derived from the payload's stage field; when stage is
// absent, the reason field is used as a fallback. FilePath and Service carry
// whatever the rejection payload set directly (populated by release-controller
// for validation, seed_build, and duplicate_table); for compile failures,
// which have none, the handler extracts FilePath from the dbt log after the
// log is fetched.
func evidenceFromRejected(raw []byte) ([]failure.FailureEvidence, error) {
	var p rejectedPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("unmarshal release.rejected payload: %w", err)
	}

	// Parse-export-leg rejections are not model failures: a rehearsal miss is
	// a project *property* (env_var() at parse time / partial parse disabled)
	// and an upload failure is continuo-internal. Neither is fixable by a
	// model change, so no heal evidence is produced for them.
	if p.Reason == "parse_rehearsal_failed" || p.Reason == "artifact_upload_failed" {
		return nil, nil
	}

	src, ok := sourceFromPayload(p.Stage, p.Reason)
	if !ok {
		// Not a remediable pipeline-leg rejection (e.g. a parse-phase failure):
		// produce no evidence rather than misrouting it onto a leg's path.
		return nil, nil
	}

	out := make([]failure.FailureEvidence, 0, len(p.PerNode))
	for _, n := range p.PerNode {
		if n.Status != "failed" {
			continue
		}
		out = append(out, failure.FailureEvidence{
			Source:               src,
			ReleaseID:            p.ReleaseID,
			RemediationRound:     p.RemediationRound,
			NodeID:               n.NodeID,
			RelationID:           n.RelationID,
			DBTLogURI:            n.DBTLogURI,
			RunResultsURI:        n.RunResultsURI,
			CandidateArtifactURI: n.CandidateArtifactURI,
			FilePath:             n.FilePath,
			Service:              n.Service,
			NodeType:             n.NodeType,
			OtherService:         n.OtherService,
			OtherFilePath:        n.OtherFilePath,
			Repo:                 p.Repo,
			CommitSHA:            p.CommitSHA,
			CodeBundleURI:        p.CodeBundleURI,
			Shadow:               p.Shadow,
			ChangedAncestors:     changedAncestors(n.ChangedAncestors),
		})
	}
	return out, nil
}

// classifyRejectionMessages builds a StreamConsumer over the given stream and
// consumer group that decodes each message with the release.rejected:v1
// payload shape and classifies its whole failed-node set via one call to
// handlers.ClassifyRejection, so one rejected release yields one batched
// remediation.requested:v2 trigger. release.rejected:v1 and its per-round
// retry replay on remediation.retry_requested:v1 share this exact shape (the
// retry payload adds only the top-level remediation_round field), so one
// handler serves both consumers. The consumer group is created idempotently
// by StreamConsumer.Start; call Start(ctx) in a goroutine to begin consuming.
func classifyRejectionMessages(rc *goredis.Client, stream, group string, deps handlers.Deps, logger *slog.Logger) *pkgredis.StreamConsumer {
	handler := func(ctx context.Context, msg goredis.XMessage) error {
		raw, ok := msg.Values["payload"].(string)
		if !ok {
			logger.Error(stream+" missing payload — discarding", "message_id", msg.ID)
			return nil // permanent: ACK by returning nil so the message is not left in the PEL
		}
		evs, err := evidenceFromRejected([]byte(raw))
		if err != nil {
			logger.Error(stream+" decode failure — discarding", "message_id", msg.ID, "error", err)
			return nil // permanent: malformed payload cannot be retried
		}
		if err := handlers.ClassifyRejection(ctx, deps, evs); err != nil {
			return err // transient: do not ACK; allow redelivery via PEL sweep
		}
		return nil
	}
	return pkgredis.NewStreamConsumer(rc, stream, group, handler, logger)
}

// NewReleaseRejectedConsumer constructs a StreamConsumer that reads
// release.rejected:v1 and classifies its failed nodes via one call to
// handlers.ClassifyRejection, emitting one batched trigger for the release.
// The consumer group is created idempotently by StreamConsumer.Start; call
// Start(ctx) in a goroutine to begin consuming.
func NewReleaseRejectedConsumer(rc *goredis.Client, deps handlers.Deps, logger *slog.Logger) *pkgredis.StreamConsumer {
	return classifyRejectionMessages(rc, streams.ReleaseRejectedV1, streams.RemediationReleaseRejected, deps, logger)
}
