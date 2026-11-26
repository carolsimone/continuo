package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/remediation/domain/failure"
	"github.com/carolsimone/continuo/remediation/service/handlers"
)

// rejectedPayload mirrors the release.rejected:v1 wire shape produced by
// release-controller. Only the fields the classifier needs are decoded.
type rejectedPayload struct {
	ReleaseID string `json:"release_id"`
	Stage     string `json:"stage"`  // "parse" | "compile" | "seed_build" | "validation"; absent in the stage-less duplicate_table rejection
	Reason    string `json:"reason"` // one of the reject_reason vocabulary values (pkg/domain/model.RejectReason)
	Repo      string `json:"repo"`
	CommitSHA string `json:"commit_sha"`
	// CodeBundleURI locates the rejected release's code-bundle document,
	// stamped by release-controller. Empty when the rejection carries no
	// bundle: a compile- or parse-stage rejection precedes the parse that
	// produces one.
	CodeBundleURI string `json:"code_bundle_uri"`
	// Shadow marks a rejection emitted for a fix's release check.
	// release-controller never sets it — a release check's failure emits no
	// release event at all — so it appears only on a message another producer
	// left in the backlog. The field exists solely so such a message is
	// dropped (see evidenceFromRejected) rather than misclassified.
	Shadow  bool `json:"shadow"`
	PerNode []struct {
		NodeID               string `json:"node_id"`
		Status               string `json:"status"`
		DBTLogURI            string `json:"dbt_log_uri"`
		RunResultsURI        string `json:"run_results_uri"`
		CandidateArtifactURI string `json:"candidate_artifact_uri"`
		// ValidationOp is how the validation leg prepared the node, a value
		// of the validation_op vocabulary (pkg/domain/model.ValidationOp).
		// It decodes as a string, so a value this build does not declare
		// still decodes. Empty on a non-validation rejection and from a
		// producer that does not send it.
		ValidationOp string `json:"validation_op"`
		// RelationID is the contested physical relation for a
		// duplicate-relation rejection, distinct from NodeID (the target
		// claimant's own unique_id) — the two differ whenever the target
		// carries an alias. Empty for every other reason.
		RelationID string `json:"relation_id"`
		// FilePath and Project carry the source location from the candidate
		// topology, set by release-controller on validation, seed_build, and
		// duplicate_table rejections. When present, the remediation agent can
		// locate the source file without querying GetNodeLocation, which only
		// holds promoted topology.
		FilePath string `json:"file_path"`
		Project  string `json:"project"`
		// NodeType is the failing node's kind (dbt-model, dbt-seed,
		// dbt-snapshot, python-node, python-csv, or python-api), set by
		// release-controller on validation and duplicate-relation
		// rejections. It travels onto the remediation trigger, where it
		// tells a python target apart from a dbt one without a topology
		// lookup of its own: a python validation failure selects the fixer
		// that repairs the contract yaml declaring the node, and a python
		// duplicate-relation failure is skipped.
		NodeType string `json:"node_type"`
		// OtherProject and OtherFilePath locate the competing node that also
		// produces the contested relation (RelationID), set on duplicate-relation
		// rejections so the agent can name the relation's other producer in the
		// rename prompt. The path is carried because two nodes in the SAME
		// project can collide, in which case the project name alone identifies
		// nothing.
		OtherProject  string `json:"other_project"`
		OtherFilePath string `json:"other_file_path"`
		// Kind and Detail are set on a parse-stage rejection: the contract kind
		// topology-controller assigned the node and its parser's own error text,
		// carried inline because no Job ran and so no log exists.
		Kind   string `json:"kind"`
		Detail string `json:"detail"`
		// ChangedAncestors are the node's changed transitive ancestors, stamped
		// by release-controller from the candidate topology, each with the file
		// path and project THAT topology declares for it — the location an
		// upstream fix must edit, which for a node this release renamed or moved
		// is not where the promoted graph places it.
		ChangedAncestors []changedAncestorPayload `json:"changed_ancestors"`
	} `json:"per_node"`
}

// changedAncestorPayload is one entry of a per_node entry's changed_ancestors
// array: the changed upstream's id plus the file path and project the rejected
// release's candidate topology declares for it and its minimum upstream hop
// distance from the failing node.
type changedAncestorPayload struct {
	NodeID   string `json:"node_id"`
	FilePath string `json:"file_path"`
	Project  string `json:"project"`
	Depth    int    `json:"depth"`
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
		out = append(out, failure.ChangedAncestor{NodeID: a.NodeID, FilePath: a.FilePath, Project: a.Project, Depth: a.Depth})
	}
	return out
}

// sourceFromPayload resolves the remediation Source from a release.rejected
// payload. It prefers the explicit stage field and falls back to the reason
// field, which covers the stage-less duplicate_table rejection. The bool is
// false when the rejection is not remediable — a stage this build does not
// know, or a stage-less reason that names no release check leg; the caller then
// produces no evidence rather than misrouting it.
func sourceFromPayload(stage, reason string) (pkg_model.RemediationSource, bool) {
	switch stage {
	case "parse":
		return pkg_model.RemediationSourceParse, true
	case "compile":
		return pkg_model.RemediationSourceCompile, true
	case "seed_build":
		return pkg_model.RemediationSourceSeedBuild, true
	case "validation":
		return pkg_model.RemediationSourceValidation, true
	case "":
		switch reason {
		case "compile_failed":
			return pkg_model.RemediationSourceCompile, true
		case "seed_build_failed":
			return pkg_model.RemediationSourceSeedBuild, true
		case "validation_failed":
			return pkg_model.RemediationSourceValidation, true
		case "duplicate_table":
			return pkg_model.RemediationSourceDuplicateTable, true
		}
	}
	return "", false
}

// evidenceFromRejected translates a release.rejected:v1 payload into one
// FailureEvidence per failed node, none of which names a remediation round: 0
// is round 1, and a retry stamps its own round on the evidence. Nodes with
// status != "failed" are skipped, and so is a node validation copied from its
// production table: no change to the release's code fixes a copy of
// production. A rejection whose reason the contract declares and marks not
// healable yields no evidence at all.
// The Source field is derived from the payload's stage field; when stage is
// absent, the reason field is used as a fallback. FilePath and Project carry
// whatever the rejection payload set directly (populated by release-controller
// for validation, seed_build, duplicate_table, and parse); for compile
// failures, which have none, the handler extracts FilePath from the dbt log
// after the log is fetched.
func evidenceFromRejected(raw []byte) ([]failure.FailureEvidence, error) {
	var p rejectedPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("unmarshal release.rejected payload: %w", err)
	}

	// A rejection flagged shadow is one emitted for a fix's release check.
	// Classifying it would mint a remediation trigger keyed on a fix's
	// release check id — which agent-remediation cannot resolve for an image tag through
	// /releases/{id}, leaving a bogus attempt that retries forever.
	// release-controller never flags a payload shadow, so dropping these
	// costs nothing.
	if p.Shadow {
		return nil, nil
	}

	// A reason the contract declares and marks not healable is not a model
	// failure a heal proposal could fix: a rehearsal miss is a project
	// property, an upload failure or an internal_error is continuo's own, an
	// invalid_artifact is a manifest or python contract the release ships
	// unreadable or invalid (or naming another project, or declaring an invalid
	// node setting), and a failed copy of an unchanged upstream is settled by a
	// run of that node. A reason that is empty or that this build does not
	// declare (an older producer, a retry replay) is not dropped here; its
	// stage decides.
	if r := pkg_model.RejectReason(p.Reason); r.IsValid() && !r.Healable() {
		return nil, nil
	}

	src, ok := sourceFromPayload(p.Stage, p.Reason)
	if !ok {
		// Not a remediable release-check-leg rejection (a stage this build does not
		// know, or a stage-less reason that names no leg): produce no evidence
		// rather than misrouting it onto a leg's path.
		return nil, nil
	}

	out := make([]failure.FailureEvidence, 0, len(p.PerNode))
	for _, n := range p.PerNode {
		if n.Status != "failed" || pkg_model.ValidationOp(n.ValidationOp) == pkg_model.ValidationOpCloneFromProd {
			continue
		}
		out = append(out, failure.FailureEvidence{
			Source:               src,
			ReleaseID:            p.ReleaseID,
			NodeID:               n.NodeID,
			RelationID:           n.RelationID,
			DBTLogURI:            n.DBTLogURI,
			RunResultsURI:        n.RunResultsURI,
			CandidateArtifactURI: n.CandidateArtifactURI,
			FilePath:             n.FilePath,
			Project:              n.Project,
			NodeType:             n.NodeType,
			OtherProject:         n.OtherProject,
			OtherFilePath:        n.OtherFilePath,
			Repo:                 p.Repo,
			CommitSHA:            p.CommitSHA,
			CodeBundleURI:        p.CodeBundleURI,
			ParseKind:            pkg_model.ParseFailureKind(n.Kind),
			Detail:               n.Detail,
			ChangedAncestors:     changedAncestors(n.ChangedAncestors),
		})
	}
	return out, nil
}

// classifyRejectionBody classifies the failed-node set of one rejection with
// one call to handlers.ClassifyRejection, so one rejected release yields one
// batched heal.triggered:v1 trigger. A rejection that cannot be decoded
// is a permanent failure: the error wraps events.ErrPermanent, so the consumer
// dead-letters it. A rejection that yields no failed node to classify (a reason
// the contract marks not healable, a shadow rejection, a stage this build does
// not know, or only copies of production) is acknowledged with one DEBUG line
// naming its release, stage and reason. round, when above 0, is the remediation
// round the evidence is stamped with. Any error from classification is
// returned as is, so the consumer redelivers it.
func classifyRejectionBody(ctx context.Context, deps handlers.Deps, logger *slog.Logger, stream, tenantID string, rejection []byte, round int) error {
	evs, err := evidenceFromRejected(rejection)
	if err != nil {
		return fmt.Errorf("%w: %s decode: %v", pkgevents.ErrPermanent, stream, err)
	}
	if len(evs) == 0 {
		logNothingToClassify(ctx, logger, stream, rejection)
		return nil
	}
	if round > 0 {
		for i := range evs {
			evs[i].RemediationRound = round
		}
	}
	return handlers.ClassifyRejection(ctx, deps, tenantID, evs)
}

// newRejectionHandler returns the MessageHandler that classifies the failed
// nodes of one release.rejected:v1 message from the named stream, under the
// tenant the message names, or the default tenant when it names none. A message
// with no payload field is a permanent failure, as is one whose payload cannot
// be decoded (see classifyRejectionBody).
func newRejectionHandler(stream string, deps handlers.Deps, logger *slog.Logger) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		raw, ok := msg.Values["payload"].(string)
		if !ok {
			return fmt.Errorf("%w: %s message has no payload field", pkgevents.ErrPermanent, stream)
		}
		return classifyRejectionBody(ctx, deps, logger, stream, messageTenant(msg), []byte(raw), 0)
	}
}

// messageTenant returns the tenant_id the message carries, or the default
// tenant when it carries none.
func messageTenant(msg goredis.XMessage) string {
	if t, ok := msg.Values["tenant_id"].(string); ok && t != "" {
		return t
	}
	return pkgevents.DefaultTenantID
}

// logNothingToClassify logs at DEBUG that a rejection yields no failed node to
// classify, naming the rejection's release, stage and reason. raw has already
// decoded as a rejection, so its header decodes too.
func logNothingToClassify(ctx context.Context, logger *slog.Logger, stream string, raw []byte) {
	var head struct {
		ReleaseID string `json:"release_id"`
		Stage     string `json:"stage"`
		Reason    string `json:"reason"`
	}
	_ = json.Unmarshal(raw, &head)
	logger.DebugContext(ctx, "release rejection has no failed node to classify",
		"stream", stream, "release_id", head.ReleaseID, "stage", head.Stage, "reason", head.Reason)
}

// NewReleaseRejectedConsumer constructs a StreamConsumer that reads
// release.rejected:v1 and classifies its failed nodes via one call to
// handlers.ClassifyRejection, emitting one batched trigger for the release.
// The consumer group is created idempotently by StreamConsumer.Start; call
// Start(ctx) in a goroutine to begin consuming.
func NewReleaseRejectedConsumer(rc *goredis.Client, deps handlers.Deps, logger *slog.Logger) *pkgredis.StreamConsumer {
	return pkgredis.NewStreamConsumer(rc, streams.ReleaseRejectedV1, streams.RemediationReleaseRejected,
		newRejectionHandler(streams.ReleaseRejectedV1, deps, logger), logger)
}
