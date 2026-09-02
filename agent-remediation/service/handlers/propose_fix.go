// Package handlers holds the agent-remediation application layer. Handlers are
// thin: they orchestrate ports and the domain prompt/diff inside a unit of work
// and hold no infrastructure dependencies directly.
//
// The driver here works a whole rejected release at a time. One
// remediation.requested:v2 trigger carries the release's healable failing set;
// the driver groups it into fix clusters, calls one Fixer per cluster, submits
// one fix-verification run per edited service, and records a single
// proposal row for the attempt carrying every node's outcome. Nothing is
// announced from here: the verification reconciler emits
// remediation.proposed:v1 once every run judging the fix has passed.
package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/event"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/domain/typology"
	"github.com/carolsimone/continuo/agent-remediation/service/fixer"
	"github.com/carolsimone/continuo/agent-remediation/service/llmcache"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/promptlog"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
)

// Deps holds every collaborator ProposeFix needs, all behind ports so the
// handler imports no adapter or infrastructure package.
type Deps struct {
	NewUoW      func() uow.UnitOfWork
	LLM         ports.LLMProvider
	Evidence    ports.EvidenceReader
	Source      ports.SourceReader
	Sanitizer   ports.LogSanitizer
	Artifacts   ports.ArtifactWriter
	Clock       ports.Clock
	Logger      *slog.Logger
	MaxAttempts int
	// ServiceRepoPaths maps a dbt service_name to its project root within the
	// source repo, e.g. "service-1" → "services/service-1". The fixers use it
	// to build the full repository path of an offending source file, and the
	// driver to decide which service a proposed edit belongs to and which part
	// of its path is project-relative.
	ServiceRepoPaths map[string]string
	Locator          ports.NodeLocator
	Upstream         ports.UpstreamChangeReader
	Versions         ports.VersionReader
	Precedents       ports.PrecedentReader
	CandidateSource  ports.CandidateSourceReader
	// RepoArchive fetches a full repo checkout at a commit, for a fixer that
	// needs to search across many files (e.g. locating a python node's
	// declaring contract yaml) rather than read one file at a time.
	RepoArchive ports.RepoArchive
	// ContractLocator finds which contract yaml in that checkout declares a
	// given python node.
	ContractLocator ports.ContractLocator
	// ContractInspector reads the node declarations out of a contract yaml
	// document, so a proposed edit can be checked against the declarations the
	// repository already held.
	ContractInspector ports.ContractInspector
	// Packager merges a directory of python-node contract yaml files into the
	// wire contract a release is submitted with.
	Packager ports.ContractPackager
	// Pipeline submits fix-verification runs and reads their status.
	Pipeline ports.VerificationPipeline
	// Releases reads a candidate release's image tag for the service a
	// verification re-runs, and its original failing validation nodes for the
	// sibling-failure check a contract fix opens with.
	Releases ports.ReleaseReader
	// PriorAttempts reads the attempts already recorded for a failing node, so
	// a fixer can show the model what earlier attempts tried.
	PriorAttempts repository.AttemptLister
	// SQLDialect is the sqlglot dialect the operator's warehouse engine
	// speaks, used when packaging a proposed contract fix.
	SQLDialect string
}

// ProposeFix turns one rejected release's healable failing set into a single
// fix attempt. It counts prior attempts and enforces the per-round cap, groups
// the failing set into clusters (a shared changed ancestor is fixed once for
// every node it broke; everything else is fixed in its own source), dispatches
// each cluster to the Fixer its error class and node kind resolve to, submits
// one fix-verification run per edited service, and records a single
// proposal row for the attempt with each node's outcome on it.
//
// It announces nothing. A fix is a proposal only once every verification run
// judging it has passed, and the reconciler that polls those runs is what
// emits remediation.proposed:v1.
func ProposeFix(ctx context.Context, deps Deps, t Trigger) error {
	// A trigger without a remediation round (unset, or explicit 0) belongs to
	// round 1, the round every rejection starts a release at.
	if t.RemediationRound < 1 {
		t.RemediationRound = 1
	}
	if len(t.Nodes) == 0 {
		deps.Logger.Info("remediation.requested trigger carries no failing nodes — nothing to fix",
			"release", t.ReleaseID, "message_id", t.MessageID)
		return nil
	}

	// Read-only dedup pre-check before any write. A redelivery of a trigger that
	// was already handled (including one re-emitted with a fresh Redis message id
	// but the same upstream outbox_entry_id) is ACKed here without touching the
	// database. This is what keeps a completed trigger from minting a phantom
	// in-flight 'generating' row for a fresh attempt number that record()'s dedup
	// claim would then abandon.
	done, err := alreadyProcessed(ctx, deps, t)
	if err != nil {
		return err
	}
	if done {
		deps.Logger.Info("remediation.requested trigger already processed — skipping",
			"message_id", t.MessageID, "release", t.ReleaseID)
		return nil
	}

	attemptsInRound, totalAttempts, err := countAttempts(ctx, deps, t)
	if err != nil {
		return err
	}
	attempt := totalAttempts + 1
	nodeIDs := t.NodeIDs()

	// Per-(release, round) attempt cap: record the whole failing set escalated
	// and call no model.
	if attemptsInRound >= deps.MaxAttempts {
		return record(ctx, deps, t, attempt, proposal.Proposal{
			Status:          proposal.StatusEscalated,
			ResolvedNodeIDs: nodeIDs,
			NodeOutcomes:    outcomesFor(nodeIDs, proposal.NodeOutcome{Status: proposal.StatusEscalated}),
		})
	}

	clusters := groupClusters(t)

	// Mark this attempt in-flight in its own committed transaction, right before
	// the first model call, so the release UI can show a "Generating fix…"
	// indicator while the fix is produced. It runs after the attempt-cap guard so
	// an escalated (unhealable) set never shows the chip. A cluster that skips
	// internally finalizes the row to a terminal state; a brief
	// generating→blank flicker for that case is acceptable.
	if err := markGenerating(ctx, deps, t, attempt); err != nil {
		return err
	}

	svc := fixerServices(deps)

	outcomes := make(map[string]proposal.NodeOutcome, len(t.Nodes))
	contracts := map[string][]byte{}
	var edits []proposal.FileEdit
	var rationales []string
	var confidence proposal.Confidence
	var model string
	sourceResolved := true

	// A shared-upstream cluster whose ancestor cannot be targeted is replaced by
	// one independent cluster per member, appended to the same queue so they run
	// through the identical path.
	queue := append([]typology.Cluster(nil), clusters...)
	for i := 0; i < len(queue); i++ {
		c := queue[i]
		out, err := fixCluster(ctx, deps, svc, t, c, attempt)
		if err != nil {
			// Transient (LLM or non-404 read): the message is redelivered, and
			// the LLM cache replays the clusters that already finished.
			return err
		}
		if out.fallback {
			for _, m := range c.Members {
				queue = append(queue, typology.Cluster{
					TargetNodeID: m, Members: []string{m}, Kind: typology.KindIndependent,
				})
			}
			continue
		}
		if out.status != proposal.StatusProposed {
			for _, m := range c.Members {
				outcomes[m] = proposal.NodeOutcome{Status: out.status, Reason: out.reason}
			}
			continue
		}
		for i := range out.edits {
			out.edits[i].MemberNodeIDs = append([]string(nil), c.Members...)
		}
		edits = append(edits, out.edits...)
		for _, m := range c.Members {
			outcomes[m] = proposal.NodeOutcome{Status: proposal.StatusVerifying}
		}
		// The contract is keyed by the service its own edits belong to, derived
		// from their paths exactly as submitVerifications groups them, so the
		// contract and the edits it packages can never end up on different
		// verification runs — which would silently verify these edits as a dbt
		// project instead.
		if out.contract != nil && len(out.edits) > 0 {
			if service, _, ok := serviceForPath(deps.ServiceRepoPaths, out.edits[0].Path); ok {
				contracts[service] = out.contract
			}
		}
		if out.rationale != "" {
			rationales = append(rationales, c.TargetNodeID+": "+out.rationale)
		}
		if confidence == "" || confidenceRank(out.confidence) < confidenceRank(confidence) {
			confidence = out.confidence
		}
		if model == "" {
			model = out.model
		}
		sourceResolved = sourceResolved && out.sourceResolved
	}

	deps.Logger.Info("release fix attempt assembled",
		"release", t.ReleaseID, "attempt", attempt, "nodes", len(t.Nodes),
		"clusters", len(queue), "edits", len(edits))

	// Nothing to verify: the attempt is settled by what the clusters reported.
	// Every cluster skipping is a skip (nothing was wrong that this agent knows
	// how to fix); anything else means at least one fix was attempted and did
	// not produce a change a release could run.
	if len(edits) == 0 {
		return record(ctx, deps, t, attempt, proposal.Proposal{
			Status:          settledStatus(outcomes),
			ResolvedNodeIDs: nodeIDs,
			NodeOutcomes:    outcomes,
			Rationale:       strings.Join(rationales, "\n"),
			Model:           model,
		})
	}

	verifications, err := submitVerifications(ctx, deps, t, attempt, edits, contracts)
	// Edits no release could ever run — outside every configured service, or
	// mixing two manifest kinds in one service — are permanent: a redelivery
	// would produce the same edits and route them no better. The attempt ends
	// here with the reason recorded rather than being retried forever.
	if unverifiable(err) {
		deps.Logger.Error("the attempt's edits cannot be verified by any release",
			"release", t.ReleaseID, "attempt", attempt, "error", err)
		return record(ctx, deps, t, attempt, proposal.Proposal{
			Status:          proposal.StatusFailed,
			ResolvedNodeIDs: nodeIDs,
			NodeOutcomes:    outcomesFor(nodeIDs, proposal.NodeOutcome{Status: proposal.StatusFailed, Reason: err.Error()}),
			Rationale:       err.Error(),
		})
	}
	if err != nil {
		// Transient: artifact writes and verification submissions are both
		// idempotent on the attempt's keys, so a redelivery repeats them safely.
		return err
	}

	return record(ctx, deps, t, attempt, proposal.Proposal{
		Status:          proposal.StatusVerifying,
		ResolvedNodeIDs: nodeIDs,
		NodeOutcomes:    outcomes,
		Verifications:   verifications,
		Edits:           edits,
		Confidence:      confidence,
		Rationale:       strings.Join(rationales, "\n"),
		Model:           model,
		SourceResolved:  sourceResolved,
		Repo:            t.Repo,
		CommitSHA:       t.CommitSHA,
	})
}

// groupClusters partitions the trigger's failing set into fix targets. Nodes
// that fail the same way and share an ancestor this release changed become one
// cluster targeting that ancestor; every other node is its own cluster. The
// grouping is pure — it reads only what the trigger already carries — so the
// routing decision is decided before any port is touched.
func groupClusters(t Trigger) []typology.Cluster {
	nodes := make([]typology.FailingNode, 0, len(t.Nodes))
	dag := typology.DagView{ChangedAncestorsByNode: make(map[string][]string, len(t.Nodes))}
	for _, n := range t.Nodes {
		nodes = append(nodes, typology.FailingNode{
			NodeID:         n.NodeID,
			ErrorSignature: n.ErrorSignature,
			Category:       n.Category,
			Reason:         n.Reason,
		})
		ids := make([]string, 0, len(n.ChangedAncestors))
		for _, a := range n.ChangedAncestors {
			ids = append(ids, a.NodeID)
		}
		dag.ChangedAncestorsByNode[n.NodeID] = ids
	}
	return coalesceUpstream(typology.Group(nodes, dag, typology.SharedUpstreamCause{}))
}

// coalesceUpstream merges shared-upstream clusters that name the same fix
// target. Grouping partitions the failing set per error signature, so one
// changed ancestor that broke its descendants in two different ways yields one
// cluster per signature — both targeting that ancestor. Fixing them separately
// would call the model twice for one file and, worse, have both fixes write the
// artifact key the target's edit is keyed on, so the attempt would record two
// edits for one path of which only the last one written actually exists. The
// members are unioned instead and the ancestor is repaired once, with every
// failure it caused shown to the model in the same call.
//
// Only shared-upstream clusters can collide this way: an independent cluster's
// target is the failing node itself, and a node appears in the failing set once.
// Order is preserved (each merged cluster keeps the position of its first
// occurrence) and members are sorted, so the result stays deterministic.
func coalesceUpstream(clusters []typology.Cluster) []typology.Cluster {
	at := map[string]int{}
	out := make([]typology.Cluster, 0, len(clusters))
	for _, c := range clusters {
		if c.Kind != typology.KindSharedUpstream {
			out = append(out, c)
			continue
		}
		i, seen := at[c.TargetNodeID]
		if !seen {
			at[c.TargetNodeID] = len(out)
			out = append(out, c)
			continue
		}
		merged := append(append([]string(nil), out[i].Members...), c.Members...)
		sort.Strings(merged)
		out[i].Members = merged
	}
	return out
}

// clusterOutcome is what fixing one cluster produced, projected onto the fields
// the attempt's single proposal row is assembled from.
type clusterOutcome struct {
	edits []proposal.FileEdit
	// contract is the packaged contract yaml a python fix must be verified
	// with, uploaded under the verification run of whichever service this
	// cluster's edits belong to; nil for a dbt fix, whose edits are verified by
	// re-running the project itself.
	contract []byte
	status   proposal.Status
	// reason explains a non-proposed status to the operator; rationale is the
	// model's account of a proposed fix.
	reason         string
	rationale      string
	model          string
	confidence     proposal.Confidence
	sourceResolved bool
	// fallback reports that a shared-upstream cluster could not be targeted, so
	// each of its members must be fixed in its own source instead.
	fallback bool
}

// fixCluster produces one cluster's fix. A shared-upstream cluster is repaired
// once in the changed ancestor every member descends from; an independent
// cluster is repaired in the failing node's own source by the Fixer its error
// class and node kind resolve to. An upstream target that cannot be located or
// is not a dbt model ends as a fallback, and the caller re-runs the members
// independently.
func fixCluster(ctx context.Context, deps Deps, svc fixer.Services, t Trigger, c typology.Cluster, attempt int) (clusterOutcome, error) {
	// Scope LLM response caching to this inbound trigger: a redelivery reuses
	// the completions the first delivery paid for, while a new trigger (a later
	// attempt for the same release) misses and calls the model again. The cache
	// also keys on the request itself, so the several clusters of one trigger
	// do not collide under this single key. The failure identity lets the
	// prompt-logging decorator tie each logged prompt back to the cluster it was
	// built for.
	llmCtx := llmcache.ContextWithIdempotencyKey(ctx, t.idempotencyKey())
	llmCtx = promptlog.ContextWithFailure(llmCtx, promptlog.Failure{
		Source:    t.Source,
		ReleaseID: t.ReleaseID,
		NodeID:    c.TargetNodeID,
		Attempt:   attempt,
	})

	if c.Kind == typology.KindSharedUpstream {
		r, err := fixer.ProposeUpstreamFix(llmCtx, svc, upstreamInputFor(t, c, attempt))
		if err != nil {
			return clusterOutcome{}, err
		}
		if r.Proposal.Status == proposal.StatusSkipped {
			deps.Logger.Info("shared-upstream fix unavailable; each member falls back to its own source",
				"release", t.ReleaseID, "target", c.TargetNodeID,
				"members", c.Members, "reason", r.Proposal.Rationale)
			return clusterOutcome{fallback: true}, nil
		}
		return outcomeFromResult(r), nil
	}

	node, ok := nodeByID(t, c.TargetNodeID)
	if !ok {
		return clusterOutcome{}, fmt.Errorf("cluster targets node %q, which the trigger does not carry", c.TargetNodeID)
	}
	fx, err := fixer.For(t.Source, node.NodeType)
	if err != nil {
		return clusterOutcome{}, err // unknown error class: surfaced loudly, not silently skipped
	}
	r, err := fx.Propose(llmCtx, svc, inputFor(t, node, attempt))
	if err != nil {
		return clusterOutcome{}, err
	}
	out := outcomeFromResult(r)
	// A Fixer that repairs the failing node's own source names no target on its
	// edits, because it only ever edits that one node.
	for i := range out.edits {
		if out.edits[i].TargetNodeID == "" {
			out.edits[i].TargetNodeID = node.NodeID
		}
	}
	return out, nil
}

// outcomeFromResult projects a Fixer's Result onto a clusterOutcome. A result
// labelled proposed that named no file is downgraded to failed: a fix with no
// edit changes nothing a verification run could run, and no pull request could
// carry it either, so recording its members as being verified would promise a
// verdict that never arrives.
func outcomeFromResult(r fixer.Result) clusterOutcome {
	out := clusterOutcome{
		edits:          append([]proposal.FileEdit(nil), r.Proposal.Edits...),
		contract:       r.VerificationContract,
		status:         r.Proposal.Status,
		reason:         r.Proposal.Rationale,
		rationale:      r.Proposal.Rationale,
		model:          r.Proposal.Model,
		confidence:     r.Proposal.Confidence,
		sourceResolved: r.Proposal.SourceResolved,
	}
	if out.status == proposal.StatusProposed && len(out.edits) == 0 {
		out.status = proposal.StatusFailed
		out.reason = "the fix could not be applied to version-controlled source, so there is nothing to verify"
	}
	return out
}

// inputFor projects the trigger's release-level facts and one failing node onto
// the evidence a Fixer reads.
func inputFor(t Trigger, n TriggerNode, attempt int) fixer.Input {
	return fixer.Input{
		Source:               t.Source,
		ReleaseID:            t.ReleaseID,
		NodeID:               n.NodeID,
		RelationID:           n.RelationID,
		ErrorSignature:       n.ErrorSignature,
		Category:             n.Category,
		Reason:               n.Reason,
		ErrorExcerpt:         n.ErrorExcerpt,
		Repo:                 t.Repo,
		CommitSHA:            t.CommitSHA,
		FilePath:             n.FilePath,
		Service:              n.Service,
		NodeType:             n.NodeType,
		OtherService:         n.OtherService,
		OtherFilePath:        n.OtherFilePath,
		DBTLogURI:            n.DBTLogURI,
		CandidateArtifactURI: n.CandidateArtifactURI,
		CodeBundleURI:        t.CodeBundleURI,
		Attempt:              attempt,
	}
}

// upstreamInputFor projects a shared-upstream cluster onto the evidence the
// upstream fixer reads: the changed ancestor to repair, and each failing
// descendant that shares it as a cause.
func upstreamInputFor(t Trigger, c typology.Cluster, attempt int) fixer.UpstreamInput {
	members := make([]fixer.MemberFailure, 0, len(c.Members))
	for _, id := range c.Members {
		n, ok := nodeByID(t, id)
		if !ok {
			continue
		}
		members = append(members, fixer.MemberFailure{
			NodeID:         n.NodeID,
			ErrorSignature: n.ErrorSignature,
			Category:       n.Category,
			Reason:         n.Reason,
			ErrorExcerpt:   n.ErrorExcerpt,
		})
	}
	filePath, service := ancestorLocation(t, c.TargetNodeID)
	return fixer.UpstreamInput{
		ReleaseID:      t.ReleaseID,
		Repo:           t.Repo,
		CommitSHA:      t.CommitSHA,
		CodeBundleURI:  t.CodeBundleURI,
		TargetNodeID:   c.TargetNodeID,
		TargetFilePath: filePath,
		TargetService:  service,
		Attempt:        attempt,
		Members:        members,
	}
}

// ancestorLocation is where the rejected release's candidate topology declares
// the changed ancestor, as the trigger carries it on the failing nodes that
// descend from it. Empty when the rejection carried no location for it, and the
// fixer then falls back to the promoted graph.
//
// A node this release renamed or moved is at its OLD path in the promoted
// graph, so the candidate's own answer is the one an edit must use; the ids
// travel with it, so no extra lookup is needed to find it.
func ancestorLocation(t Trigger, ancestorID string) (filePath, service string) {
	for _, n := range t.Nodes {
		for _, a := range n.ChangedAncestors {
			if a.NodeID == ancestorID && a.FilePath != "" && a.Service != "" {
				return a.FilePath, a.Service
			}
		}
	}
	return "", ""
}

// fixerServices binds the driver's ports to the bundle every Fixer collaborates
// through.
func fixerServices(deps Deps) fixer.Services {
	return fixer.Services{
		LLM: deps.LLM, Source: deps.Source, Evidence: deps.Evidence,
		Sanitizer: deps.Sanitizer, Artifacts: deps.Artifacts,
		Logger: deps.Logger, ServiceRepoPaths: deps.ServiceRepoPaths,
		Locator: deps.Locator, Upstream: deps.Upstream, Versions: deps.Versions,
		Precedents: deps.Precedents, CandidateSource: deps.CandidateSource,
		Archive: deps.RepoArchive, ContractLocator: deps.ContractLocator,
		ContractInspector: deps.ContractInspector,
		Packager:          deps.Packager, Releases: deps.Releases,
		PriorAttempts: deps.PriorAttempts, SQLDialect: deps.SQLDialect,
	}
}

// nodeByID returns the trigger's failing node with this id.
func nodeByID(t Trigger, nodeID string) (TriggerNode, bool) {
	for _, n := range t.Nodes {
		if n.NodeID == nodeID {
			return n, true
		}
	}
	return TriggerNode{}, false
}

// outcomesFor stamps the same outcome on every named node, for the attempts
// that resolve identically for the whole failing set (the cap escalation, and
// an attempt whose edits no configured service can run).
func outcomesFor(nodeIDs []string, o proposal.NodeOutcome) map[string]proposal.NodeOutcome {
	out := make(map[string]proposal.NodeOutcome, len(nodeIDs))
	for _, id := range nodeIDs {
		out[id] = o
	}
	return out
}

// settledStatus is the attempt's status when no cluster produced an edit: a
// skip when every node was skipped, and a failure as soon as one node was
// actually attempted and did not yield a change.
func settledStatus(outcomes map[string]proposal.NodeOutcome) proposal.Status {
	for _, o := range outcomes {
		if o.Status != proposal.StatusSkipped {
			return proposal.StatusFailed
		}
	}
	return proposal.StatusSkipped
}

// confidenceRank orders the confidence values so a batched attempt can report
// the weakest cluster's: a reviewer judges the whole change at once, so the
// least confident part of it is what the number has to reflect.
func confidenceRank(c proposal.Confidence) int {
	switch c {
	case proposal.ConfidenceLow:
		return 0
	case proposal.ConfidenceHigh:
		return 2
	default:
		return 1
	}
}

// signatureFor returns the error signature of one of the trigger's nodes, empty
// when the trigger does not carry it.
func signatureFor(t Trigger, nodeID string) string {
	if n, ok := nodeByID(t, nodeID); ok {
		return n.ErrorSignature
	}
	return ""
}

// alreadyProcessed opens a read-only transaction and reports whether this
// inbound trigger was already handled, on either dedup axis (message id/stream
// or upstream outbox_entry_id). It performs no write, so re-emitted completed
// triggers are ACKed without claiming a fresh message_processing row.
func alreadyProcessed(ctx context.Context, deps Deps, t Trigger) (bool, error) {
	u := deps.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return false, fmt.Errorf("begin (dedup pre-check): %w", err)
	}
	defer func() { _ = u.Rollback() }()
	done, err := u.MessageProcessingRepo().AlreadyProcessed(
		ctx, t.MessageID, streams.RemediationRequestedV2, t.OutboxEntryID)
	if err != nil {
		return false, fmt.Errorf("dedup pre-check: %w", err)
	}
	return done, nil
}

// countAttempts opens a read-only transaction and returns two counts of prior
// TERMINAL attempts for this release. inRound, scoped to this trigger's own
// remediation round, is what the per-round attempt cap is checked against — a
// later release is new code and starts its own count, and a human's "try again"
// on a rejected release starts a fresh round with its own count too. total is
// the cumulative count across every round of this release so far, and is what
// this attempt is numbered from: the proposal table's uniqueness is
// (release_id, attempt), not scoped by round, so a round's first attempt must
// continue the release's existing attempt sequence rather than restart it —
// restarting would collide with, and silently overwrite, a row an earlier round
// already wrote (a round-2 retry of a set round 1 escalated is the ordinary
// case this guards). For a round-1 trigger — the common case — total equals
// inRound. In-flight rows are excluded by the repository from both counts, so
// the in-progress attempt keeps a stable attempt number across a redelivery.
func countAttempts(ctx context.Context, deps Deps, t Trigger) (inRound, total int, err error) {
	u := deps.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return 0, 0, fmt.Errorf("begin (count): %w", err)
	}
	defer func() { _ = u.Rollback() }()
	repo := u.ProposalRepo()
	for round := 1; round <= t.RemediationRound; round++ {
		n, err := repo.CountAttempts(ctx, t.ReleaseID, round)
		if err != nil {
			return 0, 0, fmt.Errorf("count attempts (round %d): %w", round, err)
		}
		total += n
		inRound = n
	}
	return inRound, total, nil
}

// markGenerating writes an in-flight 'generating' proposal row for the attempt
// in its own committed transaction, just before the first model call. The row
// already names the whole failing set the attempt addresses, so the release page
// can show every node as being worked on. The insert is idempotent (ON CONFLICT
// DO NOTHING on (release_id, attempt)), so a redelivery of an in-flight attempt
// re-uses the existing row rather than creating a second one.
func markGenerating(ctx context.Context, deps Deps, t Trigger, attempt int) error {
	u := deps.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin (mark generating): %w", err)
	}
	defer func() { _ = u.Rollback() }()
	nodeIDs := t.NodeIDs()
	p := proposal.Proposal{
		Source:           t.Source,
		ReleaseID:        t.ReleaseID,
		RemediationRound: t.RemediationRound,
		ResolvedNodeIDs:  nodeIDs,
		NodeOutcomes:     outcomesFor(nodeIDs, proposal.NodeOutcome{Status: proposal.StatusGenerating}),
		Attempt:          attempt,
		Status:           proposal.StatusGenerating,
		Services:         t.Services(),
		CreatedAt:        deps.Clock.Now(),
	}
	p.NormalizeRepresentativeViews()
	p.ErrorSignature = signatureFor(t, p.NodeID)
	if err := u.ProposalRepo().InsertGenerating(ctx, p); err != nil {
		return fmt.Errorf("mark generating: %w", err)
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit (mark generating): %w", err)
	}
	return nil
}

// record finalizes the attempt: it stamps the trigger's identity onto the
// proposal, derives the representative views of its batched fields, and upserts
// the row. It announces nothing — a fix becomes a proposal only once every
// verification run judging it has passed, and the reconciler that polls those
// runs is what emits.
//
// A caller that does not narrow the addressed set gets the trigger's whole
// failing set, and the attempt's error signature is the representative node's.
// An attempt awaiting verification also stores the raw trigger, so the
// reconciler resolving those runs can rebuild the trigger and retry with their
// error as new evidence; every other status resolves within this call and
// needs nothing to replay.
//
// Inbound dedup is performed atomically inside the transaction: the
// message_processing claim and the proposal write commit or roll back together.
// A redelivered trigger collides on the claim and causes a rollback with a nil
// return (consumer ACKs, no duplicate written). A transient error rolls back
// without persisting the claim, so the message is cleanly retried.
func record(ctx context.Context, deps Deps, t Trigger, attempt int, p proposal.Proposal) error {
	p.Source = t.Source
	p.ReleaseID = t.ReleaseID
	p.RemediationRound = t.RemediationRound
	p.Attempt = attempt
	p.CreatedAt = deps.Clock.Now()
	if len(p.ResolvedNodeIDs) == 0 {
		p.ResolvedNodeIDs = t.NodeIDs()
	}
	p.Services = proposal.UnionServices(t.Services(), verificationServices(p.Verifications))
	p.NormalizeRepresentativeViews()
	p.ErrorSignature = signatureFor(t, p.NodeID)
	if p.Status == proposal.StatusVerifying {
		p.TriggerPayload = t.RawPayload
	}

	u := deps.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = u.Rollback() }()

	// Claim this inbound trigger atomically within the write transaction. A
	// duplicate (redelivered or replayed message) returns dup=true: log and
	// return nil so the consumer ACKs without writing anything. The rollback
	// deferred above discards the tx without persisting the claim.
	if _, dup, err := messageprocessing.DedupWithOutboxEntryID(
		ctx, u.MessageProcessingRepo(), deps.Logger,
		t.MessageID, streams.RemediationRequestedV2, t.RawPayload, t.OutboxEntryID,
	); err != nil {
		return fmt.Errorf("dedup: %w", err)
	} else if dup {
		deps.Logger.Info("duplicate remediation.requested trigger — skipping",
			"message_id", t.MessageID, "release", t.ReleaseID)
		return nil
	}

	if err := u.ProposalRepo().Upsert(ctx, p); err != nil {
		return fmt.Errorf("upsert proposal: %w", err)
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	deps.Logger.Info("proposal recorded",
		"release", t.ReleaseID, "node", p.NodeID, "nodes", len(p.ResolvedNodeIDs),
		"status", p.Status, "attempt", attempt)
	return nil
}

// verificationServices is the services an attempt's verification runs cover.
func verificationServices(vs []proposal.Verification) []string {
	out := make([]string, 0, len(vs))
	for _, v := range vs {
		out = append(out, v.Service)
	}
	return out
}

// Enqueue builds the deterministic remediation.proposed:v1 outbox entry for an
// attempt whose fix every verification run has passed, and creates it on the
// repository bound to the caller's transaction. p carries the whole
// announcement: the failure it addresses (source, release, representative node,
// error signature), every node it resolves, and every file it changes — each
// edit naming the node whose source it changes. sourceResolved indicates
// whether the fix rewrote real version-controlled source. msgProcID is the
// message_processing row UUID of the inbound trigger behind the write, stored
// on the outbox entry for provenance; uuid.Nil for a write no inbound message
// drove.
func Enqueue(ctx context.Context, u uow.UnitOfWork, clock ports.Clock, p proposal.Proposal, sourceResolved bool, msgProcID uuid.UUID) error {
	eventID := event.RemediationEventID(p.ReleaseID, p.Attempt)
	edits := make([]event.ProposedEdit, 0, len(p.Edits))
	for _, e := range p.Edits {
		edits = append(edits, event.ProposedEdit{
			Path:         e.Path,
			ContentURI:   e.ContentURI,
			DiffURI:      e.DiffURI,
			TargetNodeID: e.TargetNodeID,
		})
	}
	payload := event.RemediationProposed{
		EventID:          eventID.String(),
		Source:           p.Source,
		ReleaseID:        p.ReleaseID,
		RemediationRound: p.RemediationRound,
		NodeID:           p.NodeID,
		ResolvedNodeIDs:  append([]string(nil), p.ResolvedNodeIDs...),
		ErrorSignature:   p.ErrorSignature,
		ProposedSQLURI:   p.ProposedSQLURI,
		DiffURI:          p.DiffURI,
		Edits:            edits,
		Rationale:        p.Rationale,
		Confidence:       string(p.Confidence),
		Model:            p.Model,
		Attempt:          p.Attempt,
		SourceResolved:   sourceResolved,
		ProposedAt:       clock.Now().Format(time.RFC3339),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal proposed event: %w", err)
	}
	now := clock.Now()
	entry := &outbox.Entry{
		ID:            uuid.NewSHA1(uuid.NameSpaceOID, []byte(eventID.String())),
		AggregateType: "remediation_agent",
		AggregateID:   event.AggregateIDForRelease(p.ReleaseID),
		EventType:     event.EventType,
		Payload:       body,
		StreamName:    streams.RemediationProposedV1,
		Status:        "pending",
		MaxRetries:    outbox.DefaultMaxRetries,
		CreatedAt:     now,
	}
	if msgProcID != uuid.Nil {
		id := msgProcID
		entry.MessageProcessingID = &id
	}
	return u.OutboxRepo().Create(ctx, entry)
}
