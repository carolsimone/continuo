// Package gate holds the long check that asks release-controller to judge a
// try: it submits one release check per edited project, then polls the checks
// until they give a verdict. The check id that names each check lives here too.
package gate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/overlay"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
)

const nameReleaseCheck workflow.StepName = "release_check"

var (
	errNoEdits      = errors.New("the try has no edit to verify")
	errNoSubmitTime = errors.New("the gate input names no time its checks were submitted at")
	// errNotABundle marks a project whose packaged contract is not an ODCS
	// producer bundle, the only contract format a release check reads.
	errNotABundle = errors.New("the packaged contract is not an ODCS producer bundle")
)

// codeNotABundle is the finding code of errNotABundle.
const codeNotABundle = "packaged_contract_not_a_bundle"

// ReleaseCheck is the long check `release_check`. It runs a real
// release check over the edits of each project the try changed: the check
// carries the parse, candidate-schema and validation stages to a passed or failed verdict
// without ever promoting, so it decides whether the edits fix the failing
// release.
type ReleaseCheck struct {
	evidence         ports.EvidenceReader
	artifacts        ports.ArtifactWriter
	releases         ports.ReleaseReader
	checks           ports.ReleaseChecks
	projectRepoPaths map[string]string
	clock            ports.Clock
	timeout          time.Duration
	logger           *slog.Logger
}

var _ step.LongCheck = (*ReleaseCheck)(nil)

// NewReleaseCheck builds the release_check step. timeout is how long a
// check may be running, or a try's checks unreadable, before the gate gives up.
func NewReleaseCheck(
	evidence ports.EvidenceReader, artifacts ports.ArtifactWriter, releases ports.ReleaseReader,
	checks ports.ReleaseChecks, projectRepoPaths map[string]string,
	clock ports.Clock, timeout time.Duration, logger *slog.Logger,
) *ReleaseCheck {
	return &ReleaseCheck{
		evidence: evidence, artifacts: artifacts, releases: releases, checks: checks,
		projectRepoPaths: projectRepoPaths, clock: clock, timeout: timeout,
		logger: shared.LoggerOrDefault(logger),
	}
}

// Meta describes the step. It reads the packaged_contract evidence of the
// clusters that have one, so it declares no Needs: a dbt cluster has none.
func (*ReleaseCheck) Meta() step.Meta {
	return step.Meta{
		Name: nameReleaseCheck,
		Kind: step.KindCheck,
		Description: "Submits one release check per edited project to release-controller and waits for their verdict. " +
			"A failed check gives a finding per failing node, and a check that does not answer in time fails the try.",
		Stages:            []workflow.Stage{workflow.StageGate},
		MayStandAsGateRun: true,
	}
}

// plannedCheck is one check that judges part of a try, with the manifest kind it
// is verified as and the id it is submitted under. A python project's check
// uploads the packaged contract in its edits; a dbt check has none.
type plannedCheck struct {
	edits   shared.ProjectEdits
	kind    string
	checkID string
}

// isODCSBundle reports whether contract is an ODCS producer bundle: a JSON
// object with a bundle_version key.
func isODCSBundle(contract []byte) bool {
	var doc map[string]json.RawMessage
	if json.Unmarshal(contract, &doc) != nil {
		return false
	}
	_, ok := doc["bundle_version"]
	return ok
}

// refusalFinding reports the finding for an error plan returned that no check
// could judge: a refusal of the edit groups, or a packaged contract that is not
// an ODCS producer bundle. It reports false for any other error.
func refusalFinding(err error) (remediation.Finding, bool) {
	if errors.Is(err, errNotABundle) {
		return remediation.Finding{Code: codeNotABundle, Message: err.Error()}, true
	}
	return shared.RefusalFinding(err)
}

// plan groups the try's edits into the checks that judge them, sorted by project.
// A project with a packaged contract is verified as kind python, and the
// try is refused with errNotABundle when that contract is not an ODCS producer
// bundle.
func (g *ReleaseCheck) plan(in step.GateInput) ([]plannedCheck, error) {
	if len(shared.EditsOf(in.Drafts)) == 0 {
		return nil, errNoEdits
	}
	groups, err := shared.Group(g.projectRepoPaths, in.Trigger, in.Drafts)
	if err != nil {
		return nil, err
	}
	checks := make([]plannedCheck, len(groups))
	for i, grp := range groups {
		check := plannedCheck{edits: grp, kind: ports.ManifestKindDbt, checkID: CheckID(in.Trigger.ReleaseID, grp.Project, in.Try.Attempt)}
		if grp.Contract != nil {
			if !isODCSBundle(grp.Contract) {
				return nil, fmt.Errorf("%w: project %s", errNotABundle, grp.Project)
			}
			check.kind = ports.ManifestKindPython
		}
		checks[i] = check
	}
	return checks, nil
}

// Submit posts one release check per project the try edits. A python project
// is verified by the odcs-bundle.json its fix packed, uploaded under the check's
// own id as kind python. A dbt project is verified by re-running its
// project with the proposed content laid over it, so each edit's content is
// read back from the artifact the draft stored and packed into one source
// overlay, keyed by the edit's path within that project's directory.
//
// Both the artifact writes and the submission are idempotent on the try's keys,
// since the check id is a pure function of (release, project, attempt), so a
// repeat of a submission is safe. The result is waiting, with the check ids; Poll
// reports the checks' projects, kinds and phases.
func (g *ReleaseCheck) Submit(ctx context.Context, in step.GateInput) (step.GateVerdict, error) {
	checks, err := g.plan(in)
	if f, refused := refusalFinding(err); refused {
		g.logger.Warn("the attempt's edits cannot be verified by any release; the try fails and the next try is shown why",
			"release", in.Trigger.ReleaseID, "attempt", in.Try.Attempt, "error", err)
		return step.GateVerdict{
			Result:      remediation.CheckResult{Check: string(nameReleaseCheck), Findings: []remediation.Finding{f}},
			VerifyError: f.Message,
		}, nil
	}
	if err != nil {
		return step.GateVerdict{}, err
	}
	t, attempt := in.Trigger, in.Try.Attempt
	releaseChecks := make([]proposal.ReleaseCheck, 0, len(checks))
	for _, r := range checks {
		project := r.edits.Project
		overlayURI := ""
		if r.edits.Contract != nil {
			// A python project's artifact IS the packed bundle: there is no
			// project to overlay, so the check reads the bundle written under
			// its own id.
			if _, err := g.artifacts.Write(ctx,
				project+"/"+r.checkID+"/"+ports.ODCSBundleName, string(r.edits.Contract), "application/json"); err != nil {
				return step.GateVerdict{}, fmt.Errorf("write the release check's contract for %s: %w", project, err)
			}
		} else {
			files := make([]overlay.File, 0, len(r.edits.Edits))
			var deletes []string
			for _, e := range r.edits.Edits {
				content, err := g.evidence.Fetch(ctx, e.ContentURI)
				if err != nil {
					return step.GateVerdict{}, fmt.Errorf("read proposed content for %s: %w", e.Path, err)
				}
				files = append(files, overlay.File{
					Path:    strings.TrimPrefix(e.Path, r.edits.Prefix+"/"),
					Content: []byte(content),
				})
				// A moved file must be absent from the check's project, as it is
				// from the pull request's tree.
				if e.RenamedFrom != "" {
					deletes = append(deletes, strings.TrimPrefix(e.RenamedFrom, r.edits.Prefix+"/"))
				}
			}
			tarball, err := overlay.Build(files, deletes)
			if err != nil {
				return step.GateVerdict{}, fmt.Errorf("build source overlay for %s: %w", project, err)
			}
			overlayURI, err = g.artifacts.Write(ctx,
				project+"/"+r.checkID+"/source-overlay.tar.gz", string(tarball), "application/gzip")
			if err != nil {
				return step.GateVerdict{}, fmt.Errorf("write source overlay for %s: %w", project, err)
			}
		}

		// The trigger carries no image tag, so the failing release's own tag is
		// read and reused: a release check never promotes, so it never
		// reaches the path an image tag would otherwise drive.
		imageTag, err := g.releases.ImageTag(ctx, t.ReleaseID, project)
		if err != nil {
			return step.GateVerdict{}, fmt.Errorf("read image tag of release %s for %s: %w", t.ReleaseID, project, err)
		}
		if err := g.checks.Submit(ctx, ports.ReleaseCheckRequest{
			CheckID:           r.checkID,
			Project:           project,
			ImageTag:          imageTag,
			Kind:              r.kind,
			VerifiesReleaseID: t.ReleaseID,
			Attempt:           attempt,
			SourceOverlayURI:  overlayURI,
		}); err != nil {
			return step.GateVerdict{}, fmt.Errorf("submit release check %s: %w", r.checkID, err)
		}
		g.logger.Info("release check submitted",
			"release", t.ReleaseID, "attempt", attempt, "project", project,
			"check", r.checkID, "kind", r.kind, "edits", len(r.edits.Edits))
		releaseChecks = append(releaseChecks, proposal.ReleaseCheck{
			Project: project, Kind: r.kind, CheckID: r.checkID, Phase: proposal.PhaseQueued,
		})
	}
	return step.GateVerdict{
		Result:        remediation.CheckResult{Check: string(nameReleaseCheck), Waiting: true, CheckIDs: checkIDs(releaseChecks)},
		ReleaseChecks: releaseChecks,
	}, nil
}

// Poll reads the status of every check of the try once and gives the gate's
// verdict: waiting while a check is queued or running inside its budget, failed
// on the first failed check or when the budget is spent, passed when every check
// passed. The checks of the try are rebuilt from the input the way Submit built
// them, and refused when they are not the checks the waiting result names. The
// verdict carries the project, kind, phase and errors of each check it read, and
// Phases lists those seen queued or running. A check whose status could not be
// read is left out of both, so the runner keeps what it recorded for it. An
// input with no SubmittedAt is refused before any check is read, since the budget
// of a check whose status cannot be read counts from it.
func (g *ReleaseCheck) Poll(ctx context.Context, in step.GateInput, waiting remediation.CheckResult) (step.GateVerdict, error) {
	if in.SubmittedAt.IsZero() {
		return step.GateVerdict{}, errNoSubmitTime
	}
	planned, err := g.plan(in)
	if err != nil {
		return step.GateVerdict{}, err
	}
	got := read{nodeErrors: map[string]string{}}
	for _, r := range planned {
		got.checks = append(got.checks, proposal.ReleaseCheck{
			Project: r.edits.Project, Kind: r.kind, CheckID: r.checkID, Phase: proposal.PhaseQueued,
		})
	}
	if ids := checkIDs(got.checks); !slices.Equal(ids, waiting.CheckIDs) {
		return step.GateVerdict{}, fmt.Errorf("the try's checks are %v and the waiting result names %v", ids, waiting.CheckIDs)
	}
	for i := range got.checks {
		st, err := g.checks.Status(ctx, got.checks[i].CheckID)
		if err != nil {
			g.logger.Warn("release check: read status", "check", got.checks[i].CheckID, "error", err)
			break
		}
		got.checks[i] = applyStatus(got.checks[i], st)
		got.read = i + 1
		if st.Phase == proposal.PhaseFailed {
			for node, msg := range st.NodeErrors {
				got.nodeErrors[node] = msg
			}
		}
	}
	return g.decide(in, got), nil
}
