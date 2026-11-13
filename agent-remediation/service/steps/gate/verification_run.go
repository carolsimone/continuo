// Package gate holds the long check that asks the release pipeline to judge a
// try: it submits one verification run per edited service, then polls the runs
// until they give a verdict. The run id that names each run lives here too.
package gate

import (
	"context"
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

const nameVerificationRun workflow.StepName = "verification_run"

var (
	errNoEdits      = errors.New("the try has no edit to verify")
	errNoSubmitTime = errors.New("the gate input names no time its runs were submitted at")
)

// VerificationRun is the long check `verification_run`. It runs the real
// release pipeline over the try's edits: a verification run carries the full
// parse, candidate-schema and validation pipeline to a passed or failed verdict
// without ever promoting, so it decides whether the edits fix the failing
// release.
type VerificationRun struct {
	evidence         ports.EvidenceReader
	artifacts        ports.ArtifactWriter
	releases         ports.ReleaseReader
	pipeline         ports.VerificationPipeline
	serviceRepoPaths map[string]string
	clock            ports.Clock
	timeout          time.Duration
	logger           *slog.Logger
}

var _ step.LongCheck = (*VerificationRun)(nil)

// NewVerificationRun builds the verification_run step. timeout is how long a
// run may be running, or a try's runs unreadable, before the gate gives up.
func NewVerificationRun(
	evidence ports.EvidenceReader, artifacts ports.ArtifactWriter, releases ports.ReleaseReader,
	pipeline ports.VerificationPipeline, serviceRepoPaths map[string]string,
	clock ports.Clock, timeout time.Duration, logger *slog.Logger,
) *VerificationRun {
	return &VerificationRun{
		evidence: evidence, artifacts: artifacts, releases: releases, pipeline: pipeline,
		serviceRepoPaths: serviceRepoPaths, clock: clock, timeout: timeout,
		logger: shared.LoggerOrDefault(logger),
	}
}

// Meta describes the step. It reads the packaged_contract evidence of the
// clusters that have one, so it declares no Needs: a dbt cluster has none.
func (*VerificationRun) Meta() step.Meta {
	return step.Meta{
		Name: nameVerificationRun,
		Kind: step.KindCheck,
		Description: "Submits one verification run per edited service to the release pipeline and waits for their verdict. " +
			"A failed run gives a finding per failing node, and a run that does not answer in time fails the try.",
		Stages:            []workflow.Stage{workflow.StageGate},
		MayStandAsGateRun: true,
	}
}

// plannedRun is one run that judges part of a try, with the manifest kind it
// is verified as and the id it is submitted under.
type plannedRun struct {
	edits shared.ServiceEdits
	kind  string
	runID string
}

// plan groups the try's edits into the runs that judge them, sorted by service.
func (g *VerificationRun) plan(in step.GateInput) ([]plannedRun, error) {
	if len(shared.EditsOf(in.Drafts)) == 0 {
		return nil, errNoEdits
	}
	groups, err := shared.Group(g.serviceRepoPaths, in.Trigger, in.Drafts)
	if err != nil {
		return nil, err
	}
	runs := make([]plannedRun, len(groups))
	for i, grp := range groups {
		kind := ports.VerificationKindDbt
		if grp.Contract != nil {
			kind = ports.VerificationKindPython
		}
		runs[i] = plannedRun{edits: grp, kind: kind, runID: RunID(in.Trigger.ReleaseID, grp.Service, in.Try.Attempt)}
	}
	return runs, nil
}

// Submit posts one verification run per service the try edits. A python service
// is verified by the contract yaml its fix packaged, uploaded under the run's
// own id. A dbt service is verified by re-running its project with the proposed
// content laid over it, so each edit's content is read back from the artifact
// the draft stored and packed into one source overlay, keyed by the edit's path
// within that service's project.
//
// Both the artifact writes and the submission are idempotent on the try's keys,
// since the run id is a pure function of (release, service, attempt), so a
// repeat of a submission is safe. The result is waiting, with the run ids; Poll
// reports the runs' services, kinds and phases.
func (g *VerificationRun) Submit(ctx context.Context, in step.GateInput) (step.GateVerdict, error) {
	runs, err := g.plan(in)
	if f, refused := shared.RefusalFinding(err); refused {
		g.logger.Error("the attempt's edits cannot be verified by any release",
			"release", in.Trigger.ReleaseID, "attempt", in.Try.Attempt, "error", err)
		return step.GateVerdict{
			Result:      remediation.CheckResult{Check: string(nameVerificationRun), Findings: []remediation.Finding{f}},
			VerifyError: f.Message,
		}, nil
	}
	if err != nil {
		return step.GateVerdict{}, err
	}
	t, attempt := in.Trigger, in.Try.Attempt
	verifications := make([]proposal.Verification, 0, len(runs))
	for _, r := range runs {
		service := r.edits.Service
		overlayURI := ""
		if r.kind == ports.VerificationKindPython {
			// A python service's artifact IS the packaged contract: there is no
			// project to overlay, so the run reads the contract written under its
			// own id.
			if _, err := g.artifacts.Write(ctx,
				service+"/"+r.runID+"/contract.yaml", string(r.edits.Contract), "application/yaml"); err != nil {
				return step.GateVerdict{}, fmt.Errorf("write verification contract for %s: %w", service, err)
			}
		} else {
			files := make([]overlay.File, 0, len(r.edits.Edits))
			for _, e := range r.edits.Edits {
				content, err := g.evidence.Fetch(ctx, e.ContentURI)
				if err != nil {
					return step.GateVerdict{}, fmt.Errorf("read proposed content for %s: %w", e.Path, err)
				}
				files = append(files, overlay.File{
					Path:    strings.TrimPrefix(e.Path, r.edits.Prefix+"/"),
					Content: []byte(content),
				})
			}
			tarball, err := overlay.Build(files)
			if err != nil {
				return step.GateVerdict{}, fmt.Errorf("build source overlay for %s: %w", service, err)
			}
			overlayURI, err = g.artifacts.Write(ctx,
				service+"/"+r.runID+"/source-overlay.tar.gz", string(tarball), "application/gzip")
			if err != nil {
				return step.GateVerdict{}, fmt.Errorf("write source overlay for %s: %w", service, err)
			}
		}

		// The trigger carries no image tag, so the failing release's own tag is
		// read and reused: a verification run never promotes, so it never
		// reaches the path an image tag would otherwise drive.
		imageTag, err := g.releases.ImageTag(ctx, t.ReleaseID, service)
		if err != nil {
			return step.GateVerdict{}, fmt.Errorf("read image tag of release %s for %s: %w", t.ReleaseID, service, err)
		}
		if err := g.pipeline.Submit(ctx, ports.VerificationRequest{
			RunID:             r.runID,
			Service:           service,
			ImageTag:          imageTag,
			Kind:              r.kind,
			VerifiesReleaseID: t.ReleaseID,
			Attempt:           attempt,
			SourceOverlayURI:  overlayURI,
		}); err != nil {
			return step.GateVerdict{}, fmt.Errorf("submit verification run %s: %w", r.runID, err)
		}
		g.logger.Info("verification run submitted",
			"release", t.ReleaseID, "attempt", attempt, "service", service,
			"run", r.runID, "kind", r.kind, "edits", len(r.edits.Edits))
		verifications = append(verifications, proposal.Verification{
			Service: service, Kind: r.kind, RunID: r.runID, Phase: proposal.PhaseQueued,
		})
	}
	return step.GateVerdict{
		Result:        remediation.CheckResult{Check: string(nameVerificationRun), Waiting: true, RunIDs: runIDs(verifications)},
		Verifications: verifications,
	}, nil
}

// Poll reads the status of every run of the try once and gives the gate's
// verdict: waiting while a run is queued or running inside its budget, failed
// on the first failed run or when the budget is spent, passed when every run
// passed. The runs of the try are rebuilt from the input the way Submit built
// them, and refused when they are not the runs the waiting result names. The
// verdict carries the service, kind, phase and errors of each run it read, and
// Phases lists those seen queued or running. A run whose status could not be
// read is left out of both, so the runner keeps what it recorded for it. An
// input with no SubmittedAt is refused before any run is read, since the budget
// of a run whose status cannot be read counts from it.
func (g *VerificationRun) Poll(ctx context.Context, in step.GateInput, waiting remediation.CheckResult) (step.GateVerdict, error) {
	if in.SubmittedAt.IsZero() {
		return step.GateVerdict{}, errNoSubmitTime
	}
	planned, err := g.plan(in)
	if err != nil {
		return step.GateVerdict{}, err
	}
	got := read{nodeErrors: map[string]string{}}
	for _, r := range planned {
		got.runs = append(got.runs, proposal.Verification{
			Service: r.edits.Service, Kind: r.kind, RunID: r.runID, Phase: proposal.PhaseQueued,
		})
	}
	if ids := runIDs(got.runs); !slices.Equal(ids, waiting.RunIDs) {
		return step.GateVerdict{}, fmt.Errorf("the try's runs are %v and the waiting result names %v", ids, waiting.RunIDs)
	}
	for i := range got.runs {
		st, err := g.pipeline.Status(ctx, got.runs[i].RunID)
		if err != nil {
			g.logger.Warn("verification: read run status", "run", got.runs[i].RunID, "error", err)
			break
		}
		got.runs[i] = applyStatus(got.runs[i], st)
		got.read = i + 1
		if st.Phase == proposal.PhaseFailed {
			for node, msg := range st.NodeErrors {
				got.nodeErrors[node] = msg
			}
		}
	}
	return g.decide(in, got), nil
}
