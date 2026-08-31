package handlers

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/service/overlay"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// errUnmappedEdit marks an edit whose path lies outside every service this
// install configures a repository path for. Nothing downstream can run such an
// edit — a shadow release is submitted for one service, and the overlay it lays
// down is relative to that service's project — so the attempt ends with the
// reason recorded rather than being redelivered forever.
var errUnmappedEdit = errors.New("edit belongs to no configured service")

// errMixedManifestKinds marks a service whose edits would have to be verified
// as two different manifest kinds at once: a packaged python contract plus at
// least one edit to something that is not a python node's contract. A release
// parses one kind, so whichever lane were chosen would verify only half the
// change and silently ignore the rest.
var errMixedManifestKinds = errors.New("mixes a python contract with dbt edits in one attempt; a release verifies one manifest kind")

// errDuplicateEditPath marks an attempt carrying two edits to one repository
// path. A source overlay holds one file per path and a python service is
// verified by one packaged contract, so the second edit would silently replace
// the first: the shadow release would run a file the attempt does not describe,
// and the proposal would record an edit nothing ever verified.
var errDuplicateEditPath = errors.New("two edits change the same file")

// unverifiable reports whether a verification failure is permanent — the edits
// themselves are unrunnable, so a redelivery would fail identically. The caller
// records the attempt rather than returning the error for redelivery.
func unverifiable(err error) bool {
	return errors.Is(err, errUnmappedEdit) ||
		errors.Is(err, errMixedManifestKinds) ||
		errors.Is(err, errDuplicateEditPath)
}

// submitVerifications posts one shadow verification release per service the
// attempt edits, and returns what it posted. A shadow release is a real release
// that runs the full parse → candidate-schema → validation pipeline and stops at
// "validated" instead of promoting, so it is what decides whether these edits
// actually fix the failing release.
//
// What the shadow runs depends on the service's manifest kind. A python service
// is verified by the contract yaml its fix packaged, uploaded under the shadow's
// own id. A dbt service is verified by re-running its project with the proposed
// content laid over it, so each edit's content is read back from the artifact the
// Fixer wrote and packed into one source overlay, keyed by the edit's path
// within that service's project.
//
// Both the artifact writes and the submission are idempotent on the attempt's
// keys — the shadow id is a pure function of (release, service, attempt) — so a
// redelivery repeats this safely.
func submitVerifications(
	ctx context.Context, deps Deps, t Trigger, attempt int,
	edits []proposal.FileEdit, contracts map[string][]byte,
) ([]proposal.Verification, error) {
	byService := map[string][]proposal.FileEdit{}
	prefixes := map[string]string{}
	seen := map[string]bool{}
	for _, e := range edits {
		service, prefix, ok := serviceForPath(deps.ServiceRepoPaths, e.Path)
		if !ok {
			return nil, fmt.Errorf("%w: %s", errUnmappedEdit, e.Path)
		}
		// One path may be changed once per attempt. Two edits to it cannot both
		// be verified, and choosing one silently would verify a fix the attempt
		// does not describe.
		if seen[e.Path] {
			deps.Logger.Error("two of the attempt's edits change the same file; the attempt cannot be verified",
				"release", t.ReleaseID, "attempt", attempt, "path", e.Path, "target", e.TargetNodeID)
			return nil, fmt.Errorf("%w: %s", errDuplicateEditPath, e.Path)
		}
		seen[e.Path] = true
		byService[service] = append(byService[service], e)
		prefixes[service] = prefix
	}

	services := make([]string, 0, len(byService))
	for service := range byService {
		services = append(services, service)
	}
	sort.Strings(services)

	// Every service is checked before any is submitted, so an attempt one of
	// them cannot verify spends no release slot on the others — the attempt is
	// recorded as a whole, and a half-submitted one would leave shadow releases
	// running for a fix that was never recorded as being verified.
	for _, service := range services {
		if contracts[service] == nil {
			continue
		}
		if edit, ok := nonContractEdit(t, byService[service]); ok {
			deps.Logger.Error("a service's edits span two manifest kinds; the attempt cannot be verified",
				"release", t.ReleaseID, "attempt", attempt, "service", service,
				"edit", edit.Path, "target", edit.TargetNodeID)
			return nil, fmt.Errorf("service %s %w", service, errMixedManifestKinds)
		}
	}

	verifications := make([]proposal.Verification, 0, len(services))
	for _, service := range services {
		shadowID := ShadowReleaseID(t.ReleaseID, service, attempt)
		kind := ports.ShadowKindDbt
		overlayURI := ""

		if contract := contracts[service]; contract != nil {
			// A python service's artifact IS the packaged contract: there is no
			// project to overlay, so the shadow release reads the contract
			// written under its own id.
			kind = ports.ShadowKindPython
			if _, err := deps.Artifacts.Write(ctx,
				service+"/"+shadowID+"/contract.yaml", string(contract), "application/yaml"); err != nil {
				return nil, fmt.Errorf("write shadow contract for %s: %w", service, err)
			}
		} else {
			files := make([]overlay.File, 0, len(byService[service]))
			for _, e := range byService[service] {
				content, err := deps.Evidence.Fetch(ctx, e.ContentURI)
				if err != nil {
					return nil, fmt.Errorf("read proposed content for %s: %w", e.Path, err)
				}
				files = append(files, overlay.File{
					Path:    strings.TrimPrefix(e.Path, prefixes[service]+"/"),
					Content: []byte(content),
				})
			}
			tarball, err := overlay.Build(files)
			if err != nil {
				return nil, fmt.Errorf("build source overlay for %s: %w", service, err)
			}
			overlayURI, err = deps.Artifacts.Write(ctx,
				service+"/"+shadowID+"/source-overlay.tar.gz", string(tarball), "application/gzip")
			if err != nil {
				return nil, fmt.Errorf("write source overlay for %s: %w", service, err)
			}
		}

		// The trigger carries no image tag, so the failing release's own tag is
		// read and reused: a shadow never promotes, so it never reaches the path
		// an image tag would otherwise drive.
		imageTag, err := deps.Releases.ImageTag(ctx, t.ReleaseID, service)
		if err != nil {
			return nil, fmt.Errorf("read image tag of release %s for %s: %w", t.ReleaseID, service, err)
		}
		if err := deps.Releases.Submit(ctx, ports.ShadowSubmission{
			ReleaseID:        shadowID,
			Service:          service,
			ImageTag:         imageTag,
			Repo:             t.Repo,
			CommitSHA:        t.CommitSHA,
			Kind:             kind,
			SourceOverlayURI: overlayURI,
			// The shadow verifies THIS release's rejection. When the fix edits a
			// different service than the rejected release changed, that release's
			// own candidate is what the shadow must run its edited service
			// against, not the service's production manifest.
			VerifiesReleaseID: t.ReleaseID,
		}); err != nil {
			return nil, fmt.Errorf("submit shadow release %s: %w", shadowID, err)
		}
		deps.Logger.Info("shadow verification release submitted",
			"release", t.ReleaseID, "attempt", attempt, "service", service,
			"shadow", shadowID, "kind", kind, "edits", len(byService[service]))

		verifications = append(verifications, proposal.Verification{
			Service:         service,
			Kind:            kind,
			ShadowReleaseID: shadowID,
		})
	}
	return verifications, nil
}

// nonContractEdit returns the first edit in a python service's group that is
// not a python node's contract, and whether one exists. An edit qualifies only
// when it changes the source of a failing node the trigger carries and that
// node is a python kind; an edit to a dbt node, or to an upstream ancestor the
// trigger never named, is something a python release cannot verify.
func nonContractEdit(t Trigger, edits []proposal.FileEdit) (proposal.FileEdit, bool) {
	for _, e := range edits {
		n, ok := nodeByID(t, e.TargetNodeID)
		if !ok || !pkg_model.NodeType(n.NodeType).IsPython() {
			return e, true
		}
	}
	return proposal.FileEdit{}, false
}

// serviceForPath resolves which configured service owns a repository path.
// It is a delegate to proposal.ServiceForPath, kept with an unexported name
// so existing handler callers do not churn.
func serviceForPath(serviceRepoPaths map[string]string, filePath string) (service, prefix string, ok bool) {
	return proposal.ServiceForPath(serviceRepoPaths, filePath)
}
