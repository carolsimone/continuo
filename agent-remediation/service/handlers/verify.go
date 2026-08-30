package handlers

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

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
	for _, e := range edits {
		service, prefix, ok := serviceForPath(deps.ServiceRepoPaths, e.Path)
		if !ok {
			return nil, fmt.Errorf("%w: %s", errUnmappedEdit, e.Path)
		}
		byService[service] = append(byService[service], e)
		prefixes[service] = prefix
	}

	services := make([]string, 0, len(byService))
	for service := range byService {
		services = append(services, service)
	}
	sort.Strings(services)

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

// serviceForPath resolves which configured service owns a repository path, and
// returns that service's project root within the repo. The owner is the service
// whose root is the longest prefix of the path, so a repo where one service's
// root nests inside another's still resolves to the nearest one. A service
// mapped to the repository root itself ("") owns any path no other service
// claims. Ties between equally specific roots go to the smaller service name, so
// the answer does not depend on map iteration order.
func serviceForPath(serviceRepoPaths map[string]string, filePath string) (service, prefix string, ok bool) {
	best := -1
	for name, root := range serviceRepoPaths {
		if root != "" && !strings.HasPrefix(filePath, root+"/") {
			continue
		}
		if len(root) < best || (len(root) == best && name >= service) {
			continue
		}
		service, prefix, best = name, root, len(root)
	}
	return service, prefix, best >= 0
}
