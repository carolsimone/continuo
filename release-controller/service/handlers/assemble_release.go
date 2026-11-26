package handlers

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/uow"
)

// AssembledSet is the full manifest set for a single-project release.
type AssembledSet struct {
	ManifestKeys []release.ManifestKey
	ImageTags    map[string]string
}

// AssembleManifestSet builds the full manifest set for a single-project release:
// the changed project's new manifest key + every OTHER project's stored pointer.
// existing is the live set of service_prod pointers, read once by the caller.
// The changed project's prior pointer (if present) is replaced, never duplicated.
func AssembleManifestSet(existing []*release.ServiceProd, bucket, changedProject, releaseID, imageTag string, changedKind release.ManifestKind) AssembledSet {
	keys := []release.ManifestKey{{
		Project: changedProject,
		S3URI:   CanonicalManifestKey(bucket, changedProject, releaseID, changedKind),
		Kind:    changedKind,
	}}
	tags := map[string]string{changedProject: imageTag}
	for _, sp := range existing {
		if sp.ProjectName() == changedProject {
			continue // replaced by the fresh delta
		}
		keys = append(keys, release.ManifestKey{Project: sp.ProjectName(), S3URI: sp.ManifestS3Key(), Kind: sp.ManifestKind()})
		tags[sp.ProjectName()] = sp.ImageTag()
	}
	return AssembledSet{ManifestKeys: keys, ImageTags: tags}
}

// applyVerifiedReleaseOverride assembles the rejected release's changed project
// from THAT release's candidate, for a verification run verifying a fix in a
// different project.
//
// A verification run is a single-project delta like any other run: its own
// project carries the fix, and every other project is assembled from the
// live service_prod pointers. When the fix edits a DOWNSTREAM project, the
// project whose release was actually rejected is one of those "other"
// projects, so the fix would be checked against that project's PRODUCTION
// code — not the candidate whose rejection the fix is answering. The change
// would then be judged on a graph the failure never occurred in.
//
// The verification run therefore names the release it verifies, and that
// release's own changed project is swapped to its candidate manifest key and
// image tag. The original release is read once here; if it is gone, never
// got far enough to have a candidate manifest, or holds the kind python, the
// set is left as assembled (assembleFor) and the verification runs against
// production — a weaker check, but a
// running one.
//
// Only the rejected release's own delta is restored. Sibling edits the same
// attempt made in OTHER projects are not co-verified: one release is one
// project delta, so each verification sees its own change plus the rejected
// candidate. The follow-up release that runs after the fix PR is merged remains
// the gate that judges the whole change together.
func applyVerifiedReleaseOverride(set AssembledSet, bucket string, verification, original *pipeline.Run) AssembledSet {
	project := original.ChangedProject()
	if project == "" || project == verification.ChangedProject() {
		return set
	}
	key := CanonicalManifestKey(bucket, project, original.ID(), original.ManifestKind())
	replaced := false
	for i, k := range set.ManifestKeys {
		if k.Project != project {
			continue
		}
		set.ManifestKeys[i] = release.ManifestKey{Project: project, S3URI: key, Kind: original.ManifestKind()}
		replaced = true
	}
	if !replaced {
		// The rejected release's project has no production pointer yet (its
		// first release is the one that was rejected). Its candidate is still
		// what the fix must be judged against, so it joins the set.
		set.ManifestKeys = append(set.ManifestKeys,
			release.ManifestKey{Project: project, S3URI: key, Kind: original.ManifestKind()})
	}
	if tag := original.ImageTags()[project]; tag != "" {
		set.ImageTags[project] = tag
	}
	return set
}

// assembleFor builds the manifest set a run runs against: the standard
// single-project assembly, plus — for a verification run naming the rejected
// release it verifies — that release's own candidate in place of its
// production pointer. It is called from every site that assembles a set, so a
// verification's compile leg and its parse leg cannot disagree about which
// manifest a project contributes.
func assembleFor(
	ctx context.Context, u uow.UnitOfWork, logger *slog.Logger,
	r *pipeline.Run, pointers []*release.ServiceProd, bucket string,
) AssembledSet {
	imageTag := r.ImageTags()[r.ChangedProject()]
	set := AssembleManifestSet(pointers, bucket, r.ChangedProject(), r.ID(), imageTag, r.ManifestKind())
	if r.Kind() == pipeline.KindCandidate || r.VerifiesReleaseID() == "" {
		return set
	}
	original, err := u.RunRepo().Get(ctx, r.VerifiesReleaseID())
	if err != nil || original == nil {
		logger.Warn("verification run verifies a release that cannot be read; assembling from production instead",
			"release_id", r.ID(), "verifies_release_id", r.VerifiesReleaseID(), "error", err)
		return set
	}
	if original.CandidateTopologyRef().IsZero() {
		logger.Warn("the verified release never parsed, so it has no candidate manifest; assembling from production instead",
			"release_id", r.ID(), "verifies_release_id", original.ID())
		return set
	}
	if original.ManifestKind() == release.ManifestKindPython {
		// A release stored with the kind python holds a contract.yaml no
		// parser reads, and its canonical key would name an artifact that does
		// not exist, so its candidate is never assembled.
		logger.Warn("the verified release has the kind python, which no parser reads; assembling from production instead",
			"release_id", r.ID(), "verifies_release_id", original.ID())
		return set
	}
	out := applyVerifiedReleaseOverride(set, bucket, r, original)
	logger.Info("verification run assembles the verified release's candidate for its changed project",
		"release_id", r.ID(), "verifies_release_id", original.ID(),
		"project", original.ChangedProject(), "verification_project", r.ChangedProject())
	return out
}
