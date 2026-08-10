package handlers

import (
	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// AssembledSet is the full manifest set for a single-service release.
type AssembledSet struct {
	ManifestKeys []release.ManifestKey
	ImageTags    map[string]string
}

// AssembleManifestSet builds the full manifest set for a single-service release:
// the changed service's new manifest key + every OTHER service's stored pointer.
// existing is the live set of service_prod pointers, read once by the caller.
// The changed service's prior pointer (if present) is replaced, never duplicated.
func AssembleManifestSet(existing []*release.ServiceProd, bucket, changedService, releaseID, imageTag string, changedKind release.ManifestKind) AssembledSet {
	keys := []release.ManifestKey{{
		Service: changedService,
		S3URI:   CanonicalManifestKey(bucket, changedService, releaseID, changedKind),
		Kind:    changedKind,
	}}
	tags := map[string]string{changedService: imageTag}
	for _, sp := range existing {
		if sp.ServiceName() == changedService {
			continue // replaced by the fresh delta
		}
		keys = append(keys, release.ManifestKey{Service: sp.ServiceName(), S3URI: sp.ManifestS3Key(), Kind: sp.ManifestKind()})
		tags[sp.ServiceName()] = sp.ImageTag()
	}
	return AssembledSet{ManifestKeys: keys, ImageTags: tags}
}
