package handlers

import (
	"context"

	"github.com/carolsimone/continuo/release-controller/domain/repository"
)

// ManifestKey identifies one service's manifest in the assembled set carried by
// release.requested:v1.
type ManifestKey struct {
	Service string `json:"service"`
	S3URI   string `json:"s3_uri"`
}

// AssembledSet is the full manifest set for a single-service release.
type AssembledSet struct {
	ManifestKeys []ManifestKey
	ImageTags    map[string]string
}

// AssembleManifestSet builds the full manifest set for a single-service release:
// the changed service's new manifest key + every OTHER service's stored pointer.
// The changed service's prior pointer (if present) is replaced, never duplicated.
func AssembleManifestSet(ctx context.Context, repo repository.ServiceProdRepository, bucket, changedService, releaseID, imageTag string) (AssembledSet, error) {
	existing, err := repo.List(ctx)
	if err != nil {
		return AssembledSet{}, err
	}
	keys := []ManifestKey{{Service: changedService, S3URI: CanonicalManifestKey(bucket, changedService, releaseID)}}
	tags := map[string]string{changedService: imageTag}
	for _, sp := range existing {
		if sp.ServiceName() == changedService {
			continue // replaced by the fresh delta
		}
		keys = append(keys, ManifestKey{Service: sp.ServiceName(), S3URI: sp.ManifestS3Key()})
		tags[sp.ServiceName()] = sp.ImageTag()
	}
	return AssembledSet{ManifestKeys: keys, ImageTags: tags}, nil
}
