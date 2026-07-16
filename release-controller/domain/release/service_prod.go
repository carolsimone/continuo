package release

import (
	"time"

	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
)

// ServiceProd is the per-service production pointer: it records which manifest,
// image, and runtime manifest artifact are currently live for a single dbt
// service. The full production topology is assembled by collecting every
// service's pointer at release time. Because each service owns its own pointer,
// a release that changes one service leaves every other service's artifact
// exactly where it was.
type ServiceProd struct {
	serviceName     string
	releaseID       string
	manifestS3Key   string
	imageTag        string
	runtimeManifest pkgmodel.RuntimeManifestRef
	updatedAt       time.Time
}

// NewServiceProd constructs a ServiceProd that pins no runtime manifest. This is
// the shape of a pointer seeded from an existing production topology, or one
// written by a release whose parse produced no artifact; its nodes' Jobs parse
// the dbt project themselves.
func NewServiceProd(serviceName, releaseID, manifestS3Key, imageTag string, updatedAt time.Time) *ServiceProd {
	return NewServiceProdWithRuntime(
		serviceName, releaseID, manifestS3Key, imageTag,
		pkgmodel.RuntimeManifestRef{}, updatedAt,
	)
}

// NewServiceProdWithRuntime constructs a ServiceProd pinned to runtimeManifest.
// Passing the zero reference is equivalent to NewServiceProd.
func NewServiceProdWithRuntime(serviceName, releaseID, manifestS3Key, imageTag string,
	runtimeManifest pkgmodel.RuntimeManifestRef, updatedAt time.Time) *ServiceProd {
	return &ServiceProd{
		serviceName:     serviceName,
		releaseID:       releaseID,
		manifestS3Key:   manifestS3Key,
		imageTag:        imageTag,
		runtimeManifest: runtimeManifest,
		updatedAt:       updatedAt,
	}
}

func (s *ServiceProd) ServiceName() string   { return s.serviceName }
func (s *ServiceProd) ReleaseID() string     { return s.releaseID }
func (s *ServiceProd) ManifestS3Key() string { return s.manifestS3Key }
func (s *ServiceProd) ImageTag() string      { return s.imageTag }
func (s *ServiceProd) UpdatedAt() time.Time  { return s.updatedAt }

// RuntimeManifest returns the pinned runtime manifest reference, or the zero
// reference when this service pins none.
func (s *ServiceProd) RuntimeManifest() pkgmodel.RuntimeManifestRef { return s.runtimeManifest }
