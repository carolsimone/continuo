package release

import "time"

// ServiceProd is the per-service production pointer: it records which manifest
// and image are currently live for a single service. The full production
// topology is assembled by collecting every service's pointer at release time.
type ServiceProd struct {
	serviceName   string
	releaseID     string
	manifestS3Key string
	imageTag      string
	manifestKind  ManifestKind
	updatedAt     time.Time
}

// NewServiceProd constructs a ServiceProd with all fields set. manifestKind
// records how this service's artifact is parsed; it follows the promoted
// release's kind, so a service can migrate dbt→python across releases.
func NewServiceProd(serviceName, releaseID, manifestS3Key, imageTag string, manifestKind ManifestKind, updatedAt time.Time) *ServiceProd {
	return &ServiceProd{
		serviceName:   serviceName,
		releaseID:     releaseID,
		manifestS3Key: manifestS3Key,
		imageTag:      imageTag,
		manifestKind:  manifestKind,
		updatedAt:     updatedAt,
	}
}

func (s *ServiceProd) ServiceName() string        { return s.serviceName }
func (s *ServiceProd) ReleaseID() string          { return s.releaseID }
func (s *ServiceProd) ManifestS3Key() string      { return s.manifestS3Key }
func (s *ServiceProd) ImageTag() string           { return s.imageTag }
func (s *ServiceProd) ManifestKind() ManifestKind { return s.manifestKind }
func (s *ServiceProd) UpdatedAt() time.Time       { return s.updatedAt }
