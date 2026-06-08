package release

import "time"

// ServiceProd is the per-service production pointer: it records which manifest
// and image are currently live for a single dbt service. The full production
// topology is assembled by collecting every service's pointer at release time.
type ServiceProd struct {
	serviceName    string
	releaseID      string
	manifestS3Key  string
	imageTag       string
	updatedAt      time.Time
}

// NewServiceProd constructs a ServiceProd with all fields set.
func NewServiceProd(serviceName, releaseID, manifestS3Key, imageTag string, updatedAt time.Time) *ServiceProd {
	return &ServiceProd{
		serviceName:   serviceName,
		releaseID:     releaseID,
		manifestS3Key: manifestS3Key,
		imageTag:      imageTag,
		updatedAt:     updatedAt,
	}
}

func (s *ServiceProd) ServiceName() string  { return s.serviceName }
func (s *ServiceProd) ReleaseID() string    { return s.releaseID }
func (s *ServiceProd) ManifestS3Key() string { return s.manifestS3Key }
func (s *ServiceProd) ImageTag() string     { return s.imageTag }
func (s *ServiceProd) UpdatedAt() time.Time { return s.updatedAt }
