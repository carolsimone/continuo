package release

import "time"

// ServiceProd is the per-project production pointer: it records which manifest
// and image are currently live for a single project. The full production
// topology is assembled by collecting every project's pointer at release time.
type ServiceProd struct {
	projectName   string
	releaseID     string
	manifestS3Key string
	imageTag      string
	manifestKind  ManifestKind
	updatedAt     time.Time
}

// NewServiceProd constructs a ServiceProd with all fields set. manifestKind
// records how this project's artifact is parsed; it follows the promoted
// release's kind, so a project can migrate dbt→python across releases.
func NewServiceProd(projectName, releaseID, manifestS3Key, imageTag string, manifestKind ManifestKind, updatedAt time.Time) *ServiceProd {
	return &ServiceProd{
		projectName:   projectName,
		releaseID:     releaseID,
		manifestS3Key: manifestS3Key,
		imageTag:      imageTag,
		manifestKind:  manifestKind,
		updatedAt:     updatedAt,
	}
}

func (s *ServiceProd) ProjectName() string        { return s.projectName }
func (s *ServiceProd) ReleaseID() string          { return s.releaseID }
func (s *ServiceProd) ManifestS3Key() string      { return s.manifestS3Key }
func (s *ServiceProd) ImageTag() string           { return s.imageTag }
func (s *ServiceProd) ManifestKind() ManifestKind { return s.manifestKind }
func (s *ServiceProd) UpdatedAt() time.Time       { return s.updatedAt }
