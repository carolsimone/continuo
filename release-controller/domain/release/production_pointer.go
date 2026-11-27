package release

import "time"

// ProductionPointer is a project's production pointer: the release, manifest,
// image tag and manifest kind production runs for that project now. A release
// assembles its manifest set from every project's production pointer.
type ProductionPointer struct {
	projectName   string
	releaseID     string
	manifestS3Key string
	imageTag      string
	manifestKind  ManifestKind
	updatedAt     time.Time
}

// NewProductionPointer constructs a ProductionPointer with all fields set.
// manifestKind records how this project's artifact is parsed; it follows the
// promoted release's kind, so a project can migrate dbt→python across releases.
func NewProductionPointer(projectName, releaseID, manifestS3Key, imageTag string, manifestKind ManifestKind, updatedAt time.Time) *ProductionPointer {
	return &ProductionPointer{
		projectName:   projectName,
		releaseID:     releaseID,
		manifestS3Key: manifestS3Key,
		imageTag:      imageTag,
		manifestKind:  manifestKind,
		updatedAt:     updatedAt,
	}
}

func (p *ProductionPointer) ProjectName() string        { return p.projectName }
func (p *ProductionPointer) ReleaseID() string          { return p.releaseID }
func (p *ProductionPointer) ManifestS3Key() string      { return p.manifestS3Key }
func (p *ProductionPointer) ImageTag() string           { return p.imageTag }
func (p *ProductionPointer) ManifestKind() ManifestKind { return p.manifestKind }
func (p *ProductionPointer) UpdatedAt() time.Time       { return p.updatedAt }
