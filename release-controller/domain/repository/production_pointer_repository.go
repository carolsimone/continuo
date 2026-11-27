package repository

import (
	"context"

	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// ProductionPointerRepository owns the production pointers, one per project.
// Each records the manifest S3 key, image tag and manifest kind production
// runs for its project; a release assembles its manifest set from them.
type ProductionPointerRepository interface {
	List(ctx context.Context) ([]*release.ProductionPointer, error)
	Get(ctx context.Context, projectName string) (*release.ProductionPointer, error)
	Upsert(ctx context.Context, pointer *release.ProductionPointer) error
	// ProjectsWithKind lists the names of the projects whose production
	// pointer has the given manifest kind, sorted ascending. No such project
	// is an empty list, not an error.
	ProjectsWithKind(ctx context.Context, kind release.ManifestKind) ([]string, error)
}
