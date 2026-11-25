package ports

import (
	"context"
	"errors"
)

// ErrNoImagePackages reports that the rejected release records no package list
// of the image the project runs: the release's contract bundle does not record
// it, or, for a release with no contract bundle, the project's
// odcs-bundle.json does not. No fix for the project can be packaged or
// verified for that release, and asking again gives the same answer.
var ErrNoImagePackages = errors.New("no package list of the project's image is recorded for the release")

// ImagePackagesSource reads the package list (package name to version) of the
// image a project runs in a candidate release. A Python fix is packaged with
// that list, so its verification run checks the fix against the packages the
// rejected release was built with.
type ImagePackagesSource interface {
	// ImagePackages returns the package list of project's image in release
	// releaseID: from the release's contract bundle when the release has one,
	// and only otherwise from the project's odcs-bundle.json. It returns an
	// error wrapping ErrNoImagePackages, naming the release and the project,
	// when that bundle does not record it; any other error is a read a retry
	// may clear.
	ImagePackages(ctx context.Context, releaseID, project string) (map[string]string, error)
}
