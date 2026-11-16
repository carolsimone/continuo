package ports

import (
	"context"
	"errors"
)

// ErrNoImagePackages reports that neither the rejected release's contract
// bundle nor the bundle the service's CI uploaded for that release records the
// package list of the image the service runs. No fix for the service can be
// packaged or verified for that release, and asking again gives the same
// answer.
var ErrNoImagePackages = errors.New("no package list of the service's image is recorded for the release")

// ImagePackagesSource reads the package list (package name to version) of the
// image a service runs in a candidate release. A Python fix is packaged with
// that list, so its verification run checks the fix against the packages the
// rejected release was built with.
type ImagePackagesSource interface {
	// ImagePackages returns the package list of service's image in release
	// releaseID. It returns an error wrapping ErrNoImagePackages, naming the
	// release and the service, when no bundle of the release records it; any
	// other error is a read a retry may clear.
	ImagePackages(ctx context.Context, releaseID, service string) (map[string]string, error)
}
