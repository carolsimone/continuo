package s3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/pkg/contractbundle"
	"github.com/carolsimone/continuo/pkg/objectstore"
)

// maxContractBundleBytes caps the gzipped contract bundle held in memory, the
// same ceiling pkg/contractbundle.Decode refuses above.
const maxContractBundleBytes int64 = contractbundle.MaxObjectBytes

// maxProducerBundleBytes caps the odcs-bundle.json a service's CI uploaded,
// which carries the text of every contract of the service.
const maxProducerBundleBytes int64 = 64 << 20

// ImagePackagesSource implements ports.ImagePackagesSource over an
// object-store reader. It reads the package list from the rejected release's
// contract bundle, and, only for a release that has none (one rejected before
// its parse ended), from the odcs-bundle.json the service's CI uploaded for
// the release.
type ImagePackagesSource struct {
	objects  objectstore.Reader
	releases ports.ContractBundleReader
	bucket   string
}

var _ ports.ImagePackagesSource = (*ImagePackagesSource)(nil)

// NewImagePackagesSource builds an ImagePackagesSource. releases names a
// release's contract bundle; bucket is the bucket a service's CI uploads its
// odcs-bundle.json to, s3://<bucket>/<service>/<release id>/odcs-bundle.json.
func NewImagePackagesSource(objects objectstore.Reader, releases ports.ContractBundleReader, bucket string) *ImagePackagesSource {
	return &ImagePackagesSource{objects: objects, releases: releases, bucket: bucket}
}

// producerBundle is the part of a service's odcs-bundle.json this source reads.
type producerBundle struct {
	Service       string            `json:"service"`
	ImagePackages map[string]string `json:"image_packages"`
}

// ImagePackages returns the package list of service's image in release
// releaseID. A release whose parse wrote a contract bundle is answered by that
// bundle alone, with Provenance[service].ImagePackages: the bundle is what the
// release's parse read, so another document never stands in for it. Only a
// release with no contract bundle is answered by the image_packages of the
// service's odcs-bundle.json for the release. A bundle that is missing, cannot
// be read for good (too large, a wrong checksum, not the document it should
// be) or does not record the service gives an error wrapping
// ports.ErrNoImagePackages that names the release, the service and why. A
// release read or an object read a retry may clear is returned as is.
func (s *ImagePackagesSource) ImagePackages(ctx context.Context, releaseID, service string) (map[string]string, error) {
	uri, sha, err := s.releases.ContractBundle(ctx, releaseID)
	if err != nil {
		return nil, fmt.Errorf("read the contract bundle of release %s: %w", releaseID, err)
	}
	var packages map[string]string
	var miss string
	if uri != "" {
		packages, miss, err = s.fromContractBundle(ctx, uri, sha, service)
	} else {
		packages, miss, err = s.fromProducerBundle(ctx, releaseID, service)
		if miss != "" {
			miss = "the release has no contract bundle, and " + miss
		}
	}
	if err != nil || miss == "" {
		return packages, err
	}
	return nil, fmt.Errorf("release %s, service %s: %w (%s)", releaseID, service, ports.ErrNoImagePackages, miss)
}

// fromContractBundle reads the package list out of the contract bundle at uri.
// miss says why the bundle does not hold the service, "" when it does.
func (s *ImagePackagesSource) fromContractBundle(ctx context.Context, uri, sha, service string) (map[string]string, string, error) {
	ref, err := objectstore.ParseRef(uri)
	if err != nil {
		return nil, fmt.Sprintf("the contract bundle uri %q names no object", uri), nil
	}
	body, err := s.objects.Get(ctx, ref, maxContractBundleBytes)
	switch {
	case errors.Is(err, objectstore.ErrNotFound), errors.Is(err, objectstore.ErrTooLarge), errors.Is(err, objectstore.ErrInvalidRef):
		return nil, fmt.Sprintf("the contract bundle %s cannot be read: %v", uri, err), nil
	case err != nil:
		return nil, "", fmt.Errorf("get the contract bundle %s: %w", uri, err)
	}
	doc, err := contractbundle.Decode(body, sha)
	if err != nil {
		return nil, fmt.Sprintf("the contract bundle %s cannot be read: %v", uri, err), nil
	}
	prov, ok := doc.Provenance[service]
	if !ok || prov.ImagePackages == nil {
		return nil, "the contract bundle records no package list for the service", nil
	}
	return prov.ImagePackages, "", nil
}

// fromProducerBundle reads the package list out of the odcs-bundle.json the
// service's CI uploaded for the release. miss says why it does not hold the
// service, "" when it does.
func (s *ImagePackagesSource) fromProducerBundle(ctx context.Context, releaseID, service string) (map[string]string, string, error) {
	ref := objectstore.Ref{Bucket: s.bucket, Key: service + "/" + releaseID + "/" + ports.ODCSBundleName}
	body, err := s.objects.Get(ctx, ref, maxProducerBundleBytes)
	switch {
	case errors.Is(err, objectstore.ErrNotFound), errors.Is(err, objectstore.ErrTooLarge), errors.Is(err, objectstore.ErrInvalidRef):
		return nil, fmt.Sprintf("the service's bundle %s cannot be read: %v", ref, err), nil
	case err != nil:
		return nil, "", fmt.Errorf("get the service's bundle %s: %w", ref, err)
	}
	var b producerBundle
	if err := json.Unmarshal(body, &b); err != nil {
		return nil, fmt.Sprintf("the service's bundle %s is not JSON: %v", ref, err), nil
	}
	if b.Service != service {
		return nil, fmt.Sprintf("the bundle %s is of service %q", ref, b.Service), nil
	}
	if b.ImagePackages == nil {
		return nil, fmt.Sprintf("the service's bundle %s records no package list", ref), nil
	}
	return b.ImagePackages, "", nil
}
