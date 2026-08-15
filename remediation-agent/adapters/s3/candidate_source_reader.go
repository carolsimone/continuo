package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/carolsimone/continuo/pkg/codebundle"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// CandidateSourceReader reads one node's source out of a release's code
// bundle in object storage. It reuses the bundle contract decoder shared
// with the orchestrator, but exposes only the slice remediation needs.
type CandidateSourceReader struct {
	client *awss3.Client
	bucket string
}

var _ ports.CandidateSourceReader = (*CandidateSourceReader)(nil)

// maxBundleBytes caps the bundle document held in memory, mirroring the
// orchestrator's ceiling for the same object.
const maxBundleBytes int64 = 64 * 1024 * 1024

// NewCandidateSourceReader builds an S3 client for the given bucket.
// endpointURL empty → AWS default; non-empty (e.g. http://localstack:4566) → path-style addressing.
func NewCandidateSourceReader(endpointURL, bucket, region, accessKeyID, secretKey string) *CandidateSourceReader {
	cfg := aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(accessKeyID, secretKey, ""),
	}
	opts := []func(*awss3.Options){func(o *awss3.Options) { o.UsePathStyle = true }}
	if endpointURL != "" {
		opts = append(opts, func(o *awss3.Options) { o.BaseEndpoint = aws.String(endpointURL) })
	}
	return &CandidateSourceReader{client: awss3.NewFromConfig(cfg, opts...), bucket: bucket}
}

// NodeSource returns the node's bundle entry. Every permanently-unreadable
// condition — empty URI, missing object, missing node, malformed document, or
// a bundle for a release other than releaseID — maps to ports.ErrNotFound so
// the caller takes its repo-read fallback; transient fetch errors return as-is
// so the trigger redelivers.
func (r *CandidateSourceReader) NodeSource(ctx context.Context, bundleURI, uniqueID, releaseID string) (ports.CandidateSource, error) {
	if strings.TrimSpace(bundleURI) == "" {
		return ports.CandidateSource{}, ports.ErrNotFound
	}
	bucket, key := parseS3URI(bundleURI)
	if bucket == "" {
		bucket = r.bucket
	}
	out, err := r.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		var nsk *s3types.NoSuchKey
		if errors.As(err, &nsk) {
			return ports.CandidateSource{}, ports.ErrNotFound
		}
		return ports.CandidateSource{}, fmt.Errorf("get code bundle %s: %w", bundleURI, err)
	}
	defer func() { _ = out.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(out.Body, maxBundleBytes+1))
	if err != nil {
		return ports.CandidateSource{}, fmt.Errorf("read code bundle %s: %w", bundleURI, err)
	}
	if int64(len(body)) > maxBundleBytes {
		return ports.CandidateSource{}, ports.ErrNotFound
	}
	bundle, err := codebundle.Decode(body)
	if err != nil {
		return ports.CandidateSource{}, ports.ErrNotFound
	}
	if bundle.ReleaseID != releaseID {
		// The URI resolved to a bundle for a different release than the trigger's
		// own — a stale or misrouted object naming the same node id. Returning its
		// RawCode would stamp another release's source onto this node's fix, the
		// same guard the orchestrator applies to the same bundle document in
		// remediation_requested_rejections_handler.go and
		// release_promoted_versions_handler.go.
		return ports.CandidateSource{}, ports.ErrNotFound
	}
	node, ok := bundle.Nodes[uniqueID]
	if !ok {
		return ports.CandidateSource{}, ports.ErrNotFound
	}
	return ports.CandidateSource{RawCode: node.RawCode, Runtime: node.Runtime}, nil
}
